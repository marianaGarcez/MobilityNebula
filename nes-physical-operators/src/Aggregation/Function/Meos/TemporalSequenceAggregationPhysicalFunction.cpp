/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Aggregation/Function/Meos/TemporalSequenceAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <string_view>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <cstring>
#include <cstdio>

#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

// MEOS C API headers
extern "C" {
#include <meos.h>
#include <meos_geo.h>
}

namespace NES
{

constexpr static std::string_view LonFieldName = "lon";
constexpr static std::string_view LatFieldName = "lat";
constexpr static std::string_view TimestampFieldName = "timestamp";

// Global MEOS initialization
static bool meos_initialized = false;
static std::mutex meos_mutex;

static void ensureMeosInitialized() {
    std::lock_guard<std::mutex> lock(meos_mutex);
    if (!meos_initialized) {
        // Set timezone to UTC if not set
        if (!std::getenv("TZ")) {
            setenv("TZ", "UTC", 1);
            tzset();
        }
        meos_initialize();
        meos_initialized = true;
    }
}

// Single-field constructor removed - TEMPORAL_SEQUENCE requires three separate field functions

TemporalSequenceAggregationPhysicalFunction::TemporalSequenceAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction lonFunctionParam,
    PhysicalFunction latFunctionParam,
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), lonFunctionParam, std::move(resultFieldIdentifier))
    , memProviderPagedVector(std::move(memProviderPagedVector))
    , lonFunction(std::move(lonFunctionParam))
    , latFunction(std::move(latFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
{
}

void TemporalSequenceAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, ExecutionContext& executionContext, const Nautilus::Record& record)
{
    // Cast to PagedVector pointer consistently with combine() and lower()
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    
    // For TEMPORAL_SEQUENCE, we need to store lon, lat, and timestamp values
    auto lonValue = lonFunction.execute(record, executionContext.pipelineMemoryProvider.arena);
    auto latValue = latFunction.execute(record, executionContext.pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, executionContext.pipelineMemoryProvider.arena);
    
    // Create a record with all three fields for temporal sequence
    Record aggregateStateRecord({
        {std::string(LonFieldName), lonValue},
        {std::string(LatFieldName), latValue},
        {std::string(TimestampFieldName), timestampValue}
    });
    
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector);
    pagedVectorRef.writeRecord(aggregateStateRecord, executionContext.pipelineMemoryProvider.bufferProvider);
}

void TemporalSequenceAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Getting the paged vectors from the aggregation states
    const auto memArea1 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    /// Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](Nautilus::Interface::PagedVector* vector1, const Nautilus::Interface::PagedVector* vector2) -> void
        { vector1->copyFrom(*vector2); },
        memArea1,
        memArea2);
}

Nautilus::Record TemporalSequenceAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    // Ensure MEOS is initialized
    ensureMeosInitialized();
    
    /// Getting the paged vector from the aggregation state
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, memProviderPagedVector);
    const auto allFieldNames = memProviderPagedVector->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {
            return pagedVector->getTotalNumberOfEntries();
        },
        pagedVectorPtr);

    // For now, assume PagedVector is never empty (similar to ArrayAggregationPhysicalFunction)

    // Build the trajectory string in MEOS format: {(lon,lat)@timestamp,...}
    // We'll construct the string piece by piece during iteration
    auto trajectoryStr = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector) -> char*
        {
            // Allocate a buffer for the trajectory string
            // Each point is approximately 50 chars: (-123.456789,12.345678)@1234567890
            size_t bufferSize = pagedVector->getTotalNumberOfEntries() * 60 + 10;
            char* buffer = (char*)malloc(bufferSize);
            strcpy(buffer, "{");
            return buffer;
        },
        pagedVectorPtr);

    // Track if this is the first point using a counter
    auto pointCounter = nautilus::val<int64_t>(0);
    
    /// Read from paged vector
    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {
        const auto itemRecord = *candidateIt;
        
        // Read all three fields for temporal sequence
        const auto lonValue = itemRecord.read(std::string(LonFieldName));
        const auto latValue = itemRecord.read(std::string(LatFieldName));
        const auto timestampValue = itemRecord.read(std::string(TimestampFieldName));
        
        // Use cast to extract values from VarVal
        auto lon = lonValue.cast<nautilus::val<double>>();
        auto lat = latValue.cast<nautilus::val<double>>();
        auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();
        
        // Append point to trajectory string
        trajectoryStr = nautilus::invoke(
            +[](char* buffer, double lonVal, double latVal, int64_t tsVal, int64_t counter) -> char*
            {
                if (counter > 0) {
                    strcat(buffer, ",");
                }
                char pointStr[100];
                sprintf(pointStr, "(%.4f,%.4f)@%ld", lonVal, latVal, tsVal);
                strcat(buffer, pointStr);
                return buffer;
            },
            trajectoryStr,
            lon,
            lat,
            timestamp,
            pointCounter);
            
        pointCounter = pointCounter + nautilus::val<int64_t>(1);
    }
    
    // Close the trajectory string
    trajectoryStr = nautilus::invoke(
        +[](char* buffer) -> char*
        {
            strcat(buffer, "}");
            return buffer;
        },
        trajectoryStr);
    
    // For now, return the string as-is (MEOS conversion needs more complex handling)
    // TODO: Convert to binary MEOS format when needed
    auto strLen = nautilus::invoke(
        +[](const char* str) -> size_t
        {
            return strlen(str);
        },
        trajectoryStr);
        
    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(strLen + nautilus::val<size_t>(1));
    
    // Copy the trajectory string to the result
    nautilus::invoke(
        +[](int8_t* dest, const char* src, size_t len) -> void
        {
            memcpy(dest, src, len + 1);
            free((void*)src);  // Free the temporary buffer
        },
        variableSized.getContent(),
        trajectoryStr,
        strLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);

    return resultRecord;
}

void TemporalSequenceAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

size_t TemporalSequenceAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Nautilus::Interface::PagedVector);
}
void TemporalSequenceAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(
                pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState);
}

// Registry function - TEMPORAL_SEQUENCE requires three separate field functions
// and is created manually in LowerToPhysicalWindowedAggregation.cpp
// This stub is required for the registry system but should not be used
AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterTemporalSequenceAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("TEMPORAL_SEQUENCE aggregation cannot be created through the registry. "
                           "It requires three field functions (longitude, latitude, timestamp) and must be "
                           "created manually in LowerToPhysicalWindowedAggregation.cpp");
}

}
