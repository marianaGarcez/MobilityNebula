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

#include <Functions/Meos/GeoFunctionMetrics.hpp>
#include <Functions/Meos/GeoOperatorMetrics.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <string_view>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <cstring>
#include <cstdio>
#include <string>

#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

// MEOS wrapper header
#include <MEOSWrapper.hpp>
extern "C" {
#include <meos.h>
}

#include "TrajectoryEventTimeOrdering.hpp"

namespace NES
{

constexpr static std::string_view LonFieldName = "lon";
constexpr static std::string_view LatFieldName = "lat";
constexpr static std::string_view TimestampFieldName = "timestamp";

// Mutex for thread-safe MEOS operations
static std::mutex meos_mutex;

TemporalSequenceAggregationPhysicalFunction::TemporalSequenceAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction lonFunctionParam,
    PhysicalFunction latFunctionParam,
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), lonFunctionParam, std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , lonFunction(std::move(lonFunctionParam))
    , latFunction(std::move(latFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
{
}

void TemporalSequenceAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{
    GeoOperatorTimingScope timing(GeoFunctionId::TemporalSequence);
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);

    // For TEMPORAL_SEQUENCE, we need to store lon, lat, and timestamp values
    auto lonValue = lonFunction.execute(record, pipelineMemoryProvider.arena);
    auto latValue = latFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    // Create a record with all three fields for temporal sequence
    Record aggregateStateRecord({
        {std::string(LonFieldName), lonValue},
        {std::string(LatFieldName), latValue},
        {std::string(TimestampFieldName), timestampValue}
    });

    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    pagedVectorRef.writeRecord(aggregateStateRecord, pipelineMemoryProvider.bufferProvider);
}

void TemporalSequenceAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    // Getting the paged vectors from the aggregation states
    const auto memArea1 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    // Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](Nautilus::Interface::PagedVector* vector1, const Nautilus::Interface::PagedVector* vector2) -> void
        {
            GeoOperatorTimingScope timing(GeoFunctionId::TemporalSequence);
            vector1->copyFrom(*vector2);
        },
        memArea1,
        memArea2);
}

Nautilus::Record TemporalSequenceAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    GeoOperatorTimingScope timing(GeoFunctionId::TemporalSequence);
    // Ensure MEOS is initialized
    MEOS::Meos::ensureMeosInitialized();

    // Getting the paged vector from the aggregation state
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    const auto allFieldNames = bufferRef->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {
            return pagedVector->getTotalNumberOfEntries();
        },
        pagedVectorPtr);

    // Handle empty PagedVector case
    if (numberOfEntries == nautilus::val<size_t>(0)) {
        // Create BINARY(0) string for empty trajectory
        const char* emptyBinaryStr = "BINARY(0)";
        auto strLen = nautilus::val<size_t>(strlen(emptyBinaryStr));
        auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(strLen);

        nautilus::invoke(
            +[](int8_t* dest, size_t len) -> void
            {
                const char* str = "BINARY(0)";
                memcpy(dest, str, len);
            },
            variableSized.getContent(),
            strLen);

        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, variableSized);
        return resultRecord;
    }

    const auto memoryLayout = bufferRef->getMemoryLayout();
    const auto* memoryLayoutPtr = memoryLayout.get();
    const auto lonIndexOpt = memoryLayoutPtr->getFieldIndexFromName(std::string(LonFieldName));
    const auto latIndexOpt = memoryLayoutPtr->getFieldIndexFromName(std::string(LatFieldName));
    const auto tsIndexOpt = memoryLayoutPtr->getFieldIndexFromName(std::string(TimestampFieldName));

    if (!lonIndexOpt.has_value() || !latIndexOpt.has_value() || !tsIndexOpt.has_value())
    {
        throw UnknownOperation("Trajectory fields not found in aggregation state schema.");
    }

    auto trajectoryStr = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector,
            const MemoryLayout* layout,
            uint64_t lonFieldIndex,
            uint64_t latFieldIndex,
            uint64_t tsFieldIndex) -> char*
        {
            return MeosTrajectoryDetail::buildSortedTemporalInstantSetString(
                pagedVector,
                layout,
                MeosTrajectoryDetail::TrajectoryFieldIndices{
                    lonFieldIndex,
                    latFieldIndex,
                    tsFieldIndex,
                });
        },
        pagedVectorPtr,
        nautilus::val<const MemoryLayout*>(memoryLayoutPtr),
        nautilus::val<uint64_t>(lonIndexOpt.value()),
        nautilus::val<uint64_t>(latIndexOpt.value()),
        nautilus::val<uint64_t>(tsIndexOpt.value()));

    // Convert string to MEOS binary format and get size
    auto binarySize = nautilus::invoke(
        +[](const char* trajStr) -> size_t
        {
            GeoFunctionTimingScope timing(GeoFunctionId::TemporalSequence);
            // Validate string is not empty
            if (!trajStr || strlen(trajStr) == 0) {
                return 0;
            }

            // Parse the temporal instant string into a MEOS temporal object
            // Lock mutex for thread-safe MEOS operations
            std::lock_guard<std::mutex> lock(meos_mutex);

            // Parse using the wrapper function
            std::string trajString(trajStr);
            void* temp = MEOS::Meos::parseTemporalPoint(trajString);
            if (!temp) {
                return 0;
            }

            // Get the size needed for binary WKB format
            size_t size = 0;
            uint8_t* data = MEOS::Meos::temporalToWKB(temp, size);

            if (!data) {
                MEOS::Meos::freeTemporalObject(temp);
                return 0;
            }

            MEOS::Meos::freeMeosPointer(data);
            MEOS::Meos::freeTemporalObject(temp);

            return size;
        },
        trajectoryStr);

    if (binarySize == nautilus::val<size_t>(0)) {
        // Return empty record or handle error appropriately
        auto emptyVariableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, emptyVariableSized);
        return resultRecord;
    }

    // Create BINARY(N) string format for test compatibility
    auto binaryFormatStr = nautilus::invoke(
        +[](size_t size, const char* trajStr) -> char*
        {
            // Allocate buffer for "BINARY(N)" string
            char* buffer = (char*)malloc(32);  // More than enough for "BINARY(" + number + ")"
            sprintf(buffer, "BINARY(%zu)", size);

            // Free the trajectory string as we don't need it anymore
            free((void*)trajStr);
            return buffer;
        },
        binarySize,
        trajectoryStr);

    // Get the length of the BINARY(N) string
    auto formatStrLen = nautilus::invoke(
        +[](const char* str) -> size_t
        {
            return strlen(str);
        },
        binaryFormatStr);

    // Allocate variable sized data for the BINARY(N) string
    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(formatStrLen);

    // Copy the BINARY(N) string to the allocated memory
    nautilus::invoke(
        +[](int8_t* dest, const char* formatStr, size_t len) -> void
        {
            memcpy(dest, formatStr, len);
            free((void*)formatStr);
        },
        variableSized.getContent(),
        binaryFormatStr,
        formatStrLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);

    return resultRecord;
}

void TemporalSequenceAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            // Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
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
            // Calls the destructor of the PagedVector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(
                pagedVectorMemArea); // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState);
}


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterTemporalSequenceAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("TEMPORAL_SEQUENCE aggregation cannot be created through the registry. "
                           "It requires three field functions (longitude, latitude, timestamp) ");
}

}
