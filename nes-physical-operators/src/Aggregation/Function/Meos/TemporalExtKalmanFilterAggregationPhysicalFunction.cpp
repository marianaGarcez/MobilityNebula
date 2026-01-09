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

#include <Aggregation/Function/Meos/TemporalExtKalmanFilterAggregationPhysicalFunction.hpp>

#include <Functions/Meos/GeoFunctionMetrics.hpp>
#include <Functions/Meos/GeoOperatorMetrics.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <MEOSWrapper.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <fmt/format.h>
#include <nautilus/function.hpp>
#include <val.hpp>

#include <cmath>
#include <cstring>
#include <mutex>

#include "TrajectoryEventTimeOrdering.hpp"

namespace NES {

constexpr static std::string_view LonFieldName = "lon";
constexpr static std::string_view LatFieldName = "lat";
constexpr static std::string_view TimestampFieldName = "timestamp";

// Mutex for thread-safe MEOS operations shared with other MEOS aggregations
static std::mutex kalman_meos_mutex;

TemporalExtKalmanFilterAggregationPhysicalFunction::TemporalExtKalmanFilterAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction lonFunctionParam,
    PhysicalFunction latFunctionParam,
    PhysicalFunction timestampFunctionParam,
    double gate,
    double q,
    double variance,
    bool toDrop,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType),
                                  std::move(resultType),
                                  lonFunctionParam,
                                  std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , lonFunction(std::move(lonFunctionParam))
    , latFunction(std::move(latFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
    , gate(gate)
    , q(q)
    , variance(variance)
    , toDrop(toDrop) {}

void TemporalExtKalmanFilterAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record) {
    GeoOperatorTimingScope timing(GeoFunctionId::TemporalSequence);
    const auto pagedVectorPtr =
        static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);

    auto lonValue = lonFunction.execute(record, pipelineMemoryProvider.arena);
    auto latValue = latFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    Record aggregateStateRecord({
        {std::string(LonFieldName), lonValue},
        {std::string(LatFieldName), latValue},
        {std::string(TimestampFieldName), timestampValue},
    });

    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    pagedVectorRef.writeRecord(aggregateStateRecord, pipelineMemoryProvider.bufferProvider);
}

void TemporalExtKalmanFilterAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&) {
    const auto memArea1 =
        static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 =
        static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    nautilus::invoke(
        +[](Nautilus::Interface::PagedVector* vec1,
            const Nautilus::Interface::PagedVector* vec2) -> void {
            GeoOperatorTimingScope timing(GeoFunctionId::TemporalSequence);
            vec1->copyFrom(*vec2);
        },
        memArea1,
        memArea2);
}

Nautilus::Record TemporalExtKalmanFilterAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider) {
    MEOS::Meos::ensureMeosInitialized();

    const auto pagedVectorPtr =
        static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    const auto allFieldNames = bufferRef->getMemoryLayout()->getSchema().getFieldNames();

    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector) {
            return pagedVector->getTotalNumberOfEntries();
        },
        pagedVectorPtr);

    if (numberOfEntries == nautilus::val<size_t>(0)) {
        auto emptyVariableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, emptyVariableSized);
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
            GeoOperatorTimingScope timing(GeoFunctionId::TemporalSequence);
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

    auto binarySize = nautilus::invoke(
        +[](const char* trajStr,
            double gateParam,
            double qParam,
            double varianceParam,
            bool toDropParam) -> size_t {
            GeoOperatorTimingScope op_timing(GeoFunctionId::TemporalExtKalmanFilter);
            GeoFunctionTimingScope timing(GeoFunctionId::TemporalExtKalmanFilter);
            if (!trajStr || std::strlen(trajStr) == 0) {
                return 0;
            }

            std::lock_guard<std::mutex> lock(kalman_meos_mutex);
            std::string trajString(trajStr);
            void* rawTemp = MEOS::Meos::parseTemporalPoint(trajString);
            if (!rawTemp) {
                return 0;
            }

            // Apply MEOS-side extended Kalman filter with default parameters
            // Parameters are provided by the logical configuration
            Temporal* filtered = MEOS::Meos::safe_temporal_ext_kalman_filter(
                static_cast<const Temporal*>(rawTemp),
                gateParam,
                qParam,
                varianceParam,
                toDropParam);
            if (!filtered) {
                MEOS::Meos::freeTemporalObject(rawTemp);
                return 0;
            }

            size_t size = 0;
            uint8_t* data = MEOS::Meos::temporalToWKB(filtered, size);
            if (!data) {
                MEOS::Meos::freeTemporalObject(filtered);
                MEOS::Meos::freeTemporalObject(rawTemp);
                return 0;
            }

            MEOS::Meos::freeMeosPointer(data);
            MEOS::Meos::freeTemporalObject(filtered);
            MEOS::Meos::freeTemporalObject(rawTemp);
            return size;
        },
        trajectoryStr,
        nautilus::val<double>(gate),
        nautilus::val<double>(q),
        nautilus::val<double>(variance),
        nautilus::val<bool>(toDrop));

    if (binarySize == nautilus::val<size_t>(0)) {
        auto emptyVariableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, emptyVariableSized);
        return resultRecord;
    }

    auto binaryFormatStr = nautilus::invoke(
        +[](size_t size, const char* trajStr) -> char* {
            char* buffer = static_cast<char*>(malloc(32));
            std::snprintf(buffer, 32, "BINARY(%zu)", size);
            free((void*)trajStr);
            return buffer;
        },
        binarySize,
        trajectoryStr);

    auto formatStrLen = nautilus::invoke(
        +[](const char* str) -> size_t { return std::strlen(str); }, binaryFormatStr);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(formatStrLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* formatStr, size_t len) -> void {
            std::memcpy(dest, formatStr, len);
            free((void*)formatStr);
        },
        variableSized.getContent(),
        binaryFormatStr,
        formatStrLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;
}

void TemporalExtKalmanFilterAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    PipelineMemoryProvider&) {
    nautilus::invoke(
        +[](AggregationState* memArea) -> void {
            auto* pagedVector =
                reinterpret_cast<Nautilus::Interface::PagedVector*>(memArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

size_t TemporalExtKalmanFilterAggregationPhysicalFunction::getSizeOfStateInBytes() const {
    return sizeof(Nautilus::Interface::PagedVector);
}

void TemporalExtKalmanFilterAggregationPhysicalFunction::cleanup(
    nautilus::val<AggregationState*> aggregationState) {
    nautilus::invoke(
        +[](AggregationState* memArea) -> void {
            auto* pagedVector =
                reinterpret_cast<Nautilus::Interface::PagedVector*>(memArea);
            pagedVector->~PagedVector();
        },
        aggregationState);
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::
    RegisterTemporalExtKalmanFilterAggregationPhysicalFunction(
        AggregationPhysicalFunctionRegistryArguments) {
    throw std::runtime_error(
        "TEMPORAL_EXT_KALMAN_FILTER aggregation cannot be created through the registry. "
        "It requires three field functions (longitude, latitude, timestamp)");
}

} // namespace NES
