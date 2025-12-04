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

namespace NES {

constexpr static std::string_view LonFieldName = "lon";
constexpr static std::string_view LatFieldName = "lat";
constexpr static std::string_view TimestampFieldName = "timestamp";

// Mutex for thread-safe MEOS operations shared with other MEOS aggregations
static std::mutex kalman_meos_mutex;

struct MonotonicTimeState {
    bool initialized;
    long long lastTime;
};

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
            const Nautilus::Interface::PagedVector* vec2) -> void { vec1->copyFrom(*vec2); },
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

    auto trajectoryStr = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector) -> char* {
            size_t bufferSize = pagedVector->getTotalNumberOfEntries() * 150 + 50;
            char* buffer = static_cast<char*>(malloc(bufferSize));
            memset(buffer, 0, bufferSize);
            strcpy(buffer, "{");
            return buffer;
        },
        pagedVectorPtr);

    auto pointCounter = nautilus::val<int64_t>(0);

    MonotonicTimeState timeState{};
    timeState.initialized = false;
    timeState.lastTime = 0;
    auto timeStatePtr = nautilus::val<MonotonicTimeState*>(&timeState);

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto it = pagedVectorRef.begin(allFieldNames); it != endIt; ++it) {
        const auto itemRecord = *it;

        const auto lonValue = itemRecord.read(std::string(LonFieldName));
        const auto latValue = itemRecord.read(std::string(LatFieldName));
        const auto timestampValue = itemRecord.read(std::string(TimestampFieldName));

        auto lon = lonValue.cast<nautilus::val<double>>();
        auto lat = latValue.cast<nautilus::val<double>>();
        auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();
        trajectoryStr = nautilus::invoke(
            +[](char* buffer,
                double lonVal,
                double latVal,
                int64_t tsVal,
                int64_t counter,
                MonotonicTimeState* state) -> char* {
                if (counter > 0) {
                    strcat(buffer, ", ");
                }

                long long adjustedTime;
                if (tsVal > 1000000000000LL) {
                    adjustedTime = tsVal / 1000;
                } else {
                    adjustedTime = tsVal;
                }

                if (!state->initialized) {
                    state->initialized = true;
                    state->lastTime = adjustedTime;
                } else {
                    if (adjustedTime <= state->lastTime) {
                        adjustedTime = state->lastTime + 1;
                    }
                    state->lastTime = adjustedTime;
                }

                std::string timestampString =
                    MEOS::Meos::convertSecondsToTimestamp(adjustedTime);
                const char* timestampStr = timestampString.c_str();

                char pointStr[120];
                std::snprintf(pointStr,
                              sizeof(pointStr),
                              "Point(%.6f %.6f)@%s",
                              lonVal,
                              latVal,
                              timestampStr);
                strcat(buffer, pointStr);
                return buffer;
            },
            trajectoryStr,
            lon,
            lat,
            timestamp,
            pointCounter,
            timeStatePtr);

        pointCounter = pointCounter + nautilus::val<int64_t>(1);
    }

    trajectoryStr = nautilus::invoke(
        +[](char* buffer, int64_t) -> char* {
            strcat(buffer, "}");
            return buffer;
        },
        trajectoryStr,
        pointCounter);

    auto binarySize = nautilus::invoke(
        +[](const char* trajStr,
            double gateParam,
            double qParam,
            double varianceParam,
            bool toDropParam) -> size_t {
            if (!trajStr || std::strlen(trajStr) == 0) {
                return 0;
            }

            std::lock_guard<std::mutex> lock(kalman_meos_mutex);
            std::string trajString(trajStr);
            NES_DEBUG("TEMPORAL_EXT_KALMAN_FILTER input trajectory string: {}", trajString);
            void* rawTemp = MEOS::Meos::parseTemporalPoint(trajString);
            if (!rawTemp) {
                return 0;
            }
            size_t beforeHexSize = 0;
            char* beforeHex =
                temporal_as_hexwkb(static_cast<const Temporal*>(rawTemp),
                                   0x08,
                                   &beforeHexSize);
            if (beforeHex) {
                NES_DEBUG(
                    "TEMPORAL_EXT_KALMAN_FILTER MEOS hex WKB before filter (size={}): {}",
                    beforeHexSize,
                    beforeHex);
                free(beforeHex);
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
            size_t afterHexSize = 0;
            char* afterHex =
                temporal_as_hexwkb(filtered,
                                   0x08,
                                   &afterHexSize);
            if (afterHex) {
                NES_DEBUG(
                    "TEMPORAL_EXT_KALMAN_FILTER MEOS hex WKB after filter (size={}): {}",
                    afterHexSize,
                    afterHex);
                free(afterHex);
            }

            size_t size = 0;
            uint8_t* data = MEOS::Meos::temporalToWKB(filtered, size);
            if (!data) {
                MEOS::Meos::freeTemporalObject(filtered);
                MEOS::Meos::freeTemporalObject(rawTemp);
                return 0;
            }

            free(data);
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
