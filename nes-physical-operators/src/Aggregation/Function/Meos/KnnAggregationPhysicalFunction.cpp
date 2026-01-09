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

#include <Aggregation/Function/Meos/KnnAggregationPhysicalFunction.hpp>

#include <Functions/Meos/GeoOperatorMetrics.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <val.hpp>

#include <algorithm>
#include <cstring>

namespace NES
{

namespace
{
constexpr std::size_t MAX_K = 16;

struct KnnState
{
    std::uint64_t count;
    double distances[MAX_K];
    std::uint64_t ids[MAX_K];
};

inline void insertCandidate(KnnState& state, double distance, std::uint64_t id, std::uint64_t k)
{
    const auto limit = std::min<std::uint64_t>(k, MAX_K);
    auto used = static_cast<std::size_t>(state.count);
    if (used == 0)
    {
        state.distances[0] = distance;
        state.ids[0] = id;
        state.count = 1;
        return;
    }

    // Find insertion position (sorted ascending by distance)
    std::size_t pos = 0;
    while (pos < used && state.distances[pos] <= distance)
    {
        ++pos;
    }

    if (used < limit)
    {
        // Shift right and insert
        for (std::size_t i = used; i > pos; --i)
        {
            state.distances[i] = state.distances[i - 1];
            state.ids[i] = state.ids[i - 1];
        }
        state.distances[pos] = distance;
        state.ids[pos] = id;
        state.count = static_cast<std::uint64_t>(used + 1);
    }
    else if (pos < limit)
    {
        // Insert and drop worst element
        for (std::size_t i = limit - 1; i > pos; --i)
        {
            state.distances[i] = state.distances[i - 1];
            state.ids[i] = state.ids[i - 1];
        }
        state.distances[pos] = distance;
        state.ids[pos] = id;
    }
}
} // namespace

KnnAggregationPhysicalFunction::KnnAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction distanceFunction,
    PhysicalFunction neighbourIdFunction,
    std::uint64_t k,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(distanceFunction), std::move(resultFieldIdentifier))
    , neighbourIdFunction(std::move(neighbourIdFunction))
    , k(k)
{
    PRECONDITION(k > 0, "KNN_AGG requires k > 0");
}

void KnnAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record)
{
    const auto distanceValue = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto neighbourValue = neighbourIdFunction.execute(record, pipelineMemoryProvider.arena);

    const auto distance = distanceValue.cast<nautilus::val<double>>();
    const auto neighbourId = neighbourValue.cast<nautilus::val<std::uint64_t>>();

    nautilus::invoke(
        +[](AggregationState* rawState, double dist, std::uint64_t id, std::uint64_t kValue)
        {
            GeoOperatorTimingScope timing(GeoFunctionId::KnnAgg);
            auto* state = reinterpret_cast<KnnState*>(rawState);
            insertCandidate(*state, dist, id, kValue);
        },
        aggregationState,
        distance,
        neighbourId,
        nautilus::val<std::uint64_t>(k));
}

void KnnAggregationPhysicalFunction::combine(
    nautilus::val<AggregationState*> aggregationState1,
    nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* raw1, AggregationState* raw2, std::uint64_t kValue)
        {
            GeoOperatorTimingScope timing(GeoFunctionId::KnnAgg);
            auto* s1 = reinterpret_cast<KnnState*>(raw1);
            auto* s2 = reinterpret_cast<KnnState*>(raw2);
            const auto count2 = static_cast<std::size_t>(s2->count);
            for (std::size_t i = 0; i < count2; ++i)
            {
                insertCandidate(*s1, s2->distances[i], s2->ids[i], kValue);
            }
        },
        aggregationState1,
        aggregationState2,
        nautilus::val<std::uint64_t>(k));
}

Nautilus::Record KnnAggregationPhysicalFunction::lower(
    nautilus::val<AggregationState*> aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    auto neighboursStr = nautilus::invoke(
        +[](AggregationState* rawState, std::uint64_t kValue) -> char*
        {
            GeoOperatorTimingScope timing(GeoFunctionId::KnnAgg);
            auto* state = reinterpret_cast<KnnState*>(rawState);
            if (state->count == 0)
            {
                auto* empty = static_cast<char*>(std::malloc(1));
                if (empty)
                {
                    empty[0] = '\0';
                }
                return empty;
            }

            const auto used = static_cast<std::size_t>(std::min<std::uint64_t>(state->count, kValue));

            std::size_t len = 0;
            for (std::size_t i = 0; i < used; ++i)
            {
                len += static_cast<std::size_t>(
                    std::snprintf(nullptr, 0, "%llu:%.6f;", static_cast<unsigned long long>(state->ids[i]), state->distances[i]));
            }

            auto* buffer = static_cast<char*>(std::malloc(len + 1));
            if (!buffer)
            {
                return static_cast<char*>(nullptr);
            }

            char* ptr = buffer;
            std::size_t remaining = len + 1;
            for (std::size_t i = 0; i < used; ++i)
            {
                const auto written = std::snprintf(
                    ptr,
                    remaining,
                    "%llu:%.6f;",
                    static_cast<unsigned long long>(state->ids[i]),
                    state->distances[i]);
                ptr += written;
                remaining -= static_cast<std::size_t>(written);
            }

            return buffer;
        },
        aggregationState,
        nautilus::val<std::uint64_t>(k));

    if (!neighboursStr)
    {
        auto emptyVariableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, emptyVariableSized);
        return resultRecord;
    }

    auto neighboursLen = nautilus::invoke(
        +[](const char* str) -> std::size_t { return std::strlen(str); }, neighboursStr);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(neighboursLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* src, std::size_t len) -> void
        {
            if (len > 0)
            {
                std::memcpy(dest, src, len);
            }
            std::free(const_cast<char*>(src));
        },
        variableSized.getContent(),
        neighboursStr,
        neighboursLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;
}

void KnnAggregationPhysicalFunction::reset(
    nautilus::val<AggregationState*> aggregationState,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* rawState)
        {
            auto* state = reinterpret_cast<KnnState*>(rawState);
            state->count = 0;
        },
        aggregationState);
}

size_t KnnAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(KnnState);
}

void KnnAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
    // Nothing to clean up; KnnState is trivially destructible
}

AggregationPhysicalFunctionRegistryReturnType
AggregationPhysicalFunctionGeneratedRegistrar::RegisterKnnAggAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error(
        "KNN_AGG aggregation cannot be created through the registry. "
        "It requires both distance and neighbour functions and an explicit k parameter.");
}

} // namespace NES
