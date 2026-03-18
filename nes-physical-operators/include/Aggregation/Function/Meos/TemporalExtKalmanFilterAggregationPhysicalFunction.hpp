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

#pragma once

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES {

/**
 * @brief Physical implementation of TEMPORAL_EXT_KALMAN_FILTER(lon, lat, ts)
 *
 * Collects lon/lat/ts values in a PagedVector, applies a simple Kalman smoothing
 * over the ordered trajectory and finally materializes a MEOS-compatible
 * temporal instant set, returning a VARSIZED label of the form BINARY(N)
 * where N is the WKB size of the filtered trajectory.
 */
class TemporalExtKalmanFilterAggregationPhysicalFunction : public AggregationPhysicalFunction {
public:
    TemporalExtKalmanFilterAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction lonFunctionParam,
        PhysicalFunction latFunctionParam,
        PhysicalFunction timestampFunctionParam,
        double gate,
        double q,
        double variance,
        bool toDrop,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<TupleBufferRef> bufferRef);

    void lift(const nautilus::val<AggregationState*>& aggregationState,
              PipelineMemoryProvider& pipelineMemoryProvider,
              const Record& record) override;

    void combine(nautilus::val<AggregationState*> aggregationState1,
                 nautilus::val<AggregationState*> aggregationState2,
                 PipelineMemoryProvider& pipelineMemoryProvider) override;

    Record lower(nautilus::val<AggregationState*> aggregationState,
                           PipelineMemoryProvider& pipelineMemoryProvider) override;

    void reset(nautilus::val<AggregationState*> aggregationState,
               PipelineMemoryProvider& pipelineMemoryProvider) override;

    [[nodiscard]] size_t getSizeOfStateInBytes() const override;

    void cleanup(nautilus::val<AggregationState*> aggregationState) override;

    ~TemporalExtKalmanFilterAggregationPhysicalFunction() override = default;

private:
    std::shared_ptr<TupleBufferRef> bufferRef;
    PhysicalFunction lonFunction;
    PhysicalFunction latFunction;
    PhysicalFunction timestampFunction;
    double gate;
    double q;
    double variance;
    bool toDrop;
};

} // namespace NES
