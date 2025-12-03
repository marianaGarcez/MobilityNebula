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

#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

namespace NES {

/**
 * @brief Window aggregation that builds a temporally ordered trajectory
 *        from (lon, lat, ts) and applies a simple Kalman smoothing
 *        before producing a VARSIZED trajectory representation.
 *
 * Signature in SQL:
 *   TEMPORAL_EXT_KALMAN_FILTER(lon, lat, ts) AS trajectory
 */
class TemporalExtKalmanFilterAggregationLogicalFunction : public WindowAggregationLogicalFunction {
public:
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& lonField,
           const FieldAccessLogicalFunction& latField,
           const FieldAccessLogicalFunction& timestampField);

    TemporalExtKalmanFilterAggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField);

    void inferStamp(const Schema& schema) override;
    ~TemporalExtKalmanFilterAggregationLogicalFunction() override = default;

    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool requiresSequentialAggregation() const { return true; }

    // Accessors used when lowering to the physical implementation
    [[nodiscard]] const FieldAccessLogicalFunction& getLonField() const noexcept { return lonField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getLatField() const noexcept { return latField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const noexcept { return timestampField; }

private:
    static constexpr std::string_view NAME = "TemporalExtKalmanFilter";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;

    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
};

} // namespace NES

