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

#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/**
 * @brief Window aggregation that builds a temporally ordered trajectory
 *        from (lon, lat, ts) and applies a simple Kalman smoothing
 *        before producing a VARSIZED trajectory representation.
 *
 * Signature in SQL:
 *   TEMPORAL_EXT_KALMAN_FILTER(lon, lat, ts [, gate, q, variance, to_drop]) AS trajectory
 */
class TemporalExtKalmanFilterAggregationLogicalFunction final
{
public:
    TemporalExtKalmanFilterAggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        FieldAccessLogicalFunction asField,
        double gate = 3.0,
        double q = 0.01,
        double variance = 1.0,
        bool toDrop = false);

    ~TemporalExtKalmanFilterAggregationLogicalFunction() = default;

    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;
    [[nodiscard]] FieldAccessLogicalFunction getLonField() const;
    [[nodiscard]] FieldAccessLogicalFunction getLatField() const;
    [[nodiscard]] FieldAccessLogicalFunction getTimestampField() const;
    [[nodiscard]] double getGate() const noexcept;
    [[nodiscard]] double getQ() const noexcept;
    [[nodiscard]] double getVariance() const noexcept;
    [[nodiscard]] bool getToDrop() const noexcept;

    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] TemporalExtKalmanFilterAggregationLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] TemporalExtKalmanFilterAggregationLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] TemporalExtKalmanFilterAggregationLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] TemporalExtKalmanFilterAggregationLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] TemporalExtKalmanFilterAggregationLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] TemporalExtKalmanFilterAggregationLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    [[nodiscard]] bool operator==(const TemporalExtKalmanFilterAggregationLogicalFunction& other) const;

private:
    static constexpr std::string_view NAME = "TemporalExtKalmanFilter";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
    double gate;
    double q;
    double variance;
    bool toDrop;
};

static_assert(WindowAggregationFunctionConcept<TemporalExtKalmanFilterAggregationLogicalFunction>);

template <>
struct Reflector<TemporalExtKalmanFilterAggregationLogicalFunction>
{
    Reflected operator()(const TemporalExtKalmanFilterAggregationLogicalFunction& function) const;
};

template <>
struct Unreflector<TemporalExtKalmanFilterAggregationLogicalFunction>
{
    TemporalExtKalmanFilterAggregationLogicalFunction operator()(const Reflected& reflected) const;
};
}

namespace NES::detail
{
struct ReflectedTemporalExtKalmanFilterAggregationLogicalFunction
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
    double gate;
    double q;
    double variance;
    bool toDrop;
};

}
