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

class TemporalSequenceAggregationLogicalFunction
{
public:
    /// TEMPORAL_SEQUENCE requires three fields: longitude, latitude, and timestamp
    TemporalSequenceAggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField);

    TemporalSequenceAggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        FieldAccessLogicalFunction asField);

    ~TemporalSequenceAggregationLogicalFunction() = default;

    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    [[nodiscard]] TemporalSequenceAggregationLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] bool operator==(const TemporalSequenceAggregationLogicalFunction& other) const;

    [[nodiscard]] const FieldAccessLogicalFunction& getLonField() const { return lonField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getLatField() const { return latField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const { return timestampField; }

private:
    static constexpr std::string_view NAME = "TemporalSequence";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;  // lonField serves as onField
    FieldAccessLogicalFunction asField;
    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
};

static_assert(WindowAggregationFunctionConcept<TemporalSequenceAggregationLogicalFunction>);

template <>
struct Reflector<TemporalSequenceAggregationLogicalFunction>
{
    Reflected operator()(const TemporalSequenceAggregationLogicalFunction& function) const;
};

template <>
struct Unreflector<TemporalSequenceAggregationLogicalFunction>
{
    TemporalSequenceAggregationLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedTemporalSequenceAggregationLogicalFunction
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
};
}
