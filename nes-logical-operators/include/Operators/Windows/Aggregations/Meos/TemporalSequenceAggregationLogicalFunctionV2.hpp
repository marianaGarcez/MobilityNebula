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

class TemporalSequenceAggregationLogicalFunctionV2 final
{
public:
    TemporalSequenceAggregationLogicalFunctionV2(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        FieldAccessLogicalFunction asField);

    static TemporalSequenceAggregationLogicalFunctionV2
    create(const FieldAccessLogicalFunction& lonField, const FieldAccessLogicalFunction& latField, const FieldAccessLogicalFunction& timestampField);

    ~TemporalSequenceAggregationLogicalFunctionV2() = default;

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

    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunctionV2 withInferredStamp(const Schema& schema) const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunctionV2 withInputStamp(DataType inputStamp) const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunctionV2 withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunctionV2 withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunctionV2 withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] TemporalSequenceAggregationLogicalFunctionV2 withAsField(FieldAccessLogicalFunction asField) const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    [[nodiscard]] bool operator==(const TemporalSequenceAggregationLogicalFunctionV2& other) const;

private:
    static constexpr std::string_view NAME = "TemporalSequence";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
};

static_assert(WindowAggregationFunctionConcept<TemporalSequenceAggregationLogicalFunctionV2>);

template <>
struct Reflector<TemporalSequenceAggregationLogicalFunctionV2>
{
    Reflected operator()(const TemporalSequenceAggregationLogicalFunctionV2& function) const;
};

template <>
struct Unreflector<TemporalSequenceAggregationLogicalFunctionV2>
{
    TemporalSequenceAggregationLogicalFunctionV2 operator()(const Reflected& reflected) const;
};
}

namespace NES::detail
{
struct ReflectedTemporalSequenceAggregationLogicalFunctionV2
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
};

}
