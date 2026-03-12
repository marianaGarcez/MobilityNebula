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

#include <Operators/Windows/Aggregations/Meos/TemporalSequenceAggregationLogicalFunctionV2.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
TemporalSequenceAggregationLogicalFunctionV2::TemporalSequenceAggregationLogicalFunctionV2(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    FieldAccessLogicalFunction asField)
    : onField(lonField), asField(std::move(asField)), lonField(lonField), latField(latField), timestampField(timestampField)
{
}

TemporalSequenceAggregationLogicalFunctionV2 TemporalSequenceAggregationLogicalFunctionV2::create(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField)
{
    /// Default alias to lonField; will be adjusted in withInferredStamp
    return TemporalSequenceAggregationLogicalFunctionV2(lonField, latField, timestampField, lonField);
}

bool TemporalSequenceAggregationLogicalFunctionV2::shallIncludeNullValues() noexcept
{
    return true;
}

std::string_view TemporalSequenceAggregationLogicalFunctionV2::getName() noexcept
{
    return NAME;
}

TemporalSequenceAggregationLogicalFunctionV2 TemporalSequenceAggregationLogicalFunctionV2::withInferredStamp(const Schema& schema) const
{
    /// Infer all three coordinate/timestamp fields
    auto newLonField = this->getLonField().withInferredDataType(schema);
    auto newLatField = this->getLatField().withInferredDataType(schema);
    auto newTimestampField = this->getTimestampField().withInferredDataType(schema);

    if (!newLonField.getDataType().isNumeric() || !newLatField.getDataType().isNumeric() || !newTimestampField.getDataType().isNumeric())
    {
        throw CannotDeserialize("TemporalSequenceAggregationLogicalFunction: lon, lat, and timestamp fields must be numeric.");
    }

    /// Use lonField as the onField
    auto newOnField = newLonField;

    ///Set fully qualified name for the as Field
    const auto onFieldName = newOnField.getAs<FieldAccessLogicalFunction>()->getFieldName();
    const auto asFieldName = this->getAsField().getFieldName();
    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);

    std::string newAsFieldName;
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        newAsFieldName = attributeNameResolver + asFieldName;
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        newAsFieldName = attributeNameResolver + fieldName;
    }

    const auto newFinalAggregateStamp = DataTypeProvider::provideDataType(
        DataType::Type::VARSIZED, newOnField.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);

    auto result = *this;
    result.lonField = newLonField.getAs<FieldAccessLogicalFunction>().get();
    result.latField = newLatField.getAs<FieldAccessLogicalFunction>().get();
    result.timestampField = newTimestampField.getAs<FieldAccessLogicalFunction>().get();
    result.onField = newOnField.getAs<FieldAccessLogicalFunction>().get();
    result.finalAggregateStamp = newFinalAggregateStamp;
    result.asField = this->getAsField().withFieldName(newAsFieldName).withDataType(newFinalAggregateStamp);
    result.inputStamp = newOnField.getDataType();
    return result;
}

Reflected TemporalSequenceAggregationLogicalFunctionV2::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<TemporalSequenceAggregationLogicalFunctionV2>::operator()(const TemporalSequenceAggregationLogicalFunctionV2& function) const
{
    return reflect(detail::ReflectedTemporalSequenceAggregationLogicalFunctionV2{
        .onField = function.getOnField(),
        .asField = function.getAsField(),
        .lonField = function.getLonField(),
        .latField = function.getLatField(),
        .timestampField = function.getTimestampField()});
}

TemporalSequenceAggregationLogicalFunctionV2
Unreflector<TemporalSequenceAggregationLogicalFunctionV2>::operator()(const Reflected& reflected) const
{
    auto [onField, asField, lonField, latField, timestampField] =
        unreflect<detail::ReflectedTemporalSequenceAggregationLogicalFunctionV2>(reflected);
    return TemporalSequenceAggregationLogicalFunctionV2{lonField, latField, timestampField, asField};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterTemporalSequenceAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(
            unreflect<TemporalSequenceAggregationLogicalFunctionV2>(arguments.reflected));
    }

    if (arguments.fields.size() != 4)
    {
        throw CannotDeserialize(
            "TemporalSequenceAggregationLogicalFunction requires lon, lat, timestamp, and alias fields but got {}",
            arguments.fields.size());
    }
    return std::make_shared<WindowAggregationLogicalFunction>(
        TemporalSequenceAggregationLogicalFunctionV2(arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3]));
}

std::string TemporalSequenceAggregationLogicalFunctionV2::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

DataType TemporalSequenceAggregationLogicalFunctionV2::getInputStamp() const
{
    return inputStamp;
}

DataType TemporalSequenceAggregationLogicalFunctionV2::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType TemporalSequenceAggregationLogicalFunctionV2::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction TemporalSequenceAggregationLogicalFunctionV2::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction TemporalSequenceAggregationLogicalFunctionV2::getAsField() const
{
    return asField;
}

FieldAccessLogicalFunction TemporalSequenceAggregationLogicalFunctionV2::getLonField() const
{
    return lonField;
}

FieldAccessLogicalFunction TemporalSequenceAggregationLogicalFunctionV2::getLatField() const
{
    return latField;
}

FieldAccessLogicalFunction TemporalSequenceAggregationLogicalFunctionV2::getTimestampField() const
{
    return timestampField;
}

TemporalSequenceAggregationLogicalFunctionV2 TemporalSequenceAggregationLogicalFunctionV2::withInputStamp(DataType inputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(inputStamp);
    return copy;
}

TemporalSequenceAggregationLogicalFunctionV2 TemporalSequenceAggregationLogicalFunctionV2::withPartialAggregateStamp(DataType partialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(partialAggregateStamp);
    return copy;
}

TemporalSequenceAggregationLogicalFunctionV2 TemporalSequenceAggregationLogicalFunctionV2::withFinalAggregateStamp(DataType finalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(finalAggregateStamp);
    return copy;
}

TemporalSequenceAggregationLogicalFunctionV2 TemporalSequenceAggregationLogicalFunctionV2::withOnField(FieldAccessLogicalFunction onField) const
{
    auto copy = *this;
    copy.onField = std::move(onField);
    return copy;
}

TemporalSequenceAggregationLogicalFunctionV2 TemporalSequenceAggregationLogicalFunctionV2::withAsField(FieldAccessLogicalFunction asField) const
{
    auto copy = *this;
    copy.asField = std::move(asField);
    return copy;
}

bool TemporalSequenceAggregationLogicalFunctionV2::operator==(const TemporalSequenceAggregationLogicalFunctionV2& other) const
{
    return this->onField == other.onField && this->asField == other.asField && this->lonField == other.lonField
        && this->latField == other.latField && this->timestampField == other.timestampField;
}
}
