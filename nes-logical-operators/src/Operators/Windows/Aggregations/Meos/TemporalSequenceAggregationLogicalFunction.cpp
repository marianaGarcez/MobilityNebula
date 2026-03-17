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

#include <Operators/Windows/Aggregations/Meos/TemporalSequenceAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>

namespace NES
{

TemporalSequenceAggregationLogicalFunction::TemporalSequenceAggregationLogicalFunction(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField)
    : onField(lonField), asField(lonField), lonField(lonField), latField(latField), timestampField(timestampField)
{
}

TemporalSequenceAggregationLogicalFunction::TemporalSequenceAggregationLogicalFunction(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    FieldAccessLogicalFunction asField)
    : onField(lonField), asField(std::move(asField)), lonField(lonField), latField(latField), timestampField(timestampField)
{
}

std::string_view TemporalSequenceAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

bool TemporalSequenceAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return false;
}

TemporalSequenceAggregationLogicalFunction TemporalSequenceAggregationLogicalFunction::withInferredStamp(const Schema& schema) const
{
    auto newLonField = this->lonField.withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get();
    auto newLatField = this->latField.withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get();
    auto newTsField = this->timestampField.withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get();

    if (!newLonField.getDataType().isNumeric() || !newLatField.getDataType().isNumeric() || !newTsField.getDataType().isNumeric())
    {
        throw CannotDeserialize("TEMPORAL_SEQUENCE: lon, lat, and timestamp fields must be numeric");
    }

    const auto onFieldName = newLonField.getFieldName();
    const auto asFieldName = this->getAsField().getFieldName();
    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);

    std::string newAsFieldName;
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        newAsFieldName = attributeNameResolver + asFieldName;
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        newAsFieldName = attributeNameResolver + fieldName;
    }

    const auto newFinalStamp = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);

    auto result = *this;
    result.lonField = newLonField;
    result.latField = newLatField;
    result.timestampField = newTsField;
    result.onField = newLonField;
    result.inputStamp = newLonField.getDataType();
    result.finalAggregateStamp = newFinalStamp;
    result.asField = this->getAsField().withFieldName(newAsFieldName).withDataType(newFinalStamp);
    return result;
}

Reflected TemporalSequenceAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<TemporalSequenceAggregationLogicalFunction>::operator()(const TemporalSequenceAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedTemporalSequenceAggregationLogicalFunction{
        .onField = function.getOnField(),
        .asField = function.getAsField(),
        .latField = function.getLatField(),
        .timestampField = function.getTimestampField()});
}

TemporalSequenceAggregationLogicalFunction Unreflector<TemporalSequenceAggregationLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [onField, asField, latField, timestampField] = unreflect<detail::ReflectedTemporalSequenceAggregationLogicalFunction>(reflected);
    return TemporalSequenceAggregationLogicalFunction{onField, latField, timestampField, asField};
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterTemporalSequenceAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<TemporalSequenceAggregationLogicalFunction>(arguments.reflected));
    }
    if (arguments.fields.size() == 4)
    {
        return std::make_shared<WindowAggregationLogicalFunction>(
            TemporalSequenceAggregationLogicalFunction(arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3]));
    }
    throw CannotDeserialize("TemporalSequenceAggregationLogicalFunction requires lon, lat, timestamp, and alias fields but got {}", arguments.fields.size());
}

std::string TemporalSequenceAggregationLogicalFunction::toString() const
{
    return fmt::format("TemporalSequence: lonField={} latField={} tsField={} asField={}", onField, latField, timestampField, asField);
}

DataType TemporalSequenceAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType TemporalSequenceAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType TemporalSequenceAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction TemporalSequenceAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction TemporalSequenceAggregationLogicalFunction::getAsField() const
{
    return asField;
}

TemporalSequenceAggregationLogicalFunction TemporalSequenceAggregationLogicalFunction::withInputStamp(DataType stamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(stamp);
    return copy;
}

TemporalSequenceAggregationLogicalFunction TemporalSequenceAggregationLogicalFunction::withPartialAggregateStamp(DataType stamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(stamp);
    return copy;
}

TemporalSequenceAggregationLogicalFunction TemporalSequenceAggregationLogicalFunction::withFinalAggregateStamp(DataType stamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(stamp);
    return copy;
}

TemporalSequenceAggregationLogicalFunction TemporalSequenceAggregationLogicalFunction::withOnField(FieldAccessLogicalFunction field) const
{
    auto copy = *this;
    copy.onField = std::move(field);
    return copy;
}

TemporalSequenceAggregationLogicalFunction TemporalSequenceAggregationLogicalFunction::withAsField(FieldAccessLogicalFunction field) const
{
    auto copy = *this;
    copy.asField = std::move(field);
    return copy;
}

bool TemporalSequenceAggregationLogicalFunction::operator==(const TemporalSequenceAggregationLogicalFunction& other) const
{
    return this->onField == other.onField && this->asField == other.asField
        && this->latField == other.latField && this->timestampField == other.timestampField;
}

} // namespace NES
