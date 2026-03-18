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

#include <Operators/Windows/Aggregations/Meos/TemporalExtKalmanFilterAggregationLogicalFunction.hpp>

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

TemporalExtKalmanFilterAggregationLogicalFunction::TemporalExtKalmanFilterAggregationLogicalFunction(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    FieldAccessLogicalFunction asField,
    double gate,
    double q,
    double variance,
    bool toDrop)
    : onField(lonField)
    , asField(std::move(asField))
    , lonField(lonField)
    , latField(latField)
    , timestampField(timestampField)
    , gate(gate)
    , q(q)
    , variance(variance)
    , toDrop(toDrop)
{
}

bool TemporalExtKalmanFilterAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

std::string_view TemporalExtKalmanFilterAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

TemporalExtKalmanFilterAggregationLogicalFunction
TemporalExtKalmanFilterAggregationLogicalFunction::withInferredStamp(const Schema& schema) const
{
    auto newLonField = this->getLonField().withInferredDataType(schema);
    auto newLatField = this->getLatField().withInferredDataType(schema);
    auto newTimestampField = this->getTimestampField().withInferredDataType(schema);

    if (!newLonField.getDataType().isNumeric() || !newLatField.getDataType().isNumeric() || !newTimestampField.getDataType().isNumeric())
    {
        throw CannotDeserialize("TemporalExtKalmanFilterAggregationLogicalFunction: lon, lat, and timestamp fields must be numeric.");
    }

    auto newOnField = newLonField;

    const auto onFieldName = newOnField.getAs<FieldAccessLogicalFunction>()->getFieldName();
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

Reflected TemporalExtKalmanFilterAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<TemporalExtKalmanFilterAggregationLogicalFunction>::operator()(
    const TemporalExtKalmanFilterAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedTemporalExtKalmanFilterAggregationLogicalFunction{
        .onField = function.getOnField(),
        .asField = function.getAsField(),
        .lonField = function.getLonField(),
        .latField = function.getLatField(),
        .timestampField = function.getTimestampField(),
        .gate = function.getGate(),
        .q = function.getQ(),
        .variance = function.getVariance(),
        .toDrop = function.getToDrop()});
}

TemporalExtKalmanFilterAggregationLogicalFunction
Unreflector<TemporalExtKalmanFilterAggregationLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [onField, asField, lonField, latField, timestampField, gate, q, variance, toDrop] =
        unreflect<detail::ReflectedTemporalExtKalmanFilterAggregationLogicalFunction>(reflected);
    return TemporalExtKalmanFilterAggregationLogicalFunction{lonField, latField, timestampField, asField, gate, q, variance, toDrop};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterTemporalExtKalmanFilterAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(
            unreflect<TemporalExtKalmanFilterAggregationLogicalFunction>(arguments.reflected));
    }

    if (arguments.fields.size() == 4)
    {
        return std::make_shared<WindowAggregationLogicalFunction>(
            TemporalExtKalmanFilterAggregationLogicalFunction(
                arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3],
                3.0, 0.01, 1.0, false));
    }
    throw CannotDeserialize(
        "TemporalExtKalmanFilterAggregationLogicalFunction requires lon, lat, timestamp, and alias fields but got {}",
        arguments.fields.size());
}

std::string TemporalExtKalmanFilterAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

DataType TemporalExtKalmanFilterAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType TemporalExtKalmanFilterAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType TemporalExtKalmanFilterAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction TemporalExtKalmanFilterAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction TemporalExtKalmanFilterAggregationLogicalFunction::getAsField() const
{
    return asField;
}

FieldAccessLogicalFunction TemporalExtKalmanFilterAggregationLogicalFunction::getLonField() const
{
    return lonField;
}

FieldAccessLogicalFunction TemporalExtKalmanFilterAggregationLogicalFunction::getLatField() const
{
    return latField;
}

FieldAccessLogicalFunction TemporalExtKalmanFilterAggregationLogicalFunction::getTimestampField() const
{
    return timestampField;
}

double TemporalExtKalmanFilterAggregationLogicalFunction::getGate() const noexcept
{
    return gate;
}

double TemporalExtKalmanFilterAggregationLogicalFunction::getQ() const noexcept
{
    return q;
}

double TemporalExtKalmanFilterAggregationLogicalFunction::getVariance() const noexcept
{
    return variance;
}

bool TemporalExtKalmanFilterAggregationLogicalFunction::getToDrop() const noexcept
{
    return toDrop;
}

TemporalExtKalmanFilterAggregationLogicalFunction
TemporalExtKalmanFilterAggregationLogicalFunction::withInputStamp(DataType newInputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(newInputStamp);
    return copy;
}

TemporalExtKalmanFilterAggregationLogicalFunction
TemporalExtKalmanFilterAggregationLogicalFunction::withPartialAggregateStamp(DataType newPartialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(newPartialAggregateStamp);
    return copy;
}

TemporalExtKalmanFilterAggregationLogicalFunction
TemporalExtKalmanFilterAggregationLogicalFunction::withFinalAggregateStamp(DataType newFinalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(newFinalAggregateStamp);
    return copy;
}

TemporalExtKalmanFilterAggregationLogicalFunction
TemporalExtKalmanFilterAggregationLogicalFunction::withOnField(FieldAccessLogicalFunction newOnField) const
{
    auto copy = *this;
    copy.onField = std::move(newOnField);
    return copy;
}

TemporalExtKalmanFilterAggregationLogicalFunction
TemporalExtKalmanFilterAggregationLogicalFunction::withAsField(FieldAccessLogicalFunction newAsField) const
{
    auto copy = *this;
    copy.asField = std::move(newAsField);
    return copy;
}

bool TemporalExtKalmanFilterAggregationLogicalFunction::operator==(const TemporalExtKalmanFilterAggregationLogicalFunction& other) const
{
    return this->onField == other.onField && this->asField == other.asField
        && this->lonField == other.lonField && this->latField == other.latField
        && this->timestampField == other.timestampField;
}

} // namespace NES
