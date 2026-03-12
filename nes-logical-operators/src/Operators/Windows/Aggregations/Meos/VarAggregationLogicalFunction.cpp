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

#include <Operators/Windows/Aggregations/Meos/VarAggregationLogicalFunction.hpp>

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
VarAggregationLogicalFunction::VarAggregationLogicalFunction(const FieldAccessLogicalFunction& field) : onField(field), asField(field)
{
}

VarAggregationLogicalFunction::VarAggregationLogicalFunction(const FieldAccessLogicalFunction& field, FieldAccessLogicalFunction asField)
    : onField(field), asField(std::move(asField))
{
}

bool VarAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

std::string_view VarAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

VarAggregationLogicalFunction VarAggregationLogicalFunction::withInferredStamp(const Schema& schema) const
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    auto newOnField = this->getOnField().withInferredDataType(schema);
    if (not newOnField.getDataType().isNumeric())
    {
        throw CannotDeserialize("aggregations on non numeric fields is not supported.");
    }

    /// For variance calculation, we need to store sum, sum of squares, and count
    /// Cast the input to FLOAT64 to avoid overflow and ensure precision
    newOnField = newOnField.withDataType(DataTypeProvider::provideDataType(
        DataType::Type::FLOAT64, newOnField.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE));

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
        DataType::Type::FLOAT64, newOnField.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    return this->withOnField(newOnField.getAs<FieldAccessLogicalFunction>().get())
        .withFinalAggregateStamp(newFinalAggregateStamp)
        .withAsField(this->getAsField().withFieldName(newAsFieldName).withDataType(newFinalAggregateStamp))
        .withInputStamp(newOnField.getDataType());
}

Reflected VarAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<VarAggregationLogicalFunction>::operator()(const VarAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedVarAggregationLogicalFunction{.onField = function.getOnField(), .asField = function.getAsField()});
}

VarAggregationLogicalFunction Unreflector<VarAggregationLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [onField, asField] = unreflect<detail::ReflectedVarAggregationLogicalFunction>(reflected);
    return VarAggregationLogicalFunction{onField, asField};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterVarAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<VarAggregationLogicalFunction>(arguments.reflected));
    }

    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("VarAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<WindowAggregationLogicalFunction>(VarAggregationLogicalFunction(arguments.fields[0], arguments.fields[1]));
}

std::string VarAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

DataType VarAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType VarAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType VarAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction VarAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction VarAggregationLogicalFunction::getAsField() const
{
    return asField;
}

VarAggregationLogicalFunction VarAggregationLogicalFunction::withInputStamp(DataType inputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(inputStamp);
    return copy;
}

VarAggregationLogicalFunction VarAggregationLogicalFunction::withPartialAggregateStamp(DataType partialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(partialAggregateStamp);
    return copy;
}

VarAggregationLogicalFunction VarAggregationLogicalFunction::withFinalAggregateStamp(DataType finalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(finalAggregateStamp);
    return copy;
}

VarAggregationLogicalFunction VarAggregationLogicalFunction::withOnField(FieldAccessLogicalFunction onField) const
{
    auto copy = *this;
    copy.onField = std::move(onField);
    return copy;
}

VarAggregationLogicalFunction VarAggregationLogicalFunction::withAsField(FieldAccessLogicalFunction asField) const
{
    auto copy = *this;
    copy.asField = std::move(asField);
    return copy;
}

bool VarAggregationLogicalFunction::operator==(const VarAggregationLogicalFunction& other) const
{
    return this->onField == other.onField && this->asField == other.asField;
}
}
