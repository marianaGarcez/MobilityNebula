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

#include <Operators/Windows/Aggregations/ArrayAggregationLogicalFunction.hpp>

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
ArrayAggregationLogicalFunction::ArrayAggregationLogicalFunction(const FieldAccessLogicalFunction& field) : onField(field), asField(field)
{
}

ArrayAggregationLogicalFunction::ArrayAggregationLogicalFunction(const FieldAccessLogicalFunction& field, FieldAccessLogicalFunction asField)
    : onField(field), asField(std::move(asField))
{
}

bool ArrayAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

std::string_view ArrayAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

ArrayAggregationLogicalFunction ArrayAggregationLogicalFunction::withInferredStamp(const Schema& schema) const
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    auto newOnField = this->getOnField().withInferredDataType(schema);
    if (not newOnField.getDataType().isNumeric())
    {
        throw CannotDeserialize("aggregations on non numeric fields is not supported.");
    }

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
    return this->withOnField(newOnField.getAs<FieldAccessLogicalFunction>().get())
        .withFinalAggregateStamp(newFinalAggregateStamp)
        .withAsField(this->getAsField().withFieldName(newAsFieldName).withDataType(newFinalAggregateStamp))
        .withInputStamp(newOnField.getDataType());
}

Reflected ArrayAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<ArrayAggregationLogicalFunction>::operator()(const ArrayAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedArrayAggregationLogicalFunction{.onField = function.getOnField(), .asField = function.getAsField()});
}

ArrayAggregationLogicalFunction Unreflector<ArrayAggregationLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [onField, asField] = unreflect<detail::ReflectedArrayAggregationLogicalFunction>(reflected);
    return ArrayAggregationLogicalFunction{onField, asField};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterArray_AggAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<ArrayAggregationLogicalFunction>(arguments.reflected));
    }

    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("ArrayAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<WindowAggregationLogicalFunction>(ArrayAggregationLogicalFunction(arguments.fields[0], arguments.fields[1]));
}

std::string ArrayAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

DataType ArrayAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType ArrayAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType ArrayAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction ArrayAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction ArrayAggregationLogicalFunction::getAsField() const
{
    return asField;
}

ArrayAggregationLogicalFunction ArrayAggregationLogicalFunction::withInputStamp(DataType inputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(inputStamp);
    return copy;
}

ArrayAggregationLogicalFunction ArrayAggregationLogicalFunction::withPartialAggregateStamp(DataType partialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(partialAggregateStamp);
    return copy;
}

ArrayAggregationLogicalFunction ArrayAggregationLogicalFunction::withFinalAggregateStamp(DataType finalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(finalAggregateStamp);
    return copy;
}

ArrayAggregationLogicalFunction ArrayAggregationLogicalFunction::withOnField(FieldAccessLogicalFunction onField) const
{
    auto copy = *this;
    copy.onField = std::move(onField);
    return copy;
}

ArrayAggregationLogicalFunction ArrayAggregationLogicalFunction::withAsField(FieldAccessLogicalFunction asField) const
{
    auto copy = *this;
    copy.asField = std::move(asField);
    return copy;
}

bool ArrayAggregationLogicalFunction::operator==(const ArrayAggregationLogicalFunction& other) const
{
    return this->onField == other.onField && this->asField == other.asField;
}
}
