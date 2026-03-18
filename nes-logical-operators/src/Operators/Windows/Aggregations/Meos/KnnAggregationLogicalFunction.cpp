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

#include <Operators/Windows/Aggregations/Meos/KnnAggregationLogicalFunction.hpp>

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

KnnAggregationLogicalFunction::KnnAggregationLogicalFunction(
    const FieldAccessLogicalFunction& distanceField,
    const FieldAccessLogicalFunction& neighbourField,
    FieldAccessLogicalFunction asField,
    std::size_t k)
    : onField(distanceField)
    , asField(std::move(asField))
    , distanceField(distanceField)
    , neighbourField(neighbourField)
    , k(k)
{
}

bool KnnAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

std::string_view KnnAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

KnnAggregationLogicalFunction KnnAggregationLogicalFunction::withInferredStamp(const Schema& schema) const
{
    // Infer distance field type and ensure it is numeric
    auto newDistanceField = this->getDistanceField().withInferredDataType(schema);
    if (!newDistanceField.getDataType().isNumeric())
    {
        throw CannotDeserialize("KnnAggregationLogicalFunction: distance field must be numeric.");
    }

    // Infer neighbour field type
    auto newNeighbourField = this->getNeighbourField().withInferredDataType(schema);

    auto newOnField = newDistanceField;

    // Qualify alias field
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
    result.distanceField = newDistanceField.getAs<FieldAccessLogicalFunction>().get();
    result.neighbourField = newNeighbourField.getAs<FieldAccessLogicalFunction>().get();
    result.onField = newOnField.getAs<FieldAccessLogicalFunction>().get();
    result.finalAggregateStamp = newFinalAggregateStamp;
    result.asField = this->getAsField().withFieldName(newAsFieldName).withDataType(newFinalAggregateStamp);
    result.inputStamp = newOnField.getDataType();
    return result;
}

Reflected KnnAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<KnnAggregationLogicalFunction>::operator()(const KnnAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedKnnAggregationLogicalFunction{
        .onField = function.getOnField(),
        .asField = function.getAsField(),
        .distanceField = function.getDistanceField(),
        .neighbourField = function.getNeighbourField(),
        .k = function.getK()});
}

KnnAggregationLogicalFunction Unreflector<KnnAggregationLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [onField, asField, distanceField, neighbourField, k] =
        unreflect<detail::ReflectedKnnAggregationLogicalFunction>(reflected);
    return KnnAggregationLogicalFunction{distanceField, neighbourField, asField, k};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterKnnAggAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(
            unreflect<KnnAggregationLogicalFunction>(arguments.reflected));
    }

    PRECONDITION(
        arguments.fields.size() >= 2,
        "KnnAggregationLogicalFunction requires at least distance and neighbour fields, but got {}",
        arguments.fields.size());
    constexpr std::size_t DEFAULT_K = 10;
    return std::make_shared<WindowAggregationLogicalFunction>(
        KnnAggregationLogicalFunction(arguments.fields[0], arguments.fields[1], arguments.fields[0], DEFAULT_K));
}

std::string KnnAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

DataType KnnAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType KnnAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType KnnAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction KnnAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction KnnAggregationLogicalFunction::getAsField() const
{
    return asField;
}

FieldAccessLogicalFunction KnnAggregationLogicalFunction::getDistanceField() const
{
    return distanceField;
}

FieldAccessLogicalFunction KnnAggregationLogicalFunction::getNeighbourField() const
{
    return neighbourField;
}

std::size_t KnnAggregationLogicalFunction::getK() const noexcept
{
    return k;
}

KnnAggregationLogicalFunction KnnAggregationLogicalFunction::withInputStamp(DataType newInputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(newInputStamp);
    return copy;
}

KnnAggregationLogicalFunction KnnAggregationLogicalFunction::withPartialAggregateStamp(DataType newPartialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(newPartialAggregateStamp);
    return copy;
}

KnnAggregationLogicalFunction KnnAggregationLogicalFunction::withFinalAggregateStamp(DataType newFinalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(newFinalAggregateStamp);
    return copy;
}

KnnAggregationLogicalFunction KnnAggregationLogicalFunction::withOnField(FieldAccessLogicalFunction newOnField) const
{
    auto copy = *this;
    copy.onField = std::move(newOnField);
    return copy;
}

KnnAggregationLogicalFunction KnnAggregationLogicalFunction::withAsField(FieldAccessLogicalFunction newAsField) const
{
    auto copy = *this;
    copy.asField = std::move(newAsField);
    return copy;
}

bool KnnAggregationLogicalFunction::operator==(const KnnAggregationLogicalFunction& other) const
{
    return this->onField == other.onField && this->asField == other.asField
        && this->distanceField == other.distanceField && this->neighbourField == other.neighbourField
        && this->k == other.k;
}

} // namespace NES
