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

#include <AggregationLogicalFunctionRegistry.hpp>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <ErrorHandling.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>

namespace NES
{

namespace
{
inline constexpr auto PARTIAL_AGG_TYPE = DataType::Type::UNDEFINED;
inline constexpr auto FINAL_AGG_TYPE = DataType::Type::VARSIZED;

inline constexpr std::string_view NEIGHBOUR_KEY = "KnnAgg.neighbour_field";
inline constexpr std::string_view K_KEY = "KnnAgg.k";
} // namespace

std::shared_ptr<WindowAggregationLogicalFunction>
KnnAggregationLogicalFunction::create(const FieldAccessLogicalFunction& distanceField,
                                      const FieldAccessLogicalFunction& neighbourField,
                                      std::size_t k)
{
    // Initial alias uses the distance field; it will be updated during parsing
    return std::make_shared<KnnAggregationLogicalFunction>(distanceField, neighbourField, distanceField, k);
}

KnnAggregationLogicalFunction::KnnAggregationLogicalFunction(FieldAccessLogicalFunction distanceField,
                                                             FieldAccessLogicalFunction neighbourField,
                                                             FieldAccessLogicalFunction asField,
                                                             std::size_t k)
    : WindowAggregationLogicalFunction(
          DataTypeProvider::provideDataType(distanceField.getDataType().type),
          DataTypeProvider::provideDataType(PARTIAL_AGG_TYPE),
          DataTypeProvider::provideDataType(FINAL_AGG_TYPE),
          std::move(distanceField),
          std::move(asField))
    , distanceField(onField)
    , neighbourField(std::move(neighbourField))
    , k(k)
{
}

void KnnAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    // Infer distance field type and ensure it is numeric
    auto newDistanceField = distanceField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    INVARIANT(
        newDistanceField.getDataType().isNumeric(),
        "KnnAggregationLogicalFunction: distance field must be numeric, but was {}",
        newDistanceField.getDataType());

    // Infer neighbour field type; we currently restrict to UINT64 neighbours
    auto newNeighbourField = neighbourField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    INVARIANT(
        newNeighbourField.getDataType().isType(DataType::Type::UINT64),
        "KnnAggregationLogicalFunction: neighbour field must be UINT64, but was {}",
        newNeighbourField.getDataType());

    // Qualify alias field similar to other aggregations
    const auto onFieldName = newDistanceField.getFieldName();
    const auto asFieldName = asField.getFieldName();
    const auto attributeNameResolver =
        onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);

    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField =
            asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        const auto fieldName =
            asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField =
            asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }

    // Set final type for alias and input stamp for planner
    asField = asField.withDataType(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>();
    inputStamp = newDistanceField.getDataType();

    distanceField = newDistanceField;
    neighbourField = newNeighbourField;
}

SerializableAggregationFunction KnnAggregationLogicalFunction::serialize() const
{
    SerializableAggregationFunction saf;
    saf.set_type(std::string(NAME));

    // on_field: distance field
    SerializableFunction distanceProto;
    distanceProto.CopyFrom(LogicalFunction(distanceField).serialize());

    // neighbour field stored as FunctionList in on_field.config
    FunctionList neighbourList;
    *neighbourList.add_functions() = LogicalFunction(neighbourField).serialize();
    auto& cfg = *distanceProto.mutable_config();
    cfg[std::string(NEIGHBOUR_KEY)] = descriptorConfigTypeToProto(neighbourList);

    // k parameter stored as uint64 in config
    cfg[std::string(K_KEY)] = descriptorConfigTypeToProto(static_cast<std::uint64_t>(k));

    saf.mutable_on_field()->CopyFrom(distanceProto);

    // as_field: alias
    SerializableFunction asProto;
    asProto.CopyFrom(LogicalFunction(asField).serialize());
    saf.mutable_as_field()->CopyFrom(asProto);

    return saf;
}

std::string_view KnnAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

const FieldAccessLogicalFunction& KnnAggregationLogicalFunction::getDistanceField() const noexcept
{
    return distanceField;
}

const FieldAccessLogicalFunction& KnnAggregationLogicalFunction::getNeighbourField() const noexcept
{
    return neighbourField;
}

std::size_t KnnAggregationLogicalFunction::getK() const noexcept
{
    return k;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterKnnAggAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    // This path is only used when reconstructing from a minimal registry entry (e.g., tests).
    PRECONDITION(
        arguments.fields.size() >= 2,
        "KnnAggregationLogicalFunction requires at least distance and neighbour fields, but got {}",
        arguments.fields.size());
    constexpr std::size_t DEFAULT_K = 10;
    return KnnAggregationLogicalFunction::create(arguments.fields[0], arguments.fields[1], DEFAULT_K);
}

} // namespace NES

