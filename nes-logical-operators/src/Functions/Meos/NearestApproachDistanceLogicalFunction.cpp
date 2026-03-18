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

#include <Functions/Meos/NearestApproachDistanceLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

NearestApproachDistanceLogicalFunction::NearestApproachDistanceLogicalFunction(
    const LogicalFunction& lon1,
    const LogicalFunction& lat1,
    const LogicalFunction& ts1,
    const LogicalFunction& lon2,
    const LogicalFunction& lat2,
    const LogicalFunction& ts2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
    , lon1(lon1)
    , lat1(lat1)
    , ts1(ts1)
    , lon2(lon2)
    , lat2(lat2)
    , ts2(ts2)
{
}

bool NearestApproachDistanceLogicalFunction::operator==(const NearestApproachDistanceLogicalFunction& rhs) const
{
    return lon1 == rhs.lon1 && lat1 == rhs.lat1 && ts1 == rhs.ts1
        && lon2 == rhs.lon2 && lat2 == rhs.lat2 && ts2 == rhs.ts2;
}

DataType NearestApproachDistanceLogicalFunction::getDataType() const
{
    return dataType;
}

NearestApproachDistanceLogicalFunction NearestApproachDistanceLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> NearestApproachDistanceLogicalFunction::getChildren() const
{
    return {lon1, lat1, ts1, lon2, lat2, ts2};
}

NearestApproachDistanceLogicalFunction NearestApproachDistanceLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 6, "NearestApproachDistanceLogicalFunction requires 6 children, but got {}", children.size());
    auto copy = *this;
    copy.lon1 = children[0];
    copy.lat1 = children[1];
    copy.ts1 = children[2];
    copy.lon2 = children[3];
    copy.lat2 = children[4];
    copy.ts2 = children[5];
    return copy;
}

std::string_view NearestApproachDistanceLogicalFunction::getType() const
{
    return NAME;
}

std::string NearestApproachDistanceLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({}, {}, {}, {}, {}, {})", NAME,
                       lon1.explain(verbosity), lat1.explain(verbosity),
                       ts1.explain(verbosity), lon2.explain(verbosity),
                       lat2.explain(verbosity), ts2.explain(verbosity));
}

LogicalFunction NearestApproachDistanceLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto newChildren = getChildren();
    for (auto& child : newChildren)
    {
        child = child.withInferredDataType(schema);
    }

    INVARIANT(newChildren[0].getDataType().isNumeric(), "lon1 must be numeric, but was: {}", newChildren[0].getDataType());
    INVARIANT(newChildren[1].getDataType().isNumeric(), "lat1 must be numeric, but was: {}", newChildren[1].getDataType());
    INVARIANT(newChildren[2].getDataType().isType(DataType::Type::UINT64), "ts1 must be UINT64, but was: {}", newChildren[2].getDataType());
    INVARIANT(newChildren[3].getDataType().isNumeric(), "lon2 must be numeric, but was: {}", newChildren[3].getDataType());
    INVARIANT(newChildren[4].getDataType().isNumeric(), "lat2 must be numeric, but was: {}", newChildren[4].getDataType());
    INVARIANT(newChildren[5].getDataType().isType(DataType::Type::UINT64), "ts2 must be UINT64, but was: {}", newChildren[5].getDataType());

    return withChildren(newChildren);
}

Reflected Reflector<NearestApproachDistanceLogicalFunction>::operator()(const NearestApproachDistanceLogicalFunction& function) const
{
    return reflect(detail::ReflectedNearestApproachDistanceLogicalFunction{
        .lon1 = function.lon1,
        .lat1 = function.lat1,
        .ts1 = function.ts1,
        .lon2 = function.lon2,
        .lat2 = function.lat2,
        .ts2 = function.ts2});
}

NearestApproachDistanceLogicalFunction Unreflector<NearestApproachDistanceLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [lon1, lat1, ts1, lon2, lat2, ts2] = unreflect<detail::ReflectedNearestApproachDistanceLogicalFunction>(reflected);

    if (!lon1.has_value() || !lat1.has_value() || !ts1.has_value()
        || !lon2.has_value() || !lat2.has_value() || !ts2.has_value())
    {
        throw CannotDeserialize("NearestApproachDistanceLogicalFunction is missing a child");
    }
    return NearestApproachDistanceLogicalFunction{lon1.value(), lat1.value(), ts1.value(),
                                                   lon2.value(), lat2.value(), ts2.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterNearestApproachDistanceLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<NearestApproachDistanceLogicalFunction>(arguments.reflected);
    }

    PRECONDITION(arguments.children.size() == 6,
                 "NearestApproachDistanceLogicalFunction requires 6 children, but got {}",
                 arguments.children.size());
    return NearestApproachDistanceLogicalFunction(arguments.children[0], arguments.children[1],
                                                   arguments.children[2], arguments.children[3],
                                                   arguments.children[4], arguments.children[5]);
}

} // namespace NES
