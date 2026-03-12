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

#include <Functions/Meos/IntersectLogicalFunction.hpp>

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

IntersectLogicalFunction::IntersectLogicalFunction(const LogicalFunction& lon, const LogicalFunction& lat, const LogicalFunction& ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , lon(lon)
    , lat(lat)
    , ts(ts)
{
}

bool IntersectLogicalFunction::operator==(const IntersectLogicalFunction& rhs) const
{
    const bool simpleMatch = lon == rhs.lon and lat == rhs.lat and ts == rhs.ts;
    const bool commutativeMatch = lon == rhs.lat and lat == rhs.lon and ts == rhs.ts;
    return simpleMatch or commutativeMatch;
}

std::string IntersectLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("INTERSECT({}, {}, {})", lon.explain(verbosity), lat.explain(verbosity), ts.explain(verbosity));
}

DataType IntersectLogicalFunction::getDataType() const
{
    return dataType;
};

IntersectLogicalFunction IntersectLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction IntersectLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& node : getChildren())
    {
        newChildren.push_back(node.withInferredDataType(schema));
    }
    /// check if children dataType is correct - spatial coordinates should be numeric
    INVARIANT(
        newChildren[0].getDataType().isType(DataType::Type::FLOAT64), "the dataType of longitude child must be FLOAT64, but was: {}", newChildren[0].getDataType());
    INVARIANT(
        newChildren[1].getDataType().isType(DataType::Type::FLOAT64),
        "the dataType of latitude child must be FLOAT64, but was: {}",
        newChildren[1].getDataType());
    INVARIANT(
        newChildren[2].getDataType().isType(DataType::Type::UINT64),
        "the dataType of timestamp child must be UINT64, but was: {}",
        newChildren[2].getDataType());
    return withChildren(newChildren);
}

std::vector<LogicalFunction> IntersectLogicalFunction::getChildren() const
{
    return {lon, lat, ts};
};

IntersectLogicalFunction IntersectLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 3, "IntersectLogicalFunction requires exactly three children, but got {}", children.size());
    auto copy = *this;
    copy.lon = children[0];
    copy.lat = children[1];
    copy.ts = children[2];
    return copy;
};

std::string_view IntersectLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<IntersectLogicalFunction>::operator()(const IntersectLogicalFunction& function) const
{
    return reflect(detail::ReflectedIntersectLogicalFunction{.lon = function.lon, .lat = function.lat, .ts = function.ts});
}

IntersectLogicalFunction Unreflector<IntersectLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [lon, lat, ts] = unreflect<detail::ReflectedIntersectLogicalFunction>(reflected);

    if (!lon.has_value() || !lat.has_value() || !ts.has_value())
    {
        throw CannotDeserialize("IntersectLogicalFunction is missing a child");
    }
    return IntersectLogicalFunction{lon.value(), lat.value(), ts.value()};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterIntersectLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<IntersectLogicalFunction>(arguments.reflected);
    }

    PRECONDITION(arguments.children.size() == 3, "IntersectLogicalFunction requires exactly three children, but got {}", arguments.children.size());
    return IntersectLogicalFunction(arguments.children[0], arguments.children[1], arguments.children[2]);
}

}
