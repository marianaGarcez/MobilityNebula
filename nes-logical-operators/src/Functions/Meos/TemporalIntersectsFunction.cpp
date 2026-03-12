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

#include <Functions/Meos/TemporalIntersectsFunction.hpp>

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

TemporalIntersectsFunction::TemporalIntersectsFunction(const LogicalFunction& lon, const LogicalFunction& lat, const LogicalFunction& ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , lon(lon)
    , lat(lat)
    , ts(ts)
{
}

bool TemporalIntersectsFunction::operator==(const TemporalIntersectsFunction& rhs) const
{
    return lon == rhs.lon && lat == rhs.lat && ts == rhs.ts;
}

std::string TemporalIntersectsFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("TEMPORAL_INTERSECTS({}, {}, {})",
                      lon.explain(verbosity),
                      lat.explain(verbosity),
                      ts.explain(verbosity));
}

DataType TemporalIntersectsFunction::getDataType() const
{
    return dataType;
}

TemporalIntersectsFunction TemporalIntersectsFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction TemporalIntersectsFunction::withInferredDataType(const Schema& schema) const
{
    auto newChildren = getChildren();
    for (auto& c : newChildren)
    {
        c = c.withInferredDataType(schema);
    }
    return withChildren(newChildren).withDataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN));
}

std::vector<LogicalFunction> TemporalIntersectsFunction::getChildren() const
{
    return {lon, lat, ts};
}

TemporalIntersectsFunction TemporalIntersectsFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 3, "TemporalIntersectsFunction requires exactly 3 children, but got {}", children.size());
    auto copy = *this;
    copy.lon = children[0];
    copy.lat = children[1];
    copy.ts = children[2];
    return copy;
}

std::string_view TemporalIntersectsFunction::getType() const
{
    return NAME;
}

Reflected Reflector<TemporalIntersectsFunction>::operator()(const TemporalIntersectsFunction& function) const
{
    return reflect(detail::ReflectedTemporalIntersectsFunction{.lon = function.lon, .lat = function.lat, .ts = function.ts});
}

TemporalIntersectsFunction Unreflector<TemporalIntersectsFunction>::operator()(const Reflected& reflected) const
{
    auto [lon, lat, ts] = unreflect<detail::ReflectedTemporalIntersectsFunction>(reflected);

    if (!lon.has_value() || !lat.has_value() || !ts.has_value())
    {
        throw CannotDeserialize("TemporalIntersectsFunction is missing a child");
    }
    return TemporalIntersectsFunction{lon.value(), lat.value(), ts.value()};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterTemporalIntersectsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<TemporalIntersectsFunction>(arguments.reflected);
    }

    PRECONDITION(arguments.children.size() == 3, "TemporalIntersectsFunction requires exactly three children, but got {}", arguments.children.size());
    return TemporalIntersectsFunction(arguments.children[0], arguments.children[1], arguments.children[2]);
}

}
