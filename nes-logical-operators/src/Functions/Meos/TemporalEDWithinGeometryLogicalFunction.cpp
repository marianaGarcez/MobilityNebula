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

#include <Functions/Meos/TemporalEDWithinGeometryLogicalFunction.hpp>

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

TemporalEDWithinGeometryLogicalFunction::TemporalEDWithinGeometryLogicalFunction(
    const LogicalFunction& lon, const LogicalFunction& lat,
    const LogicalFunction& timestamp, const LogicalFunction& geometry,
    const LogicalFunction& distance)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , lon(lon)
    , lat(lat)
    , timestamp(timestamp)
    , geometry(geometry)
    , distance(distance)
{
}

bool TemporalEDWithinGeometryLogicalFunction::operator==(const TemporalEDWithinGeometryLogicalFunction& rhs) const
{
    return lon == rhs.lon && lat == rhs.lat && timestamp == rhs.timestamp
        && geometry == rhs.geometry && distance == rhs.distance;
}

DataType TemporalEDWithinGeometryLogicalFunction::getDataType() const
{
    return dataType;
}

TemporalEDWithinGeometryLogicalFunction TemporalEDWithinGeometryLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> TemporalEDWithinGeometryLogicalFunction::getChildren() const
{
    return {lon, lat, timestamp, geometry, distance};
}

TemporalEDWithinGeometryLogicalFunction TemporalEDWithinGeometryLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 5, "TemporalEDWithinGeometryLogicalFunction requires 5 children, but got {}", children.size());
    auto copy = *this;
    copy.lon = children[0];
    copy.lat = children[1];
    copy.timestamp = children[2];
    copy.geometry = children[3];
    copy.distance = children[4];
    return copy;
}

std::string_view TemporalEDWithinGeometryLogicalFunction::getType() const
{
    return NAME;
}

std::string TemporalEDWithinGeometryLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({}, {}, {}, {}, {})", NAME,
                       lon.explain(verbosity), lat.explain(verbosity),
                       timestamp.explain(verbosity), geometry.explain(verbosity),
                       distance.explain(verbosity));
}

LogicalFunction TemporalEDWithinGeometryLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto newChildren = getChildren();
    for (auto& child : newChildren)
    {
        child = child.withInferredDataType(schema);
    }

    INVARIANT(newChildren[0].getDataType().isNumeric(), "Longitude must be numeric, but was: {}", newChildren[0].getDataType());
    INVARIANT(newChildren[1].getDataType().isNumeric(), "Latitude must be numeric, but was: {}", newChildren[1].getDataType());
    INVARIANT(newChildren[2].getDataType().isType(DataType::Type::UINT64), "Timestamp must be UINT64, but was: {}", newChildren[2].getDataType());
    INVARIANT(newChildren[3].getDataType().isType(DataType::Type::VARSIZED), "Geometry literal must be VARSIZED, but was: {}", newChildren[3].getDataType());
    INVARIANT(newChildren[4].getDataType().isNumeric(), "Distance must be numeric, but was: {}", newChildren[4].getDataType());

    return withChildren(newChildren);
}

Reflected Reflector<TemporalEDWithinGeometryLogicalFunction>::operator()(const TemporalEDWithinGeometryLogicalFunction& function) const
{
    return reflect(detail::ReflectedTemporalEDWithinGeometryLogicalFunction{
        .lon = function.lon,
        .lat = function.lat,
        .timestamp = function.timestamp,
        .geometry = function.geometry,
        .distance = function.distance});
}

TemporalEDWithinGeometryLogicalFunction Unreflector<TemporalEDWithinGeometryLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [lon, lat, timestamp, geometry, distance] = unreflect<detail::ReflectedTemporalEDWithinGeometryLogicalFunction>(reflected);

    if (!lon.has_value() || !lat.has_value() || !timestamp.has_value() || !geometry.has_value() || !distance.has_value())
    {
        throw CannotDeserialize("TemporalEDWithinGeometryLogicalFunction is missing a child");
    }
    return TemporalEDWithinGeometryLogicalFunction{lon.value(), lat.value(), timestamp.value(), geometry.value(), distance.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterTemporalEDWithinGeometryLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<TemporalEDWithinGeometryLogicalFunction>(arguments.reflected);
    }

    PRECONDITION(arguments.children.size() == 5,
                 "TemporalEDWithinGeometryLogicalFunction requires 5 children, but got {}",
                 arguments.children.size());
    return TemporalEDWithinGeometryLogicalFunction(arguments.children[0], arguments.children[1],
                                                   arguments.children[2], arguments.children[3],
                                                   arguments.children[4]);
}

} // namespace NES
