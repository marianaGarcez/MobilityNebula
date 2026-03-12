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

#include <Functions/Meos/TemporalAtStBoxLogicalFunction.hpp>

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

TemporalAtStBoxLogicalFunction::TemporalAtStBoxLogicalFunction(
    const LogicalFunction& lon, const LogicalFunction& lat,
    const LogicalFunction& timestamp, const LogicalFunction& stbox)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
    , parameters{lon, lat, timestamp, stbox}
    , hasBorderParam(false)
{
}

TemporalAtStBoxLogicalFunction::TemporalAtStBoxLogicalFunction(
    const LogicalFunction& lon, const LogicalFunction& lat,
    const LogicalFunction& timestamp, const LogicalFunction& stbox,
    const LogicalFunction& borderInclusive)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
    , parameters{lon, lat, timestamp, stbox, borderInclusive}
    , hasBorderParam(true)
{
}

bool TemporalAtStBoxLogicalFunction::operator==(const TemporalAtStBoxLogicalFunction& rhs) const
{
    return parameters == rhs.parameters && hasBorderParam == rhs.hasBorderParam;
}

DataType TemporalAtStBoxLogicalFunction::getDataType() const
{
    return dataType;
}

TemporalAtStBoxLogicalFunction TemporalAtStBoxLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> TemporalAtStBoxLogicalFunction::getChildren() const
{
    return parameters;
}

TemporalAtStBoxLogicalFunction TemporalAtStBoxLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 4 || children.size() == 5,
                 "TemporalAtStBoxLogicalFunction requires 4 or 5 children, but got {}",
                 children.size());
    auto copy = *this;
    copy.parameters = children;
    copy.hasBorderParam = (children.size() == 5);
    return copy;
}

std::string_view TemporalAtStBoxLogicalFunction::getType() const
{
    return NAME;
}

std::string TemporalAtStBoxLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string args;
    for (size_t index = 0; index < parameters.size(); ++index)
    {
        if (index > 0)
        {
            args += ", ";
        }
        args += parameters[index].explain(verbosity);
    }
    return fmt::format("{}({})", NAME, args);
}

LogicalFunction TemporalAtStBoxLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(parameters.size());
    for (const auto& child : parameters)
    {
        newChildren.emplace_back(child.withInferredDataType(schema));
    }

    INVARIANT(newChildren[0].getDataType().isNumeric(), "Longitude must be numeric, but was: {}", newChildren[0].getDataType());
    INVARIANT(newChildren[1].getDataType().isNumeric(), "Latitude must be numeric, but was: {}", newChildren[1].getDataType());
    INVARIANT(newChildren[2].getDataType().isType(DataType::Type::UINT64), "Timestamp must be UINT64, but was: {}", newChildren[2].getDataType());
    INVARIANT(newChildren[3].getDataType().isType(DataType::Type::VARSIZED), "STBOX literal must be VARSIZED, but was: {}", newChildren[3].getDataType());
    if (newChildren.size() == 5)
    {
        INVARIANT(newChildren[4].getDataType().isType(DataType::Type::BOOLEAN),
                  "Border flag must be BOOL, but was: {}",
                  newChildren[4].getDataType());
    }

    return withChildren(newChildren);
}

Reflected Reflector<TemporalAtStBoxLogicalFunction>::operator()(const TemporalAtStBoxLogicalFunction& function) const
{
    detail::ReflectedTemporalAtStBoxLogicalFunction reflected;
    reflected.lon = function.parameters[0];
    reflected.lat = function.parameters[1];
    reflected.timestamp = function.parameters[2];
    reflected.stbox = function.parameters[3];
    if (function.hasBorderParam && function.parameters.size() == 5)
    {
        reflected.borderInclusive = function.parameters[4];
    }
    return reflect(reflected);
}

TemporalAtStBoxLogicalFunction Unreflector<TemporalAtStBoxLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto r = unreflect<detail::ReflectedTemporalAtStBoxLogicalFunction>(reflected);

    if (!r.lon.has_value() || !r.lat.has_value() || !r.timestamp.has_value() || !r.stbox.has_value())
    {
        throw CannotDeserialize("TemporalAtStBoxLogicalFunction is missing required children");
    }

    if (r.borderInclusive.has_value())
    {
        return TemporalAtStBoxLogicalFunction{r.lon.value(), r.lat.value(), r.timestamp.value(),
                                              r.stbox.value(), r.borderInclusive.value()};
    }
    return TemporalAtStBoxLogicalFunction{r.lon.value(), r.lat.value(), r.timestamp.value(), r.stbox.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterTemporalAtStBoxLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<TemporalAtStBoxLogicalFunction>(arguments.reflected);
    }

    if (arguments.children.size() == 4)
    {
        return TemporalAtStBoxLogicalFunction(arguments.children[0], arguments.children[1],
                                              arguments.children[2], arguments.children[3]);
    }
    if (arguments.children.size() == 5)
    {
        return TemporalAtStBoxLogicalFunction(arguments.children[0], arguments.children[1],
                                              arguments.children[2], arguments.children[3],
                                              arguments.children[4]);
    }
    PRECONDITION(false,
                 "TemporalAtStBoxLogicalFunction requires 4 or 5 children, but got {}",
                 arguments.children.size());
}

} // namespace NES
