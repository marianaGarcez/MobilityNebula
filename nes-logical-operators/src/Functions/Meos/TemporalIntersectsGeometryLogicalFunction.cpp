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

#include <Functions/Meos/TemporalIntersectsGeometryLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
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

// 4-parameter constructor for temporal-static intersection
TemporalIntersectsGeometryLogicalFunction::TemporalIntersectsGeometryLogicalFunction(
    const LogicalFunction& lon1, const LogicalFunction& lat1,
    const LogicalFunction& timestamp1, const LogicalFunction& staticGeometry)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , parameters{lon1, lat1, timestamp1, staticGeometry}
    , isTemporal6Param(false)
{
}

// 6-parameter constructor for temporal-temporal intersection
TemporalIntersectsGeometryLogicalFunction::TemporalIntersectsGeometryLogicalFunction(
    const LogicalFunction& lon1, const LogicalFunction& lat1,
    const LogicalFunction& timestamp1, const LogicalFunction& lon2,
    const LogicalFunction& lat2, const LogicalFunction& timestamp2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , parameters{lon1, lat1, timestamp1, lon2, lat2, timestamp2}
    , isTemporal6Param(true)
{
}

bool TemporalIntersectsGeometryLogicalFunction::operator==(const TemporalIntersectsGeometryLogicalFunction& rhs) const
{
    return parameters == rhs.parameters && isTemporal6Param == rhs.isTemporal6Param;
}

DataType TemporalIntersectsGeometryLogicalFunction::getDataType() const
{
    return dataType;
}

TemporalIntersectsGeometryLogicalFunction TemporalIntersectsGeometryLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

std::vector<LogicalFunction> TemporalIntersectsGeometryLogicalFunction::getChildren() const
{
    return parameters;
}

TemporalIntersectsGeometryLogicalFunction TemporalIntersectsGeometryLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 4 || children.size() == 6,
                 "TemporalIntersectsGeometryLogicalFunction requires 4 or 6 children, but got {}", children.size());
    auto copy = *this;
    copy.parameters = children;
    copy.isTemporal6Param = (children.size() == 6);
    return copy;
}

std::string_view TemporalIntersectsGeometryLogicalFunction::getType() const
{
    return NAME;
}

std::string TemporalIntersectsGeometryLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string args;
    for (size_t i = 0; i < parameters.size(); ++i)
    {
        if (i > 0) args += ", ";
        args += parameters[i].explain(verbosity);
    }
    return fmt::format("TEMPORAL_INTERSECTS_GEOMETRY({})", args);
}

LogicalFunction TemporalIntersectsGeometryLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& node : getChildren())
    {
        newChildren.push_back(node.withInferredDataType(schema));
    }

    if (isTemporal6Param)
    {
        // 6-parameter case: lon1, lat1, timestamp1, lon2, lat2, timestamp2
        INVARIANT(newChildren[0].getDataType().isNumeric(), "lon1 must be numeric, but was: {}", newChildren[0].getDataType());
        INVARIANT(newChildren[1].getDataType().isNumeric(), "lat1 must be numeric, but was: {}", newChildren[1].getDataType());
        INVARIANT(newChildren[2].getDataType().isType(DataType::Type::UINT64), "timestamp1 must be UINT64, but was: {}", newChildren[2].getDataType());
        INVARIANT(newChildren[3].getDataType().isNumeric(), "lon2 must be numeric, but was: {}", newChildren[3].getDataType());
        INVARIANT(newChildren[4].getDataType().isNumeric(), "lat2 must be numeric, but was: {}", newChildren[4].getDataType());
        INVARIANT(newChildren[5].getDataType().isType(DataType::Type::UINT64), "timestamp2 must be UINT64, but was: {}", newChildren[5].getDataType());
    }
    else
    {
        // 4-parameter case: lon1, lat1, timestamp1, static_geometry
        INVARIANT(newChildren[0].getDataType().isNumeric(), "lon1 must be numeric, but was: {}", newChildren[0].getDataType());
        INVARIANT(newChildren[1].getDataType().isNumeric(), "lat1 must be numeric, but was: {}", newChildren[1].getDataType());
        INVARIANT(newChildren[2].getDataType().isType(DataType::Type::UINT64), "timestamp1 must be UINT64, but was: {}", newChildren[2].getDataType());
        INVARIANT(newChildren[3].getDataType().isType(DataType::Type::VARSIZED), "static_geometry must be VARSIZED, but was: {}", newChildren[3].getDataType());
    }

    return withChildren(newChildren);
}

Reflected Reflector<TemporalIntersectsGeometryLogicalFunction>::operator()(const TemporalIntersectsGeometryLogicalFunction& function) const
{
    detail::ReflectedTemporalIntersectsGeometryLogicalFunction reflected;
    for (size_t i = 0; i < function.parameters.size(); ++i)
    {
        switch (i)
        {
            case 0: reflected.param0 = function.parameters[0]; break;
            case 1: reflected.param1 = function.parameters[1]; break;
            case 2: reflected.param2 = function.parameters[2]; break;
            case 3: reflected.param3 = function.parameters[3]; break;
            case 4: reflected.param4 = function.parameters[4]; break;
            case 5: reflected.param5 = function.parameters[5]; break;
            default: break;
        }
    }
    return reflect(reflected);
}

TemporalIntersectsGeometryLogicalFunction Unreflector<TemporalIntersectsGeometryLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto r = unreflect<detail::ReflectedTemporalIntersectsGeometryLogicalFunction>(reflected);

    if (!r.param0.has_value() || !r.param1.has_value() || !r.param2.has_value() || !r.param3.has_value())
    {
        throw CannotDeserialize("TemporalIntersectsGeometryLogicalFunction is missing required children");
    }

    if (r.param4.has_value() && r.param5.has_value())
    {
        return TemporalIntersectsGeometryLogicalFunction{r.param0.value(), r.param1.value(), r.param2.value(),
                                                         r.param3.value(), r.param4.value(), r.param5.value()};
    }
    return TemporalIntersectsGeometryLogicalFunction{r.param0.value(), r.param1.value(), r.param2.value(), r.param3.value()};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterTemporalIntersectsGeometryLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<TemporalIntersectsGeometryLogicalFunction>(arguments.reflected);
    }

    if (arguments.children.size() == 4)
    {
        return TemporalIntersectsGeometryLogicalFunction(arguments.children[0], arguments.children[1],
                                                         arguments.children[2], arguments.children[3]);
    }
    else if (arguments.children.size() == 6)
    {
        return TemporalIntersectsGeometryLogicalFunction(arguments.children[0], arguments.children[1],
                                                         arguments.children[2], arguments.children[3],
                                                         arguments.children[4], arguments.children[5]);
    }
    else
    {
        PRECONDITION(false, "TemporalIntersectsGeometryLogicalFunction requires 4 or 6 children, but got {}", arguments.children.size());
    }
}

}
