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

#include <Functions/Meos/TemporalEContainsGeometryLogicalFunction.hpp>

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

/* constructors */

TemporalEContainsGeometryLogicalFunction::TemporalEContainsGeometryLogicalFunction(
    const LogicalFunction& param1, const LogicalFunction& param2,
    const LogicalFunction& param3, const LogicalFunction& param4)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , parameters{param1, param2, param3, param4}
{
}

TemporalEContainsGeometryLogicalFunction::TemporalEContainsGeometryLogicalFunction(
    const LogicalFunction& lon1, const LogicalFunction& lat1,
    const LogicalFunction& ts1, const LogicalFunction& lon2,
    const LogicalFunction& lat2, const LogicalFunction& ts2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , parameters{lon1, lat1, ts1, lon2, lat2, ts2}
{
}

/* concept API */

bool TemporalEContainsGeometryLogicalFunction::operator==(const TemporalEContainsGeometryLogicalFunction& rhs) const
{
    return parameters == rhs.parameters;
}

DataType TemporalEContainsGeometryLogicalFunction::getDataType() const
{
    return dataType;
}

TemporalEContainsGeometryLogicalFunction TemporalEContainsGeometryLogicalFunction::withDataType(const DataType& dt) const
{
    auto copy = *this;
    copy.dataType = dt;
    return copy;
}

std::vector<LogicalFunction> TemporalEContainsGeometryLogicalFunction::getChildren() const
{
    return parameters;
}

TemporalEContainsGeometryLogicalFunction TemporalEContainsGeometryLogicalFunction::withChildren(const std::vector<LogicalFunction>& ch) const
{
    PRECONDITION(ch.size() == 4 || ch.size() == 6,
                 "TemporalEContainsGeometry expects 4 or 6 params, got {}", ch.size());
    auto copy = *this;
    copy.parameters = ch;
    return copy;
}

std::string_view TemporalEContainsGeometryLogicalFunction::getType() const
{
    return NAME;
}

std::string TemporalEContainsGeometryLogicalFunction::explain(ExplainVerbosity v) const
{
    std::string a;
    for (size_t i = 0; i < parameters.size(); ++i)
    {
        if (i) a += ", ";
        a += parameters[i].explain(v);
    }
    return fmt::format("TEMPORAL_ECONTAINS_GEOMETRY({})", a);
}

LogicalFunction TemporalEContainsGeometryLogicalFunction::withInferredDataType(const Schema& s) const
{
    std::vector<LogicalFunction> ch;
    ch.reserve(parameters.size());
    for (auto& p : parameters)
    {
        ch.push_back(p.withInferredDataType(s));
    }

    // light-weight checks
    auto isNum  = [](const DataType& dt) { return dt.isNumeric(); };
    auto isTime = [](const DataType& dt) { return dt.isType(DataType::Type::UINT64); };
    auto isStr  = [](const DataType& dt) { return dt.isType(DataType::Type::VARSIZED); };

    // Validate based on parameter count and types
    if (ch.size() == 6)
    {
        // 6-param: temporal-temporal (lon1, lat1, ts1, lon2, lat2, ts2)
        INVARIANT(isNum(ch[0].getDataType()) && isNum(ch[1].getDataType()) && isTime(ch[2].getDataType())
               && isNum(ch[3].getDataType()) && isNum(ch[4].getDataType()) && isTime(ch[5].getDataType()),
               "Invalid types for temporal-temporal contains");
    }
    else if (ch.size() == 4)
    {
        if (isStr(ch[0].getDataType()))
        {
            // 4-param: static-temporal (static_geom, lon, lat, ts)
            INVARIANT(isStr(ch[0].getDataType()) && isNum(ch[1].getDataType())
                   && isNum(ch[2].getDataType()) && isTime(ch[3].getDataType()),
                   "Invalid types for static-temporal contains");
        }
        else
        {
            // 4-param: temporal-static (lon, lat, ts, static_geom)
            INVARIANT(isNum(ch[0].getDataType()) && isNum(ch[1].getDataType())
                   && isTime(ch[2].getDataType()) && isStr(ch[3].getDataType()),
                   "Invalid types for temporal-static contains");
        }
    }
    else
    {
        PRECONDITION(false, "TemporalEContainsGeometry expects 4 or 6 parameters, got {}", ch.size());
    }
    return withChildren(ch);
}

/* reflection */

Reflected Reflector<TemporalEContainsGeometryLogicalFunction>::operator()(const TemporalEContainsGeometryLogicalFunction& function) const
{
    detail::ReflectedTemporalEContainsGeometryLogicalFunction reflected;
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

TemporalEContainsGeometryLogicalFunction Unreflector<TemporalEContainsGeometryLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto r = unreflect<detail::ReflectedTemporalEContainsGeometryLogicalFunction>(reflected);

    if (!r.param0.has_value() || !r.param1.has_value() || !r.param2.has_value() || !r.param3.has_value())
    {
        throw CannotDeserialize("TemporalEContainsGeometryLogicalFunction is missing required children");
    }

    if (r.param4.has_value() && r.param5.has_value())
    {
        return TemporalEContainsGeometryLogicalFunction{r.param0.value(), r.param1.value(), r.param2.value(),
                                                        r.param3.value(), r.param4.value(), r.param5.value()};
    }
    return TemporalEContainsGeometryLogicalFunction{r.param0.value(), r.param1.value(), r.param2.value(), r.param3.value()};
}

/* registry helper */

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterTemporalEContainsGeometryLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<TemporalEContainsGeometryLogicalFunction>(arguments.reflected);
    }

    if (arguments.children.size() == 6)
    {
        return TemporalEContainsGeometryLogicalFunction(arguments.children[0], arguments.children[1], arguments.children[2],
                                                        arguments.children[3], arguments.children[4], arguments.children[5]);
    }
    PRECONDITION(arguments.children.size() == 4,
                 "TemporalEContainsGeometry expects 4 or 6 params, got {}", arguments.children.size());

    return TemporalEContainsGeometryLogicalFunction(arguments.children[0], arguments.children[1],
                                                    arguments.children[2], arguments.children[3]);
}

} // namespace NES
