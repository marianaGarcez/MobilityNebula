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

#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class TemporalEContainsGeometryLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "TemporalEContainsGeometry";

    /// temporal-static (lon, lat, ts, static_geom) or static-temporal (static_geom, lon, lat, ts)
    TemporalEContainsGeometryLogicalFunction(const LogicalFunction& param1, const LogicalFunction& param2,
                                             const LogicalFunction& param3, const LogicalFunction& param4);

    /// temporal-temporal (lon1, lat1, ts1, lon2, lat2, ts2)
    TemporalEContainsGeometryLogicalFunction(const LogicalFunction& lon1, const LogicalFunction& lat1,
                                             const LogicalFunction& ts1, const LogicalFunction& lon2,
                                             const LogicalFunction& lat2, const LogicalFunction& ts2);

    [[nodiscard]] bool operator==(const TemporalEContainsGeometryLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] TemporalEContainsGeometryLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] TemporalEContainsGeometryLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    std::vector<LogicalFunction> parameters;

    friend Reflector<TemporalEContainsGeometryLogicalFunction>;
};

static_assert(LogicalFunctionConcept<TemporalEContainsGeometryLogicalFunction>);

template <>
struct Reflector<TemporalEContainsGeometryLogicalFunction>
{
    Reflected operator()(const TemporalEContainsGeometryLogicalFunction& function) const;
};

template <>
struct Unreflector<TemporalEContainsGeometryLogicalFunction>
{
    TemporalEContainsGeometryLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedTemporalEContainsGeometryLogicalFunction
{
    std::optional<LogicalFunction> param0;
    std::optional<LogicalFunction> param1;
    std::optional<LogicalFunction> param2;
    std::optional<LogicalFunction> param3;
    std::optional<LogicalFunction> param4;
    std::optional<LogicalFunction> param5;
};
}

FMT_OSTREAM(NES::TemporalEContainsGeometryLogicalFunction);
