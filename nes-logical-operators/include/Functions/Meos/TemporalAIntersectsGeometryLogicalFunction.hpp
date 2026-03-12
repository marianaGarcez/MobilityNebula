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

class TemporalAIntersectsGeometryLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "TemporalAIntersectsGeometry";

    /// Constructor with 4 parameters for temporal-static intersection: lon1, lat1, timestamp1, static_geometry_wkt
    TemporalAIntersectsGeometryLogicalFunction(const LogicalFunction& lon1, const LogicalFunction& lat1,
                                               const LogicalFunction& timestamp1, const LogicalFunction& staticGeometry);

    /// Constructor with 6 parameters for temporal-temporal intersection: lon1, lat1, timestamp1, lon2, lat2, timestamp2
    TemporalAIntersectsGeometryLogicalFunction(const LogicalFunction& lon1, const LogicalFunction& lat1,
                                               const LogicalFunction& timestamp1, const LogicalFunction& lon2,
                                               const LogicalFunction& lat2, const LogicalFunction& timestamp2);

    [[nodiscard]] bool operator==(const TemporalAIntersectsGeometryLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] TemporalAIntersectsGeometryLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] TemporalAIntersectsGeometryLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    std::vector<LogicalFunction> parameters;
    bool isTemporal6Param;

    friend Reflector<TemporalAIntersectsGeometryLogicalFunction>;
};

static_assert(LogicalFunctionConcept<TemporalAIntersectsGeometryLogicalFunction>);

template <>
struct Reflector<TemporalAIntersectsGeometryLogicalFunction>
{
    Reflected operator()(const TemporalAIntersectsGeometryLogicalFunction& function) const;
};

template <>
struct Unreflector<TemporalAIntersectsGeometryLogicalFunction>
{
    TemporalAIntersectsGeometryLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedTemporalAIntersectsGeometryLogicalFunction
{
    std::optional<LogicalFunction> param0;
    std::optional<LogicalFunction> param1;
    std::optional<LogicalFunction> param2;
    std::optional<LogicalFunction> param3;
    std::optional<LogicalFunction> param4;
    std::optional<LogicalFunction> param5;
};
}

FMT_OSTREAM(NES::TemporalAIntersectsGeometryLogicalFunction);
