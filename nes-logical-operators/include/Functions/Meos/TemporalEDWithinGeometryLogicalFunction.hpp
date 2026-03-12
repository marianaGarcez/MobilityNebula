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

class TemporalEDWithinGeometryLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "TemporalEDWithinGeometry";

    TemporalEDWithinGeometryLogicalFunction(const LogicalFunction& lon, const LogicalFunction& lat,
                                            const LogicalFunction& timestamp, const LogicalFunction& geometry,
                                            const LogicalFunction& distance);

    [[nodiscard]] bool operator==(const TemporalEDWithinGeometryLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] TemporalEDWithinGeometryLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] TemporalEDWithinGeometryLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction lon;
    LogicalFunction lat;
    LogicalFunction timestamp;
    LogicalFunction geometry;
    LogicalFunction distance;

    friend Reflector<TemporalEDWithinGeometryLogicalFunction>;
};

static_assert(LogicalFunctionConcept<TemporalEDWithinGeometryLogicalFunction>);

template <>
struct Reflector<TemporalEDWithinGeometryLogicalFunction>
{
    Reflected operator()(const TemporalEDWithinGeometryLogicalFunction& function) const;
};

template <>
struct Unreflector<TemporalEDWithinGeometryLogicalFunction>
{
    TemporalEDWithinGeometryLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedTemporalEDWithinGeometryLogicalFunction
{
    std::optional<LogicalFunction> lon;
    std::optional<LogicalFunction> lat;
    std::optional<LogicalFunction> timestamp;
    std::optional<LogicalFunction> geometry;
    std::optional<LogicalFunction> distance;
};
}

FMT_OSTREAM(NES::TemporalEDWithinGeometryLogicalFunction);
