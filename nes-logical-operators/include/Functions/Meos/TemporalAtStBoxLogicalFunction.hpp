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

class TemporalAtStBoxLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "TemporalAtStBox";

    TemporalAtStBoxLogicalFunction(const LogicalFunction& lon, const LogicalFunction& lat,
                                   const LogicalFunction& timestamp, const LogicalFunction& stbox);

    TemporalAtStBoxLogicalFunction(const LogicalFunction& lon, const LogicalFunction& lat,
                                   const LogicalFunction& timestamp, const LogicalFunction& stbox,
                                   const LogicalFunction& borderInclusive);

    [[nodiscard]] bool operator==(const TemporalAtStBoxLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] TemporalAtStBoxLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] TemporalAtStBoxLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    std::vector<LogicalFunction> parameters;
    bool hasBorderParam;

    friend Reflector<TemporalAtStBoxLogicalFunction>;
};

static_assert(LogicalFunctionConcept<TemporalAtStBoxLogicalFunction>);

template <>
struct Reflector<TemporalAtStBoxLogicalFunction>
{
    Reflected operator()(const TemporalAtStBoxLogicalFunction& function) const;
};

template <>
struct Unreflector<TemporalAtStBoxLogicalFunction>
{
    TemporalAtStBoxLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedTemporalAtStBoxLogicalFunction
{
    std::optional<LogicalFunction> lon;
    std::optional<LogicalFunction> lat;
    std::optional<LogicalFunction> timestamp;
    std::optional<LogicalFunction> stbox;
    std::optional<LogicalFunction> borderInclusive;
};
}

FMT_OSTREAM(NES::TemporalAtStBoxLogicalFunction);
