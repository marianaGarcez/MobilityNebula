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

/**
 * @brief Logical function for nearest approach distance between two temporal points
 *        represented by (lon1, lat1, ts1, lon2, lat2, ts2).
 *
 * The underlying MEOS implementation uses nad_tgeo_tgeo.
 */
class NearestApproachDistanceLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "NearestApproachDistance";

    NearestApproachDistanceLogicalFunction(const LogicalFunction& lon1,
                                           const LogicalFunction& lat1,
                                           const LogicalFunction& ts1,
                                           const LogicalFunction& lon2,
                                           const LogicalFunction& lat2,
                                           const LogicalFunction& ts2);

    [[nodiscard]] bool operator==(const NearestApproachDistanceLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] NearestApproachDistanceLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] NearestApproachDistanceLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    LogicalFunction lon1;
    LogicalFunction lat1;
    LogicalFunction ts1;
    LogicalFunction lon2;
    LogicalFunction lat2;
    LogicalFunction ts2;

    friend Reflector<NearestApproachDistanceLogicalFunction>;
};

static_assert(LogicalFunctionConcept<NearestApproachDistanceLogicalFunction>);

template <>
struct Reflector<NearestApproachDistanceLogicalFunction>
{
    Reflected operator()(const NearestApproachDistanceLogicalFunction& function) const;
};

template <>
struct Unreflector<NearestApproachDistanceLogicalFunction>
{
    NearestApproachDistanceLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedNearestApproachDistanceLogicalFunction
{
    std::optional<LogicalFunction> lon1;
    std::optional<LogicalFunction> lat1;
    std::optional<LogicalFunction> ts1;
    std::optional<LogicalFunction> lon2;
    std::optional<LogicalFunction> lat2;
    std::optional<LogicalFunction> ts2;
};
}

FMT_OSTREAM(NES::NearestApproachDistanceLogicalFunction);
