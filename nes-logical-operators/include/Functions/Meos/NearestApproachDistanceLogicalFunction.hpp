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

#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES {

/**
 * @brief Logical function for nearest approach distance between two temporal points
 *        represented by (lon1, lat1, ts1, lon2, lat2, ts2).
 *
 * The underlying MEOS implementation uses nad_tgeo_tgeo.
 */
class NearestApproachDistanceLogicalFunction : public LogicalFunctionConcept {
public:
    static constexpr std::string_view NAME = "NearestApproachDistance";

    NearestApproachDistanceLogicalFunction(LogicalFunction lon1,
                                           LogicalFunction lat1,
                                           LogicalFunction ts1,
                                           LogicalFunction lon2,
                                           LogicalFunction lat2,
                                           LogicalFunction ts2);

    [[nodiscard]] DataType getDataType() const override;
    [[nodiscard]] LogicalFunction withDataType(const DataType& dataType) const override;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;

    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override;

    [[nodiscard]] SerializableFunction serialize() const override;

private:
    DataType dataType;
    std::vector<LogicalFunction> parameters;
};

} // namespace NES

FMT_OSTREAM(NES::NearestApproachDistanceLogicalFunction);

