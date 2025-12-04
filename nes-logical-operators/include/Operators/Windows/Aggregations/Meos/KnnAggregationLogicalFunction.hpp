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

#include <cstddef>
#include <string_view>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/**
 * @brief Window aggregation that keeps the k nearest neighbours for a reference object
 *        inside each window/group. The aggregation operates on a pre-computed distance
 *        field and a neighbour identifier field and returns a VARSIZED summary.
 *
 * SQL shape (as used in Query 9):
 *   KNN_AGG(mindist, device_id2, 10) AS neighbors
 *
 * - mindist:  distance field (e.g., from nearestApproachDistance)
 * - device_id2: neighbour identifier
 * - 10:       k (maximum number of neighbours to keep)
 */
class KnnAggregationLogicalFunction final : public WindowAggregationLogicalFunction
{
public:
    static constexpr std::string_view NAME = "KnnAgg";

    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& distanceField,
           const FieldAccessLogicalFunction& neighbourField,
           std::size_t k);

    KnnAggregationLogicalFunction(FieldAccessLogicalFunction distanceField,
                                  FieldAccessLogicalFunction neighbourField,
                                  FieldAccessLogicalFunction asField,
                                  std::size_t k);

    void inferStamp(const Schema& schema) override;
    ~KnnAggregationLogicalFunction() override = default;

    [[nodiscard]] SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] const FieldAccessLogicalFunction& getDistanceField() const noexcept;
    [[nodiscard]] const FieldAccessLogicalFunction& getNeighbourField() const noexcept;
    [[nodiscard]] std::size_t getK() const noexcept;

private:
    FieldAccessLogicalFunction distanceField;
    FieldAccessLogicalFunction neighbourField;
    std::size_t k;
};

} // namespace NES

