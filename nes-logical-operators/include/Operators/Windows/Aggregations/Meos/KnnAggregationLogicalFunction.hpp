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
#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
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
 */
class KnnAggregationLogicalFunction final
{
public:
    KnnAggregationLogicalFunction(
        const FieldAccessLogicalFunction& distanceField,
        const FieldAccessLogicalFunction& neighbourField,
        FieldAccessLogicalFunction asField,
        std::size_t k);

    ~KnnAggregationLogicalFunction() = default;

    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;
    [[nodiscard]] FieldAccessLogicalFunction getDistanceField() const;
    [[nodiscard]] FieldAccessLogicalFunction getNeighbourField() const;
    [[nodiscard]] std::size_t getK() const noexcept;

    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] KnnAggregationLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] KnnAggregationLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] KnnAggregationLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] KnnAggregationLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] KnnAggregationLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] KnnAggregationLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    [[nodiscard]] bool operator==(const KnnAggregationLogicalFunction& other) const;

private:
    static constexpr std::string_view NAME = "KnnAgg";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    FieldAccessLogicalFunction distanceField;
    FieldAccessLogicalFunction neighbourField;
    std::size_t k;
};

static_assert(WindowAggregationFunctionConcept<KnnAggregationLogicalFunction>);

template <>
struct Reflector<KnnAggregationLogicalFunction>
{
    Reflected operator()(const KnnAggregationLogicalFunction& function) const;
};

template <>
struct Unreflector<KnnAggregationLogicalFunction>
{
    KnnAggregationLogicalFunction operator()(const Reflected& reflected) const;
};
}

namespace NES::detail
{
struct ReflectedKnnAggregationLogicalFunction
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
    FieldAccessLogicalFunction distanceField;
    FieldAccessLogicalFunction neighbourField;
    std::size_t k;
};

}
