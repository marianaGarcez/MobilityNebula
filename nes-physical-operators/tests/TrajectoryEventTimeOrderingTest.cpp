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

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>

#include <DataTypes/Schema.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <nautilus/val.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include "../src/Aggregation/Function/Meos/TrajectoryEventTimeOrdering.hpp"

namespace NES
{

class TrajectoryEventTimeOrderingTest : public Testing::BaseUnitTest
{
public:
    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(TrajectoryEventTimeOrderingTest, SortsByEventTimeAndStabilizesTies)
{
    using Nautilus::Interface::PagedVector;
    using Nautilus::Interface::PagedVectorRef;

    auto schema = Schema{}
                      .addField("lon", DataType::Type::FLOAT64)
                      .addField("lat", DataType::Type::FLOAT64)
                      .addField("timestamp", DataType::Type::UINT64);

    auto layout = std::make_shared<RowLayout>(4096, schema);
    auto bufferRef = std::make_shared<Nautilus::Interface::BufferRef::RowTupleBufferRef>(layout);
    auto bufferManager = BufferManager::create(4096, 16);

    PagedVector pagedVector;
    PagedVectorRef pagedVectorRef(nautilus::val<PagedVector*>(&pagedVector), bufferRef);
    auto provider = nautilus::val<AbstractBufferProvider*>(bufferManager.get());

    // Insert out-of-order by event time (milliseconds since epoch).
    // Use realistic 13-digit millisecond epoch values so the normalization logic treats them as milliseconds.
    // Base = 1700000000000ms -> 2023-11-14 22:13:20+00.
    // Expected event-time order:
    //   base, base (tie stabilized but forced strictly increasing by +1 micro), base+1500ms, base+2000ms
    constexpr uint64_t baseMs = 1700000000000ULL;
    pagedVectorRef.writeRecord(
        Nautilus::Record({{"lon", Nautilus::VarVal(1.0)}, {"lat", Nautilus::VarVal(1.0)}, {"timestamp", Nautilus::VarVal(uint64_t(baseMs + 2000ULL))}}),
        provider);
    pagedVectorRef.writeRecord(
        Nautilus::Record({{"lon", Nautilus::VarVal(2.0)}, {"lat", Nautilus::VarVal(2.0)}, {"timestamp", Nautilus::VarVal(uint64_t(baseMs))}}),
        provider);
    pagedVectorRef.writeRecord(
        Nautilus::Record({{"lon", Nautilus::VarVal(3.0)}, {"lat", Nautilus::VarVal(3.0)}, {"timestamp", Nautilus::VarVal(uint64_t(baseMs))}}),
        provider);
    pagedVectorRef.writeRecord(
        Nautilus::Record({{"lon", Nautilus::VarVal(4.0)}, {"lat", Nautilus::VarVal(4.0)}, {"timestamp", Nautilus::VarVal(uint64_t(baseMs + 1500ULL))}}),
        provider);

    const auto lonIdx = layout->getFieldIndexFromName("lon");
    const auto latIdx = layout->getFieldIndexFromName("lat");
    const auto tsIdx = layout->getFieldIndexFromName("timestamp");
    ASSERT_TRUE(lonIdx.has_value());
    ASSERT_TRUE(latIdx.has_value());
    ASSERT_TRUE(tsIdx.has_value());

    char* sortedStr = MeosTrajectoryDetail::buildSortedTemporalInstantSetString(
        &pagedVector,
        layout.get(),
        MeosTrajectoryDetail::TrajectoryFieldIndices{lonIdx.value(), latIdx.value(), tsIdx.value()});
    ASSERT_NE(sortedStr, nullptr);

    const std::string s(sortedStr);
    std::free(sortedStr);

    const auto pBaseA =
        s.find("Point(2.000000 2.000000)@2023-11-14 22:13:20.000000+00");
    const auto pBaseB =
        s.find("Point(3.000000 3.000000)@2023-11-14 22:13:20.000001+00");
    const auto p1500 =
        s.find("Point(4.000000 4.000000)@2023-11-14 22:13:21.500000+00");
    const auto p2000 =
        s.find("Point(1.000000 1.000000)@2023-11-14 22:13:22.000000+00");

    ASSERT_NE(pBaseA, std::string::npos) << s;
    ASSERT_NE(pBaseB, std::string::npos) << s;
    ASSERT_NE(p1500, std::string::npos) << s;
    ASSERT_NE(p2000, std::string::npos) << s;

    EXPECT_LT(pBaseA, pBaseB);
    EXPECT_LT(pBaseB, p1500);
    EXPECT_LT(p1500, p2000);
}

}
