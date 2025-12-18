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
#include <cstring>
#include <memory>
#include <optional>
#include <string>

#include <DataTypes/Schema.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <nautilus/val.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include "../src/Aggregation/Function/Meos/TrajectoryEventTimeOrdering.hpp"

namespace NES
{

class TrajectoryEventTimeOrderingTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TrajectoryEventTimeOrderingTest.log", LogLevel::LOG_DEBUG);
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(TrajectoryEventTimeOrderingTest, SortsByEventTimeAndStabilizesTies)
{
    using Nautilus::Interface::PagedVector;

    auto schema = Schema{}
                      .addField("lon", DataType::Type::FLOAT64)
                      .addField("lat", DataType::Type::FLOAT64)
                      .addField("timestamp", DataType::Type::UINT64);

    auto layout = std::make_shared<RowLayout>(4096, schema);
    auto bufferManager = BufferManager::create(4096, 16);

    PagedVector pagedVector;

    // Insert out-of-order by event time (milliseconds since epoch).
    // Use realistic 13-digit millisecond epoch values so the normalization logic treats them as milliseconds.
    // Base = 1700000000000ms -> 2023-11-14 22:13:20+00.
    // Expected event-time order:
    //   base, base (tie stabilized but forced strictly increasing by +1 micro), base+1500ms, base+2000ms
    constexpr uint64_t baseMs = 1700000000000ULL;

    auto appendRowRecord = [&](double lon, double lat, uint64_t timestampMs)
    {
        pagedVector.appendPageIfFull(bufferManager.get(), layout.get());
        const auto& page = pagedVector.getLastPage();
        const auto recordIndex = page.getNumberOfTuples();

        auto mem = page.getAvailableMemoryArea<std::byte>();
        auto* basePtr = const_cast<std::byte*>(mem.data());

        const auto lonIdx = layout->getFieldIndexFromName("lon").value();
        const auto latIdx = layout->getFieldIndexFromName("lat").value();
        const auto tsIdx = layout->getFieldIndexFromName("timestamp").value();

        const auto lonOff = layout->getFieldOffset(recordIndex, lonIdx);
        const auto latOff = layout->getFieldOffset(recordIndex, latIdx);
        const auto tsOff = layout->getFieldOffset(recordIndex, tsIdx);

        std::memcpy(basePtr + lonOff, &lon, sizeof(double));
        std::memcpy(basePtr + latOff, &lat, sizeof(double));
        std::memcpy(basePtr + tsOff, &timestampMs, sizeof(uint64_t));

        page.setNumberOfTuples(recordIndex + 1);
    };

    appendRowRecord(1.0, 1.0, baseMs + 2000ULL);
    appendRowRecord(2.0, 2.0, baseMs);
    appendRowRecord(3.0, 3.0, baseMs);
    appendRowRecord(4.0, 4.0, baseMs + 1500ULL);

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
