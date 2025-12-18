#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::MeosTrajectoryDetail
{
struct TrajectoryFieldIndices
{
    uint64_t lonFieldIndex;
    uint64_t latFieldIndex;
    uint64_t tsFieldIndex;
};

namespace detail
{
template <typename T>
T readUnalignedChecked(std::span<const std::byte> mem, uint64_t offset)
{
    if (offset + sizeof(T) > mem.size())
    {
        throw UnknownOperation("Attempted to read beyond TupleBuffer bounds while building trajectory.");
    }
    T value{};
    std::memcpy(&value, mem.data() + offset, sizeof(T));
    return value;
}

inline double readFloating(const MemoryLayout* layout,
                           uint64_t tupleIndex,
                           uint64_t fieldIndex,
                           std::span<const std::byte> tupleBufferMem)
{
    const auto fieldType = layout->getPhysicalType(fieldIndex).type;
    const auto offset = layout->getFieldOffset(tupleIndex, fieldIndex);

    switch (fieldType)
    {
        case DataType::Type::FLOAT32:
            return static_cast<double>(readUnalignedChecked<float>(tupleBufferMem, offset));
        case DataType::Type::FLOAT64:
            return readUnalignedChecked<double>(tupleBufferMem, offset);
        default:
            throw UnknownOperation("Unsupported floating-point field type for trajectory.");
    }
}

inline int64_t readIntLike(const MemoryLayout* layout,
                           uint64_t tupleIndex,
                           uint64_t fieldIndex,
                           std::span<const std::byte> tupleBufferMem)
{
    const auto fieldType = layout->getPhysicalType(fieldIndex).type;
    const auto offset = layout->getFieldOffset(tupleIndex, fieldIndex);

    switch (fieldType)
    {
        case DataType::Type::INT8:
            return static_cast<int64_t>(readUnalignedChecked<int8_t>(tupleBufferMem, offset));
        case DataType::Type::INT16:
            return static_cast<int64_t>(readUnalignedChecked<int16_t>(tupleBufferMem, offset));
        case DataType::Type::INT32:
            return static_cast<int64_t>(readUnalignedChecked<int32_t>(tupleBufferMem, offset));
        case DataType::Type::INT64:
            return static_cast<int64_t>(readUnalignedChecked<int64_t>(tupleBufferMem, offset));
        case DataType::Type::UINT8:
            return static_cast<int64_t>(readUnalignedChecked<uint8_t>(tupleBufferMem, offset));
        case DataType::Type::UINT16:
            return static_cast<int64_t>(readUnalignedChecked<uint16_t>(tupleBufferMem, offset));
        case DataType::Type::UINT32:
            return static_cast<int64_t>(readUnalignedChecked<uint32_t>(tupleBufferMem, offset));
        case DataType::Type::UINT64:
            return static_cast<int64_t>(readUnalignedChecked<uint64_t>(tupleBufferMem, offset));
        default:
            throw UnknownOperation("Unsupported integer field type for trajectory timestamp.");
    }
}

inline int64_t normalizeEpochToMicros(const int64_t epochLike)
{
    // Heuristics based on magnitude:
    // >=1e18 -> nanoseconds, >=1e15 -> microseconds, >=1e12 -> milliseconds, else seconds.
    // Normalize to microseconds (MEOS timestamps support microsecond precision).
    const auto absValue = epochLike < 0 ? -epochLike : epochLike;
    if (absValue >= 1000000000000000000LL)
    {
        return epochLike / 1000LL;
    }
    if (absValue >= 1000000000000000LL)
    {
        return epochLike;
    }
    if (absValue >= 1000000000000LL)
    {
        return epochLike * 1000LL;
    }
    return epochLike * 1000000LL;
}

inline std::string formatUtcTimestampFromMicros(int64_t epochMicros)
{
    constexpr int64_t MicrosPerSecond = 1000000LL;

    int64_t seconds = epochMicros / MicrosPerSecond;
    int64_t micros = epochMicros % MicrosPerSecond;
    if (micros < 0)
    {
        micros += MicrosPerSecond;
        seconds -= 1;
    }

    const auto tp = std::chrono::time_point<std::chrono::system_clock>(std::chrono::seconds(seconds));
    const std::time_t time = std::chrono::system_clock::to_time_t(tp);

    std::tm utcTm{};
#if defined(_WIN32)
    if (gmtime_s(&utcTm, &time) != 0)
    {
        return "1970-01-01 00:00:00.000000+00";
    }
#else
    if (gmtime_r(&time, &utcTm) == nullptr)
    {
        return "1970-01-01 00:00:00.000000+00";
    }
#endif

    std::array<char, 64> buffer{};
    const auto baseLen = std::strftime(buffer.data(), buffer.size(), "%Y-%m-%d %H:%M:%S", &utcTm);
    if (baseLen == 0)
    {
        return "1970-01-01 00:00:00.000000+00";
    }

    std::array<char, 96> full{};
    std::snprintf(full.data(),
                  full.size(),
                  "%s.%06lld+00",
                  buffer.data(),
                  static_cast<long long>(micros));
    return std::string(full.data());
}
} // namespace detail

inline char* buildSortedTemporalInstantSetString(const Nautilus::Interface::PagedVector* pagedVector,
                                                 const MemoryLayout* layout,
                                                 TrajectoryFieldIndices fieldIndices)
{
    struct Point
    {
        int64_t tsMicros;
        double lon;
        double lat;
        uint64_t arrivalIndex;
    };

    const auto total = pagedVector->getTotalNumberOfEntries();
    std::vector<Point> points;
    points.reserve(total);

    for (uint64_t entryPos = 0; entryPos < total; ++entryPos)
    {
        const auto* tupleBuffer = pagedVector->getTupleBufferForEntry(entryPos);
        const auto posOnPageOpt = pagedVector->getBufferPosForEntry(entryPos);
        if (tupleBuffer == nullptr || !posOnPageOpt.has_value())
        {
            continue;
        }

        const auto tupleIndex = posOnPageOpt.value();
        const auto tupleBufferMem = tupleBuffer->getAvailableMemoryArea<std::byte>();

        const auto lon = detail::readFloating(layout, tupleIndex, fieldIndices.lonFieldIndex, tupleBufferMem);
        const auto lat = detail::readFloating(layout, tupleIndex, fieldIndices.latFieldIndex, tupleBufferMem);
        const auto tsRaw = detail::readIntLike(layout, tupleIndex, fieldIndices.tsFieldIndex, tupleBufferMem);

        points.push_back(Point{
            detail::normalizeEpochToMicros(tsRaw),
            lon,
            lat,
            entryPos,
        });
    }

    std::sort(points.begin(),
              points.end(),
              [](const Point& a, const Point& b)
              {
                  if (a.tsMicros != b.tsMicros)
                  {
                      return a.tsMicros < b.tsMicros;
                  }
                  return a.arrivalIndex < b.arrivalIndex;
              });

    std::string out;
    out.reserve(points.size() * 96 + 2);
    out.push_back('{');

    int64_t lastTsMicros = std::numeric_limits<int64_t>::min();
    for (size_t i = 0; i < points.size(); ++i)
    {
        auto tsMicros = points[i].tsMicros;
        if (tsMicros <= lastTsMicros)
        {
            tsMicros = lastTsMicros + 1;
        }
        lastTsMicros = tsMicros;

        const auto timestampStr = detail::formatUtcTimestampFromMicros(tsMicros);

        if (i > 0)
        {
            out.append(", ");
        }

        std::array<char, 160> pointBuf{};
        std::snprintf(pointBuf.data(),
                      pointBuf.size(),
                      "Point(%.6f %.6f)@%s",
                      points[i].lon,
                      points[i].lat,
                      timestampStr.c_str());
        out.append(pointBuf.data());
    }

    out.push_back('}');

    char* result = static_cast<char*>(std::malloc(out.size() + 1));
    if (result == nullptr)
    {
        return nullptr;
    }
    std::memcpy(result, out.c_str(), out.size() + 1);
    return result;
}
} // namespace NES::MeosTrajectoryDetail
