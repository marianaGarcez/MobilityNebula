#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>

namespace NES {

enum class GeoFunctionId : uint8_t {
    EDWithinTGeoGeo = 0,
    TemporalEIntersectsGeometry = 1,
    TemporalAIntersectsGeometry = 2,
    TemporalEContainsGeometry = 3,
    TGeoAtStbox = 4,
    NearestApproachDistance = 5,
    TemporalIntersects = 6,
    TemporalSequence = 7,
    TemporalExtKalmanFilter = 8,
    KnnAgg = 9,
    Count = 10,
};

struct GeoFunctionStats {
    std::atomic<uint64_t> calls{0};
    std::atomic<uint64_t> total_ns{0};
};

inline std::array<GeoFunctionStats, static_cast<size_t>(GeoFunctionId::Count)> g_geo_stats{};
inline std::atomic<bool> g_geo_enabled{false};
inline std::atomic<uint64_t> g_geo_flush_every{0};
inline std::atomic<uint64_t> g_geo_total_calls{0};
inline std::atomic<uint64_t> g_geo_last_flush_calls{0};
inline std::mutex g_geo_dump_mutex;

inline bool geo_env_truthy(const char* value) {
    if (!value) {
        return false;
    }
    std::string v(value);
    for (auto& ch : v) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return (v == "1" || v == "true" || v == "yes" || v == "on");
}

inline uint64_t geo_env_u64(const char* value, uint64_t default_val) {
    if (!value || !*value) {
        return default_val;
    }
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end == value) {
        return default_val;
    }
    return static_cast<uint64_t>(parsed);
}

inline const char* geo_function_name(GeoFunctionId id) {
    switch (id) {
        case GeoFunctionId::EDWithinTGeoGeo:
            return "edwithin_tgeo_geo";
        case GeoFunctionId::TemporalEIntersectsGeometry:
            return "temporal_eintersects_geometry";
        case GeoFunctionId::TemporalAIntersectsGeometry:
            return "temporal_aintersects_geometry";
        case GeoFunctionId::TemporalEContainsGeometry:
            return "temporal_econtains_geometry";
        case GeoFunctionId::TGeoAtStbox:
            return "tgeo_at_stbox";
        case GeoFunctionId::NearestApproachDistance:
            return "nearestApproachDistance";
        case GeoFunctionId::TemporalIntersects:
            return "temporal_intersects";
        case GeoFunctionId::TemporalSequence:
            return "temporal_sequence";
        case GeoFunctionId::TemporalExtKalmanFilter:
            return "temporal_ext_kalman_filter";
        case GeoFunctionId::KnnAgg:
            return "knn_agg";
        case GeoFunctionId::Count:
            return "unknown";
    }
    return "unknown";
}

inline std::filesystem::path geo_metrics_path() {
    const char* path = std::getenv("NES_GEO_FUNCTION_METRICS_FILE");
    if (path && *path) {
        return std::filesystem::path(path);
    }
    path = std::getenv("NES_TGEO_AT_STBOX_METRICS_FILE");
    if (path && *path) {
        return std::filesystem::path(path);
    }
    return std::filesystem::path("/workspace/Output/geo_function_metrics.json");
}

inline void geo_dump_metrics() {
    if (!g_geo_enabled.load(std::memory_order_relaxed)) {
        return;
    }
    std::lock_guard<std::mutex> lock(g_geo_dump_mutex);
    std::filesystem::path out_path = geo_metrics_path();
    try {
        if (!out_path.parent_path().empty()) {
            std::filesystem::create_directories(out_path.parent_path());
        }
    } catch (...) {
    }

    std::ofstream out(out_path);
    if (!out.is_open()) {
        return;
    }

    out << "{";
    bool first = true;
    for (size_t i = 0; i < static_cast<size_t>(GeoFunctionId::Count); ++i) {
        const auto calls = g_geo_stats[i].calls.load(std::memory_order_relaxed);
        const auto total_ns = g_geo_stats[i].total_ns.load(std::memory_order_relaxed);
        if (calls == 0 && total_ns == 0) {
            continue;
        }
        if (!first) {
            out << ",";
        }
        first = false;
        out << "\"" << geo_function_name(static_cast<GeoFunctionId>(i)) << "\":"
            << "{\"calls\":" << calls << ",\"total_ns\":" << total_ns << "}";
    }
    out << "}";
}

inline bool geo_timing_enabled() {
    static std::once_flag init_flag;
    std::call_once(init_flag, []() {
        bool enabled = geo_env_truthy(std::getenv("NES_GEO_FUNCTION_TIMING"));
        if (!enabled && geo_env_truthy(std::getenv("NES_TGEO_AT_STBOX_TIMING"))) {
            enabled = true;
        }
        g_geo_enabled.store(enabled, std::memory_order_relaxed);
        if (enabled) {
            uint64_t flush_every = geo_env_u64(std::getenv("NES_GEO_FUNCTION_FLUSH_EVERY"), 0);
            if (flush_every == 0) {
                flush_every = geo_env_u64(std::getenv("NES_TGEO_AT_STBOX_FLUSH_EVERY"), 0);
            }
            g_geo_flush_every.store(flush_every, std::memory_order_relaxed);
            std::atexit(geo_dump_metrics);
        }
    });
    return g_geo_enabled.load(std::memory_order_relaxed);
}

inline void geo_record_ns(GeoFunctionId id, uint64_t elapsed_ns) {
    g_geo_stats[static_cast<size_t>(id)].calls.fetch_add(1, std::memory_order_relaxed);
    g_geo_stats[static_cast<size_t>(id)].total_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);

    const uint64_t flush_every = g_geo_flush_every.load(std::memory_order_relaxed);
    if (flush_every == 0) {
        return;
    }
    const uint64_t total_calls = g_geo_total_calls.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t last = g_geo_last_flush_calls.load(std::memory_order_relaxed);
    if (total_calls - last < flush_every) {
        return;
    }
    if (g_geo_last_flush_calls.compare_exchange_strong(
            last, total_calls, std::memory_order_relaxed, std::memory_order_relaxed)) {
        geo_dump_metrics();
    }
}

class GeoFunctionTimingScope {
public:
    explicit GeoFunctionTimingScope(GeoFunctionId id)
        : id(id),
          enabled(geo_timing_enabled()),
          start(enabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point()) {}

    ~GeoFunctionTimingScope() {
        if (!enabled) {
            return;
        }
        const auto end = std::chrono::steady_clock::now();
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        if (ns > 0) {
            geo_record_ns(id, static_cast<uint64_t>(ns));
        }
    }

private:
    GeoFunctionId id;
    bool enabled;
    std::chrono::steady_clock::time_point start;
};

} // namespace NES
