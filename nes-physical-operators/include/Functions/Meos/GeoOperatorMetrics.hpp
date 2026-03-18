#pragma once

#include <Functions/Meos/GeoFunctionMetrics.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>

namespace NES {

inline std::array<GeoFunctionStats, static_cast<size_t>(GeoFunctionId::Count)> g_geo_operator_stats{};
inline std::atomic<bool> g_geo_operator_enabled{false};
inline std::atomic<uint64_t> g_geo_operator_flush_every{0};
inline std::atomic<uint64_t> g_geo_operator_total_calls{0};
inline std::atomic<uint64_t> g_geo_operator_last_flush_calls{0};
inline std::mutex g_geo_operator_dump_mutex;

inline std::filesystem::path geo_operator_metrics_path() {
    const char* path = std::getenv("NES_GEO_OPERATOR_METRICS_FILE");
    if (path && *path) {
        return std::filesystem::path(path);
    }
    return std::filesystem::path("/workspace/Output/geo_operator_metrics.json");
}

inline void geo_operator_dump_metrics() {
    if (!g_geo_operator_enabled.load(std::memory_order_relaxed)) {
        return;
    }
    std::lock_guard<std::mutex> lock(g_geo_operator_dump_mutex);
    std::filesystem::path out_path = geo_operator_metrics_path();
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
        const auto calls = g_geo_operator_stats[i].calls.load(std::memory_order_relaxed);
        const auto total_ns = g_geo_operator_stats[i].total_ns.load(std::memory_order_relaxed);
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

inline bool geo_operator_timing_enabled() {
    static std::once_flag init_flag;
    std::call_once(init_flag, []() {
        bool enabled = geo_env_truthy(std::getenv("NES_GEO_OPERATOR_TIMING"));
        g_geo_operator_enabled.store(enabled, std::memory_order_relaxed);
        if (enabled) {
            uint64_t flush_every = geo_env_u64(std::getenv("NES_GEO_OPERATOR_FLUSH_EVERY"), 0);
            g_geo_operator_flush_every.store(flush_every, std::memory_order_relaxed);
            std::atexit(geo_operator_dump_metrics);
        }
    });
    return g_geo_operator_enabled.load(std::memory_order_relaxed);
}

inline void geo_operator_record_ns(GeoFunctionId id, uint64_t elapsed_ns) {
    g_geo_operator_stats[static_cast<size_t>(id)].calls.fetch_add(1, std::memory_order_relaxed);
    g_geo_operator_stats[static_cast<size_t>(id)].total_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);

    const uint64_t flush_every = g_geo_operator_flush_every.load(std::memory_order_relaxed);
    if (flush_every == 0) {
        return;
    }
    const uint64_t total_calls = g_geo_operator_total_calls.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t last = g_geo_operator_last_flush_calls.load(std::memory_order_relaxed);
    if (total_calls - last < flush_every) {
        return;
    }
    if (g_geo_operator_last_flush_calls.compare_exchange_strong(
            last, total_calls, std::memory_order_relaxed, std::memory_order_relaxed)) {
        geo_operator_dump_metrics();
    }
}

class GeoOperatorTimingScope {
public:
    explicit GeoOperatorTimingScope(GeoFunctionId id)
        : id(id),
          enabled(geo_operator_timing_enabled()),
          start(enabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point()) {}

    ~GeoOperatorTimingScope() {
        if (!enabled) {
            return;
        }
        const auto end = std::chrono::steady_clock::now();
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        if (ns > 0) {
            geo_operator_record_ns(id, static_cast<uint64_t>(ns));
        }
    }

private:
    GeoFunctionId id;
    bool enabled;
    std::chrono::steady_clock::time_point start;
};

} // namespace NES
