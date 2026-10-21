#pragma once
#include <nlohmann/json.hpp>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <string>

namespace taskforge {
inline volatile std::sig_atomic_t interrupted = 0;
inline void handle_signal(int) { interrupted = 1; }
inline void install_signals() { std::signal(SIGINT, handle_signal); std::signal(SIGTERM, handle_signal); }
inline std::string environment(const char* key, const std::string& fallback = "") {
    const char* value = std::getenv(key);
    if (value && *value) return value;
    if (!fallback.empty()) return fallback;
    throw std::runtime_error(std::string("missing environment variable: ") + key);
}
inline int capacity_from_environment() {
    auto text = environment("WORKER_CONCURRENCY", "4");
    std::size_t consumed = 0;
    int slots = std::stoi(text, &consumed);
    if (consumed != text.size() || slots < 1 || slots > 256) throw std::runtime_error("WORKER_CONCURRENCY must be 1..256");
    return slots;
}
inline void log(const std::string& event, const nlohmann::json& details = nlohmann::json::object()) {
    static std::mutex output_mutex;
    std::lock_guard lock(output_mutex);
    std::cout << nlohmann::json{{"event", event}, {"details", details}}.dump() << std::endl;
}
inline void pause(std::stop_token stop, std::chrono::milliseconds duration) {
    std::mutex mutex;
    std::condition_variable_any wake;
    std::unique_lock lock(mutex);
    wake.wait_for(lock, stop, duration, [] { return false; });
}
}
