#pragma once
#include <taskforge/database.hpp>
#include <taskforge/runtime.hpp>
#include <cmath>
#include <functional>
#include <map>

namespace taskforge {
struct Assignment { std::string id; std::string attempt; std::string type; nlohmann::json payload; };
struct LeaseLost : std::runtime_error { LeaseLost() : std::runtime_error("lease lost") {} };
struct Cancelled : std::runtime_error { Cancelled() : std::runtime_error("cancellation requested") {} };
class ExecutionContext {
    Database& database_;
    const Assignment& job_;
    std::string worker_;
    std::stop_token stop_;
    std::chrono::steady_clock::time_point renewed_{};
public:
    ExecutionContext(Database& database, const Assignment& job, std::string worker, std::stop_token stop)
        : database_(database), job_(job), worker_(std::move(worker)), stop_(stop) {}
    void checkpoint() {
        if (stop_.stop_requested()) throw LeaseLost();
        const auto now = std::chrono::steady_clock::now();
        if (now - renewed_ < std::chrono::milliseconds(400)) return;
        auto response = database_.query("SELECT renew_lease($1::uuid,$2::uuid,$3::integer)", {job_.id, worker_, job_.attempt});
        auto state = response.get(0, 0);
        if (state.empty()) throw LeaseLost();
        if (state == "CANCEL_REQUESTED") throw Cancelled();
        renewed_ = now;
    }
    void sleep(int milliseconds) {
        const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(milliseconds);
        do {
            checkpoint();
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(until - std::chrono::steady_clock::now());
            if (remaining.count() <= 0) break;
            pause(stop_, std::min(remaining, std::chrono::milliseconds(50)));
        } while (true);
        checkpoint();
    }
};
using Handler = std::function<nlohmann::json(const Assignment&, ExecutionContext&)>;
inline const std::map<std::string, Handler>& handlers() {
    static const std::map<std::string, Handler> registry{
        {"noop", [](const Assignment&, ExecutionContext& ctx) -> nlohmann::json { ctx.checkpoint(); return {{"ok", true}}; }},
        {"echo", [](const Assignment& job, ExecutionContext& ctx) { ctx.checkpoint(); return job.payload; }},
        {"sum", [](const Assignment& job, ExecutionContext& ctx) -> nlohmann::json {
            const auto& values = job.payload.at("numbers");
            if (!values.is_array() || values.size() > 10000) throw std::runtime_error("numbers must be an array of at most 10000 values");
            double total = 0;
            for (const auto& value : values) {
                ctx.checkpoint();
                total += value.get<double>();
                if (!std::isfinite(total)) throw std::runtime_error("non-finite sum");
            }
            return {{"sum", total}};
        }},
        {"sleep", [](const Assignment& job, ExecutionContext& ctx) -> nlohmann::json {
            const auto milliseconds = job.payload.at("duration_ms").get<int>();
            if (milliseconds < 0 || milliseconds > 3600000) throw std::runtime_error("invalid duration_ms");
            ctx.sleep(milliseconds); return {{"slept_ms", milliseconds}};
        }},
        {"fail", [](const Assignment& job, ExecutionContext& ctx) -> nlohmann::json {
            ctx.checkpoint();
            if (std::stoi(job.attempt) <= job.payload.value("fail_until_attempt", 100000)) throw std::runtime_error(job.payload.value("message", std::string("intentional handler failure")));
            return {{"recovered", true}};
        }}
    };
    return registry;
}
}
