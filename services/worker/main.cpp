#include <taskforge/handlers.hpp>
#include <taskforge/thread_pool.hpp>

namespace {
void execute(taskforge::Database& database, const taskforge::Assignment& job, const std::string& worker, std::stop_token stop) {
    using namespace taskforge;
    std::string outcome = "COMPLETED", failure;
    nlohmann::json result = nullptr;
    try {
        ExecutionContext context(database, job, worker, stop);
        const auto found = handlers().find(job.type);
        if (found == handlers().end()) throw std::runtime_error("handler not allowlisted");
        result = found->second(job, context);
    } catch (const LeaseLost&) {
        log("job.abandoned", {{"job_id", job.id}, {"attempt", job.attempt}}); return;
    } catch (const Cancelled& error) {
        outcome = "CANCELLED"; failure = error.what();
    } catch (const std::exception& error) {
        outcome = "FAILED"; failure = error.what();
    }
    auto stored = database.query("SELECT finish_job($1::uuid,$2::uuid,$3::integer,$4,$5::jsonb,$6)", {job.id, worker, job.attempt, outcome, result.dump(), failure});
    // A failed database call throws. Never log completion without a durable confirmation.
    log(stored.get(0, 0) == "t" ? "job.persisted" : "job.stale_completion_rejected", {{"job_id", job.id}, {"attempt", job.attempt}, {"outcome", outcome}});
}
void lane(std::stop_token stop, const std::string& uri, const std::string& worker) {
    using namespace taskforge;
    while (!stop.stop_requested()) {
        try {
            Database database(uri);
            while (!stop.stop_requested()) {
                auto row = database.query("SELECT * FROM next_assignment($1::uuid)", {worker});
                if (!row.rows()) { pause(stop, std::chrono::milliseconds(100)); continue; }
                Assignment job{row.get(0, 0), row.get(0, 1), row.get(0, 2), nlohmann::json::parse(row.get(0, 3))};
                execute(database, job, worker, stop);
            }
        } catch (const std::exception& error) {
            log("worker.database_error", {{"message", error.what()}});
            pause(stop, std::chrono::milliseconds(1000));
        }
    }
}
}
int main() {
    using namespace taskforge;
    install_signals();
    try {
        const auto uri = environment("DATABASE_URL");
        const auto slots = capacity_from_environment();
        const auto name = environment("WORKER_NAME", "worker");
        std::string id;
        while (!interrupted && id.empty()) {
            try { Database database(uri); id = database.query("SELECT gen_random_uuid()").get(0, 0); }
            catch (const std::exception& error) { log("worker.startup_retry", {{"message", error.what()}}); pause({}, std::chrono::milliseconds(1000)); }
        }
        if (interrupted) return 0;
        ThreadPool pool(static_cast<std::size_t>(slots));
        for (int slot = 0; slot < slots; ++slot) {
            if (!pool.submit([uri, id](std::stop_token stop) { lane(stop, uri, id); })) throw std::runtime_error("thread pool rejected execution lane");
        }
        log("worker.started", {{"worker_id", id}, {"capacity", slots}});
        while (!interrupted) {
            try {
                Database database(uri);
                database.query("SELECT register_worker($1::uuid,$2,$3::integer,false)", {id, name, std::to_string(slots)});
            } catch (const std::exception& error) { log("worker.heartbeat_error", {{"message", error.what()}}); }
            pause({}, std::chrono::milliseconds(1000));
        }
        try {
            Database database(uri);
            database.query("SELECT register_worker($1::uuid,$2,$3::integer,true)", {id, name, std::to_string(slots)});
        } catch (const std::exception& error) { log("worker.drain_error", {{"message", error.what()}}); }
        pool.shutdown();
        log("worker.stopped", {{"worker_id", id}});
        return 0;
    } catch (const std::exception& error) { log("worker.fatal", {{"message", error.what()}}); return 1; }
}
