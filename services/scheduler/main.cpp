#include <taskforge/database.hpp>
#include <taskforge/runtime.hpp>
#include <thread>

int main() {
    using namespace taskforge;
    install_signals();
    try {
        const auto uri = environment("DATABASE_URL");
        log("scheduler.started");
        while (!interrupted) {
            try {
                Database database(uri);
                while (!interrupted) {
                    database.query("SELECT maintain_jobs()");
                    const auto workers = database.query("SELECT id,capacity FROM workers WHERE NOT draining AND heartbeat_at>clock_timestamp()-interval '10 seconds' ORDER BY heartbeat_at,id");
                    for (int i = 0; i < workers.rows() && !interrupted; ++i) {
                        const int slots = std::stoi(workers.get(i, 1));
                        for (int slot = 0; slot < slots && !interrupted; ++slot) {
                            auto assigned = database.query("SELECT * FROM dispatch_job($1::uuid)", {workers.get(i, 0)});
                            if (!assigned.rows()) break;
                            log("job.assigned", {{"job_id", assigned.get(0, 0)}, {"attempt", assigned.get(0, 1)}});
                        }
                    }
                    pause({}, std::chrono::milliseconds(100));
                }
            } catch (const std::exception& error) {
                log("scheduler.database_error", {{"message", error.what()}});
                pause({}, std::chrono::milliseconds(1000));
            }
        }
        log("scheduler.stopped");
        return 0;
    } catch (const std::exception& error) {
        log("scheduler.fatal", {{"message", error.what()}}); return 1;
    }
}
