#pragma once
#include <libpq-fe.h>
#include <poll.h>
#include <cerrno>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace taskforge {
class Result {
    std::unique_ptr<PGresult, decltype(&PQclear)> value_;
public:
    explicit Result(PGresult* value) : value_(value, PQclear) {
        if (!value || (PQresultStatus(value) != PGRES_TUPLES_OK && PQresultStatus(value) != PGRES_COMMAND_OK)) {
            throw std::runtime_error(value ? PQresultErrorMessage(value) : "database returned no result");
        }
    }
    int rows() const { return PQntuples(value_.get()); }
    std::string get(int row, int col) const {
        if (row < 0 || row >= rows() || col < 0 || col >= PQnfields(value_.get())) throw std::out_of_range("database result index");
        return PQgetisnull(value_.get(), row, col) ? "" : PQgetvalue(value_.get(), row, col);
    }
};
class Database {
    using Clock = std::chrono::steady_clock;
    std::unique_ptr<PGconn, decltype(&PQfinish)> connection_;
    [[noreturn]] void unavailable(const char* reason) {
        connection_.reset();
        throw std::runtime_error(reason);
    }
    void wait(short events, Clock::time_point deadline) {
        while (true) {
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now()).count();
            if (remaining <= 0) unavailable("database I/O deadline exceeded");
            const int socket = PQsocket(connection_.get());
            if (socket < 0) unavailable("database socket unavailable");
            pollfd descriptor{socket, events, 0};
            const int ready = ::poll(&descriptor, 1, static_cast<int>(remaining));
            if (ready < 0 && errno == EINTR) continue;
            if (ready == 0) unavailable("database I/O deadline exceeded");
            if (ready < 0 || (descriptor.revents & (POLLERR | POLLNVAL))) unavailable("database I/O unavailable");
            return;
        }
    }
public:
    explicit Database(const std::string& uri) : connection_(PQconnectStart(uri.c_str()), PQfinish) {
        if (!connection_) throw std::runtime_error("database connection unavailable");
        const auto deadline = Clock::now() + std::chrono::seconds(3);
        while (true) {
            const auto state = PQconnectPoll(connection_.get());
            if (state == PGRES_POLLING_OK) break;
            if (state == PGRES_POLLING_FAILED) unavailable("database connection unavailable");
            wait(state == PGRES_POLLING_WRITING ? POLLOUT : POLLIN, deadline);
        }
        if (PQsetnonblocking(connection_.get(), 1) != 0) unavailable("database nonblocking setup failed");
        query("SET statement_timeout='3s'");
        query("SET lock_timeout='1s'");
    }
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;
    Result query(const std::string& sql, const std::vector<std::string>& parameters = {}) {
        if (!connection_) throw std::runtime_error("database connection unavailable");
        const auto deadline = Clock::now() + std::chrono::seconds(5);
        std::vector<const char*> values;
        values.reserve(parameters.size());
        for (const auto& value : parameters) values.push_back(value.c_str());
        if (!PQsendQueryParams(connection_.get(), sql.c_str(), static_cast<int>(values.size()), nullptr, values.data(), nullptr, nullptr, 0))
            unavailable("database query send failed");
        while (true) {
            const int status = PQflush(connection_.get());
            if (status == 0) break;
            if (status < 0) unavailable("database query flush failed");
            wait(POLLOUT, deadline);
        }
        std::unique_ptr<PGresult, decltype(&PQclear)> result(nullptr, PQclear);
        while (true) {
            while (PQisBusy(connection_.get())) {
                wait(POLLIN, deadline);
                if (!PQconsumeInput(connection_.get())) unavailable("database query receive failed");
            }
            auto* next = PQgetResult(connection_.get());
            if (!next) break; // Drain the protocol before reusing the connection, including SQL errors.
            result.reset(next);
        }
        return Result(result.release());
    }
};
}