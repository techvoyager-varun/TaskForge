#pragma once
#include <libpq-fe.h>
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
    std::unique_ptr<PGconn, decltype(&PQfinish)> connection_;
public:
    explicit Database(const std::string& uri) : connection_(PQconnectdb(uri.c_str()), PQfinish) {
        if (!connection_ || PQstatus(connection_.get()) != CONNECTION_OK) throw std::runtime_error("database connection unavailable");
        query("SET statement_timeout='3s'");
        query("SET lock_timeout='1s'");
    }
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;
    Result query(const std::string& sql, const std::vector<std::string>& parameters = {}) {
        std::vector<const char*> values;
        values.reserve(parameters.size());
        for (const auto& value : parameters) values.push_back(value.c_str());
        return Result(PQexecParams(connection_.get(), sql.c_str(), static_cast<int>(values.size()), nullptr, values.data(), nullptr, nullptr, 0));
    }
};
}
