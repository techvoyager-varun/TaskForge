#pragma once
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace taskforge {
// Bounded pending work and joinable C++20 threads; shutdown is cooperative.
class ThreadPool {
    using Task = std::function<void(std::stop_token)>;
    std::mutex mutex_;
    std::condition_variable_any wake_;
    std::deque<Task> pending_;
    std::vector<std::jthread> threads_;
    std::size_t limit_;
    bool closing_ = false;
public:
    explicit ThreadPool(std::size_t size) : limit_(size * 2) {
        if (!size) throw std::invalid_argument("thread pool cannot be empty");
        for (std::size_t i = 0; i < size; ++i) {
            threads_.emplace_back([this](std::stop_token stop) {
                while (!stop.stop_requested()) {
                    Task task;
                    {
                        std::unique_lock lock(mutex_);
                        wake_.wait(lock, stop, [this] { return closing_ || !pending_.empty(); });
                        if (closing_ || stop.stop_requested()) return;
                        task = std::move(pending_.front());
                        pending_.pop_front();
                    }
                    task(stop);
                }
            });
        }
    }
    bool submit(Task task) {
        std::lock_guard lock(mutex_);
        if (closing_ || pending_.size() >= limit_) return false;
        pending_.push_back(std::move(task));
        wake_.notify_one();
        return true;
    }
    void shutdown() {
        {
            std::lock_guard lock(mutex_);
            closing_ = true;
            pending_.clear();
        }
        for (auto& thread : threads_) thread.request_stop();
        wake_.notify_all();
        for (auto& thread : threads_) if (thread.joinable()) thread.join();
    }
    ~ThreadPool() { shutdown(); }
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
};
}
