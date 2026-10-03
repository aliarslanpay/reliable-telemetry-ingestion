#pragma once
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>

namespace telemetry {
template<class T> class Handoff {
    const std::size_t capacity_;
    std::mutex mutex_;
    std::deque<T> items_;
public:
    explicit Handoff(std::size_t capacity) : capacity_(capacity) {
        if (capacity==0 || capacity>1024) throw std::invalid_argument("handoff capacity");
    }
    bool push(T value) {
        std::lock_guard lock(mutex_);
        if (items_.size()==capacity_) return false;
        items_.push_back(std::move(value));
        return true;
    }
    std::optional<T> pop() {
        std::lock_guard lock(mutex_);
        if (items_.empty()) return std::nullopt;
        T value=std::move(items_.front()); items_.pop_front(); return value;
    }
};
} // namespace telemetry
