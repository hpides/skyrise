#pragma once

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>

namespace skyrise {

// ConcurrentQueue is a thread-safe FIFO data structure. It provides Push() and Pop() operations. A size limit for the
// queue can be specified in the constructor. This is a rather naive implementation that uses locks for every operation.
template <typename T>
// NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
class ConcurrentQueue {
 public:
  // Returns the number of elements inside the queue.
  size_t Size() const {
    std::lock_guard<std::mutex> guard(queue_mutex_);

    return queue_.size();
  }

  // Adds a value to the queue. If the queue is full, this call blocks until space is available again or the queue gets
  // closed. If the queue has been closed, this call will do nothing and return `false`. It returns `true` otherwise.
  bool Push(const T& value) {
    std::unique_lock<std::mutex> guard = AssureCanWrite();

    if (closed_) {
      return false;
    }

    queue_.push(value);
    guard.unlock();
    queue_can_read_.notify_one();
  }

  // Adds a value to the queue. If the queue is full, this call blocks until space is available again or the queue gets
  // closed. If the queue has been closed, this call will do nothing and return `false`. It returns `true` otherwise.
  bool Push(T&& value) {
    std::unique_lock<std::mutex> guard = AssureCanWrite();

    if (closed_) {
      return false;
    }

    queue_.push(std::move(value));
    guard.unlock();
    queue_can_read_.notify_one();
    return true;
  }

  // Moves the front value of the queue to `result` and returns `true`. If the queue is empty, this call blocks until a
  // value is available. If the queue is closed this call does not block, and it returns `false`.
  bool Pop(T* result) {
    std::unique_lock<std::mutex> guard = AssureCanRead();

    // Having no value available means that the queue has been closed.
    if (queue_.empty()) {
      return false;
    }

    *result = std::move(queue_.front());
    queue_.pop();

    guard.unlock();
    queue_can_write_.notify_one();
    return true;
  }

  bool TryPop(T* result) {
    std::unique_lock<std::mutex> guard(queue_mutex_);

    // Having no value available means that the queue has been closed.
    if (queue_.empty() || closed_) {
      return false;
    }

    *result = std::move(queue_.front());
    queue_.pop();

    guard.unlock();
    queue_can_write_.notify_one();
    return true;
  }

  // Closes the queue. Values that are already in the queue can still be received. No new values will
  // be inserted in the queue.
  void Close() {
    std::unique_lock<std::mutex> guard(queue_mutex_);

    closed_ = true;
    guard.unlock();
    queue_can_write_.notify_all();
    queue_can_read_.notify_all();
  }

 private:
  std::unique_lock<std::mutex> AssureCanRead() {
    std::unique_lock<std::mutex> guard(queue_mutex_);

    if (!queue_.empty() || closed_) {
      return guard;
    }

    queue_can_read_.wait(guard, [&] { return !queue_.empty() || closed_; });
    return guard;
  }

  std::unique_lock<std::mutex> AssureCanWrite() {
    std::unique_lock<std::mutex> guard(queue_mutex_);

    return guard;
  }

  std::queue<T> queue_;
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_can_read_;
  std::condition_variable queue_can_write_;
  bool closed_ = false;
};

}  // namespace skyrise
