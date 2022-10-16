#include "errors.hpp"

namespace skyrise {

const StorageError& ConcurrentErrorState::GetError() const { return error_; }

void ConcurrentErrorState::SetError(const StorageError& error) {
  if (HasError() || !error.IsError()) {
    return;
  }

  const std::lock_guard guard(error_mutex_);
  if (error_.IsError()) {
    return;
  }

  error_ = error;
  has_error_.store(true);
}

}  // namespace skyrise
