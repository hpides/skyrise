/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cstdint>
#include <limits>

namespace skyrise {

class Noncopyable {
 protected:
  Noncopyable() = default;
  Noncopyable(const Noncopyable&) = delete;
  Noncopyable(Noncopyable&&) noexcept = default;
  Noncopyable& operator=(Noncopyable&&) noexcept = default;
  const Noncopyable& operator=(const Noncopyable&) = delete;
  ~Noncopyable() = default;
};

using ColumnCount = uint32_t;
using ColumnId = uint32_t;
inline constexpr ColumnId kInvalidColumnId{std::numeric_limits<ColumnId>::max()};

}  // namespace skyrise
