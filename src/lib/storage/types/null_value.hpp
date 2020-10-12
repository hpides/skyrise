/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <iostream>

namespace skyrise {

// Represents SQL null value in AllTypeVariant
struct NullValue {};

// Relational operators
inline bool operator==(const NullValue&, const NullValue&) { return false; }
inline bool operator!=(const NullValue&, const NullValue&) { return false; }
inline bool operator<(const NullValue&, const NullValue&) { return false; }
inline bool operator<=(const NullValue&, const NullValue&) { return false; }
inline bool operator>(const NullValue&, const NullValue&) { return false; }
inline bool operator>=(const NullValue&, const NullValue&) { return false; }
inline NullValue operator-(const NullValue&) { return NullValue{}; }

inline size_t HashValue(const NullValue&) {
  // Aggregate wants all NULLs in one bucket
  return 0;
}

inline std::ostream& operator<<(std::ostream& stream, const NullValue) { return stream << "NULL"; }

}  // namespace skyrise
