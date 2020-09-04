#pragma once

namespace skyrise {

// Kilobyte
constexpr unsigned long long operator"" _KB(unsigned long long n) { return n * 1024; }

// Megabyte
constexpr unsigned long long operator"" _MB(unsigned long long n) { return n * 1024 * 1024; }

// Gigabyte
constexpr unsigned long long operator"" _GB(unsigned long long n) { return n * 1024 * 1024 * 1024; }

// Terabyte
constexpr unsigned long long operator"" _TB(unsigned long long n) { return n * 1024 * 1024 * 1024 * 1024; }

}  // namespace skyrise
