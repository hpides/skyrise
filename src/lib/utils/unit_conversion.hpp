#pragma once

namespace skyrise {

inline unsigned long long KbToByte(unsigned long long n) { return n * 1024; }

inline unsigned long long MbToByte(unsigned long long n) { return n * 1024 * 1024; }

inline unsigned long long GbToByte(unsigned long long n) { return n * 1024 * 1024 * 1024; }

inline unsigned long long TbToByte(unsigned long long n) { return n * 1024 * 1024 * 1024 * 1024; }

inline long double ByteToKb(unsigned long long n) { return n / 1024.0L; }

inline long double ByteToMb(unsigned long long n) { return n / 1024.0L / 1024.0L; }

inline long double ByteToGb(unsigned long long n) { return n / 1024.0L / 1024.0L / 1024.0L; }

inline long double ByteToTb(unsigned long long n) { return n / 1024.0L / 1024.0L / 1024.0L / 1024.0L; }

}  // namespace skyrise
