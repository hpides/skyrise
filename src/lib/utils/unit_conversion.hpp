#pragma once

namespace skyrise {

inline unsigned long long KBToByte(unsigned long long n) { return n * 1'000; }

inline unsigned long long MBToByte(unsigned long long n) { return n * 1'000 * 1'000; }

inline unsigned long long GBToByte(unsigned long long n) { return n * 1'000 * 1'000 * 1'000; }

inline unsigned long long KiBToByte(unsigned long long n) { return n * 1'024; }

inline unsigned long long MiBToByte(unsigned long long n) { return n * 1'024 * 1'024; }

inline unsigned long long GiBToByte(unsigned long long n) { return n * 1'024 * 1'024 * 1'024; }

inline unsigned long long GiBToMiB(unsigned long long n) { return n * 1'024; }

inline long double ByteToKB(unsigned long long n) { return n / 1'000.0L; }

inline long double ByteToMB(unsigned long long n) { return n / 1'000.0L / 1'000.0L; }

inline long double ByteToGB(unsigned long long n) { return n / 1'000.0L / 1'000.0L / 1'000.0L; }

inline long double KBToMB(unsigned long long n) { return n / 1'000.0L; }

inline long double KBToGB(unsigned long long n) { return n / 1'000.0L / 1'000.0L; }

inline long double MBToGB(unsigned long long n) { return n / 1'000.0L; }

inline long double ByteToKiB(unsigned long long n) { return n / 1'024.0L; }

inline long double ByteToMiB(unsigned long long n) { return n / 1'024.0L / 1'024.0L; }

inline long double ByteToGiB(unsigned long long n) { return n / 1'024.0L / 1'024.0L / 1'024.0L; }

inline long double MiBToMB(unsigned long long n) { return n / 1'000.0L / 1'000.0L * 1'024 * 1'024; }

inline unsigned long long ByteToBit(unsigned long long n) { return n * 8.0L; }

inline unsigned long long MBToBit(unsigned long long n) { return n * 8.0L * 1'000 * 1'000; }

inline long double BitToByte(unsigned long long n) { return n / 8.0L; }

inline long double BitToMB(unsigned long long n) { return n / 8.0L / 1'000.0L / 1'000.0L; }

}  // namespace skyrise
