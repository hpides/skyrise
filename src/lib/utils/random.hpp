#include <algorithm>
#include <array>
#include <functional>
#include <random>

namespace skyrise {

// Create a properly seeded random generator of type T (e.g., std::mt19937)
template <typename T>
T RandomGenerator(uint32_t seed = 0) {
  const size_t seeds_bytes = sizeof(typename T::result_type) * T::state_size;
  const size_t seeds_length = seeds_bytes / sizeof(std::seed_seq::result_type);

  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
  std::array<std::seed_seq::result_type, seeds_length> seeds;
  if (seed != 0) {
    std::generate(seeds.begin(), seeds.end(), [&]() {
      // NOLINTNEXTLINE(hicpp-signed-bitwise)
      seed = (seed << 1) | (seed >> (-1 & 31));
      return seed;
    });
  } else {
    std::random_device random_device;
    std::generate(seeds.begin(), seeds.end(), std::ref(random_device));
  }

  std::seed_seq seed_sequence(seeds.begin(), seeds.end());

  return T{seed_sequence};
}

}  // namespace skyrise
