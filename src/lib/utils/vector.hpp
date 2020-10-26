#include <algorithm>
#include <vector>

namespace skyrise {

// Check whether vector a is a subset of vector b (i.e., if all elements of a are present in b as well)
template <typename T>
bool IsSubset(const std::vector<T>& a, const std::vector<T>& b) {
  for (const auto& element : a) {
    if (std::find(b.cbegin(), b.cend(), element) == b.cend()) {
      return false;
    }
  }
  return true;
}

}  // namespace skyrise
