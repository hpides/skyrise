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

/**
 * Splits the given vector @param elements into smaller vectors with a maximum size @param chunk_size.
 */
template <typename T>
std::vector<std::vector<T>> SplitVectorIntoChunks(const std::vector<T>& elements, size_t chunk_size) {
  std::vector<std::vector<T>> chunks;
  size_t element_count = elements.size();
  chunks.reserve(element_count / chunk_size + 1);
  for (size_t i = 0; i < element_count; i += chunk_size) {
    const auto j = std::min(element_count, i + chunk_size);
    chunks.emplace_back(elements.cbegin() + i, elements.cbegin() + j);
  }
  return chunks;
}

}  // namespace skyrise
