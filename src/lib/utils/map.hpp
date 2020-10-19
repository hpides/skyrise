#pragma once

#include <map>
#include <vector>

namespace skyrise {

// Extract the keys of an std::map<K, V>
template <typename K, typename V>
std::vector<K> ExtractMapKeys(const std::map<K, V>& map) {
  std::vector<K> keys;
  keys.reserve(map.size());

  for (const auto& entry : map) {
    keys.emplace_back(entry.first);
  }

  return keys;
}

// Extract the values of an std::map<K, V>
template <typename K, typename V>
std::vector<V> ExtractMapValues(const std::map<K, V>& map) {
  std::vector<V> values;
  values.reserve(map.size());

  for (const auto& entry : map) {
    values.emplace_back(entry.second);
  }

  return values;
}

}  // namespace skyrise
