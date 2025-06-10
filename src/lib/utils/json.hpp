#pragma once

#include <type_traits>
#include <vector>

#include <aws/core/utils/json/JsonSerializer.h>

#include "utils/assert.hpp"

namespace skyrise {

template <typename T>
Aws::Utils::Array<Aws::Utils::Json::JsonValue> VectorToJsonArray(const std::vector<T>& vector) {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> result(vector.size());
  for (size_t i = 0; i < vector.size(); ++i) {
    Aws::Utils::Json::JsonValue value;

    if constexpr (std::is_same_v<T, std::string>) {
      value.AsString(vector[i]);
    } else if constexpr (std::is_integral_v<T>) {
      value.AsInt64(static_cast<int64_t>(vector[i]));
    } else {
      value = vector[i].ToJson();
    }

    result[i] = std::move(value);
  }
  return result;
}

template <typename T>
std::vector<T> JsonArrayToVector(const Aws::Utils::Array<Aws::Utils::Json::JsonView>& array) {
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  std::vector<T> result;
  result.reserve(array.GetLength());
  for (size_t i = 0; i < array.GetLength(); ++i) {
    // NOLINTNEXTLINE(bugprone-branch-clone)
    if constexpr (std::is_same_v<T, std::string>) {
      result.emplace_back(array[i].AsString());
    } else if constexpr (std::is_integral_v<T>) {
      result.emplace_back(static_cast<T>(array[i].AsInt64()));
    } else {
      result.emplace_back(T::FromJson(array[i]));
    }
  }

  return result;
}

}  // namespace skyrise
