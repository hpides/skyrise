/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */

#include "string.hpp"

#include <string>

#include "random.hpp"
#include "time.hpp"

namespace skyrise {

std::string TrimSourceFilePath(const std::string& file_path) {
  const auto src_position = file_path.find("/src/");

  return src_position == std::string::npos ? file_path : file_path.substr(src_position + 1);
}

std::vector<std::string> SplitStringByDelimiter(const std::string& string, const char delimiter) {
  std::stringstream stream(string);
  std::string token;
  std::vector<std::string> substrings;

  while (std::getline(stream, token, delimiter)) {
    substrings.emplace_back(token);
  }

  return substrings;
}

std::string RandomString(const size_t length, const std::string& character_set) {
  auto random_generator = RandomGenerator<std::mt19937>();

  std::uniform_int_distribution<size_t> uniform_distribution(0, character_set.size() - 1);

  std::string random_string(length, '0');
  std::generate(random_string.begin(), random_string.end(),
                [&]() { return character_set[uniform_distribution(random_generator)]; });

  return random_string;
}

std::string GetUniqueName(const std::string& name) {
  return GetFormattedTimestamp("%Y%m%dT%H%M%S") + "-" + name + "-" + RandomString(8);
}

size_t StringHeapSize(const std::string& string) {
  // Get the default pre-allocated capacity of SSO strings. Note that the empty string has an unspecified capacity, so
  // we use a really short one here.
  const size_t sso_string_capacity = std::string{"."}.capacity();

  if (string.capacity() > sso_string_capacity) {
    // For heap-allocated strings, \0 is appended to denote the end of the string. capacity() is used over length()
    // since some libraries (e.g., LLVM's libc++) also over-allocate the heap strings
    // (cf. https://shaharmike.com/cpp/std-string/).
    return string.capacity() + 1;
  }

  // Assert that SSO meets expectations
  assert(string.capacity() == sso_string_capacity);
  return 0;
}

size_t StringVectorMemoryUsage(const std::vector<std::string>& string_vector) {
  const size_t base_size = sizeof(std::vector<std::string>);

  // Early out
  if (string_vector.empty()) {
    return base_size + (string_vector.capacity() * sizeof(std::string));
  }

  // Run the (expensive) calculation of aggregating the whole vector's string sizes when full estimation is desired
  // or the given input vector is small.
  size_t elements_size = string_vector.capacity() * sizeof(std::string);
  for (const auto& single_string : string_vector) {
    elements_size += StringHeapSize(single_string);
  }
  return base_size + elements_size;
}

std::string ToLowerCase(const std::string& input_string) {
  std::string output_string;
  output_string.resize(input_string.size());
  std::transform(input_string.begin(), input_string.end(), output_string.begin(),
                 [](unsigned char input_string_character) { return std::tolower(input_string_character); });
  return output_string;
}

}  // namespace skyrise
