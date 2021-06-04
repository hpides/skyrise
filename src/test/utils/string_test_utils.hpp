#pragma once

#include <string>

namespace skyrise {

/**
 * Some Description() implementations print memory addresses, which are non-deterministic. This function replaces them
 * with the following dummy address 0x00000000 (e.g., for testing).
 */
std::string RedactMemoryAddresses(const std::string& input);

}  // namespace skyrise
