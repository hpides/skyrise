#include <string>

namespace skyrise {
std::string Compress(const std::string& to_compress);

std::string Decompress(const std::string& to_decompress, size_t decompressed_size = 0);
}  // namespace skyrise
