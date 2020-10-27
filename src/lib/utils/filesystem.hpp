#include <string>

namespace skyrise {

// Read a file into an std::string; calls Fail upon encountering an error
std::string ReadFileToString(const std::string& filename);

// Write an std::string into a file; calls Fail upon encountering an error
void WriteStringToFile(const std::string& content, const std::string& filename);

}  // namespace skyrise
