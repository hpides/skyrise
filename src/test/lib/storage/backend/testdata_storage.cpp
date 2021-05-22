#include "testdata_storage.hpp"

#include <linux/limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "utils/assert.hpp"

namespace skyrise {
namespace {
bool DirectoryExists(const std::string& path) {
  struct stat info {};
  int stat_result = stat(path.c_str(), &info);
  if (stat_result == -1) {
    return false;
  }
  return S_ISDIR(info.st_mode);  // NOLINT
}

std::string FindTestdataDirectory() {
  std::array<char, PATH_MAX + 1> full_path{0};
  int exe_path_length = readlink("/proc/self/exe", full_path.data(), PATH_MAX);
  Assert(exe_path_length >= 1, "readlink() returned an error.");

  // Look in every parent directory. Given /path/to/executable we will look into /path/to/testdata, /path/testdata and
  // /testdata.
  std::string full_path_string(full_path.data());
  size_t offset = full_path_string.find_last_of('/');
  while (offset != std::string::npos) {
    full_path_string = full_path_string.substr(0, offset + 1);  // Keep trailing '/'.
    full_path_string.append("testdata");

    if (DirectoryExists(full_path_string)) {
      return full_path_string;
    }

    full_path_string = full_path_string.substr(0, offset);  // Now remove trailing '/'.
    offset = full_path_string.find_last_of('/');
  }

  Fail("Testdata could not be found.");
}
}  // namespace

TestdataStorage::TestdataStorage() : FilesystemStorage(FindTestdataDirectory()) {}

}  // namespace skyrise
