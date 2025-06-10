#include "test_storage.hpp"

#include <sys/stat.h>
#include <unistd.h>

#include "utils/assert.hpp"
#include "utils/self_name.hpp"

namespace skyrise {

namespace {

bool DirectoryExists(const std::string& path) {
  struct stat info{};
  const int stat_result = stat(path.c_str(), &info);
  if (stat_result == -1) {
    return false;
  }
  return S_ISDIR(info.st_mode);  // NOLINT
}

std::string FindTestdataDirectory() {
  // Look in every parent directory. Given /path/to/executable we will look into /path/to/resources, /path/resources and
  // /resources.
  std::string full_path_string = GetAbsolutePathOfSelf();
  size_t offset = full_path_string.find_last_of('/');
  while (offset != std::string::npos) {
    full_path_string = full_path_string.substr(0, offset + 1);  // Keep trailing '/'.
    full_path_string.append("resources");

    if (DirectoryExists(full_path_string)) {
      full_path_string.append("/test");
      Assert(DirectoryExists(full_path_string), "Directory resources/test is missing.");
      return full_path_string;
    }

    full_path_string = full_path_string.substr(0, offset);  // Now remove trailing '/'.
    offset = full_path_string.find_last_of('/');
  }

  Fail("Did not find resources directory.");
}

}  // namespace

TestStorage::TestStorage() : FilesystemStorage(FindTestdataDirectory()) {}

}  // namespace skyrise
