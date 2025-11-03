#include "self_name.hpp"

#include <array>
#if defined(__linux__)
#include <linux/limits.h>
#elif defined(__APPLE__)
#include <libproc.h>
#include <sys/syslimits.h>
#endif
#include <unistd.h>

#include "utils/assert.hpp"

namespace skyrise {

std::string GetAbsolutePathOfSelf() {
#if defined(__linux__)
  // On Linux there is '/proc/self/exe', which points to the current executable.

  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
  std::array<char, PATH_MAX + 1> executable_path_buffer;
  const auto path_name_length =
      readlink("/proc/self/exe", executable_path_buffer.data(), sizeof(executable_path_buffer) - 1);

  if (path_name_length == -1) {
    Fail("Unable to read project directory path.");
  }

  executable_path_buffer[path_name_length] = '\0';

#elif defined(__APPLE__)
  // On macOS there is 'proc_pidpath' to obtain the executable name.

  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
  std::array<char, PROC_PIDPATHINFO_MAXSIZE + 1> executable_path_buffer;
  const pid_t current_pid = getpid();
  const int returncode = proc_pidpath(current_pid, executable_path_buffer.data(), executable_path_buffer.size() - 1);
  if (returncode <= 0) {
    Fail("Unable to read project directory path.");
  }
#else
#error "No implementation for GetAbsolutePathOfSelf() found for this platform."
#endif

  return executable_path_buffer.data();
}

}  // namespace skyrise
