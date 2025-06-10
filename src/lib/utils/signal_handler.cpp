#include "signal_handler.hpp"

#include <array>
#include <csignal>
#include <iostream>

#include <boost/stacktrace.hpp>

namespace skyrise {

namespace {

constexpr std::array<int, 6> kSignalNumbers{SIGTERM, SIGSEGV, SIGINT, SIGILL, SIGABRT, SIGFPE};

void HandleSignal(int signal_number) {
  std::cerr << "Signal received: " << SignalToString(signal_number) << '\n';
#if SKYRISE_DEBUG
  std::cerr << boost::stacktrace::stacktrace(0, 15);
#endif
  std::cerr << std::flush;

#if SKYRISE_SIGNAL_HANDLER_EXIT
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  std::exit(signal_number);
#endif
}

}  // namespace

void RegisterSignalHandler() {
  const auto handler_function = [](int signal_number) { HandleSignal(signal_number); };

  for (const auto signal_number : kSignalNumbers) {
    std::signal(signal_number, handler_function);
  }
}

void DeregisterSignalHandler() {
  for (const auto signal_number : kSignalNumbers) {
    std::signal(signal_number, SIG_DFL);
  }
}

std::string SignalToString(int signal_number) {
  switch (signal_number) {
    case SIGTERM:
      return "SIGTERM";
    case SIGSEGV:
      return "SIGSEGV";
    case SIGINT:
      return "SIGINT";
    case SIGILL:
      return "SIGILL";
    case SIGABRT:
      return "SIGABRT";
    case SIGFPE:
      return "SIGFPE";
    default:
      return "UNKNOWN";
  }
}

}  // namespace skyrise
