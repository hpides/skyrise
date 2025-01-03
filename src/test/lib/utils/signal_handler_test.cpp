#include "utils/signal_handler.hpp"

#include <csignal>

#include <gtest/gtest.h>

namespace skyrise {

class SignalHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const std::string run_signal_handler_tests = RUN_SIGNAL_HANDLER_TESTS;
    if (run_signal_handler_tests == "OFF") {
      GTEST_SKIP() << "The signal handler tests are disabled. To execute them, reconfigure project with "
                      "'-DSKYRISE_ENABLE_SIGNAL_HANDLER_TESTS=ON'";
    }
  }

  static constexpr std::array<int, 6> kTestSignalNumbers{SIGTERM, SIGSEGV, SIGINT, SIGILL, SIGABRT, SIGFPE};
};

TEST_F(SignalHandlerTest, RaiseSignalNumbers) {
  RegisterSignalHandler();
  for (const auto signal_number : kTestSignalNumbers) {
    std::raise(signal_number);
    EXPECT_TRUE(true);
  }
  DeregisterSignalHandler();
}
}  // namespace skyrise
