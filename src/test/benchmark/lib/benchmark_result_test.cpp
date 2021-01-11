#include "benchmark_result.hpp"

#include <chrono>
#include <future>
#include <vector>

#include "gtest/gtest.h"

namespace skyrise {

class BenchmarkResultTest : public ::testing::Test {};

TEST_F(BenchmarkResultTest, ConcurrencyStressTest) {
  const auto begin = std::chrono::steady_clock::now();

  BenchmarkResult result(10, 1000);

  for (size_t i = 0; i < 10; i++) {
    std::vector<std::future<void>> registration_futures;

    for (size_t j = 0; j < 1000; j++) {
      registration_futures.emplace_back(std::async(
          [&](const size_t repetition, const size_t invocation_id) {
            result.RegisterInvocation(repetition, std::to_string(invocation_id));
            result.FinishInvocation(repetition, std::to_string(invocation_id), nullptr, true);
          },
          i, j));
    }

    for (const auto& registration_future : registration_futures) {
      registration_future.wait();
    }
  }

  const auto end = std::chrono::steady_clock::now();

  EXPECT_LT(result.GetBenchmarkDuration().count(), std::chrono::duration<double>(end - begin).count());

  const auto& invocation_results = result.GetInvocationResults();

  EXPECT_EQ(invocation_results.size(), 10);

  for (const auto& repetition : invocation_results) {
    EXPECT_EQ(repetition.size(), 1000);
  }

  for (size_t i = 0; i < 10; i++) {
    EXPECT_NO_THROW(result.GetRepetitionDuration(i));
  }

  result.UpdateSQSMessageBody(3, "541", "abc");
  EXPECT_EQ(invocation_results[3].find("541")->second.sqs_message_body_, "abc");
}

}  // namespace skyrise
