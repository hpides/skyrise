#include "benchmark_helper.hpp"

#include <chrono>
#include <cmath>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/lambda/model/InvokeRequest.h>

#include "benchmark_config.hpp"
#include "benchmark_result.hpp"
#include "gtest/gtest.h"

namespace skyrise {

class BenchmarkHelperTest : public ::testing::Test {};

TEST_F(BenchmarkHelperTest, GenerateJsonOutput) {
  Aws::SDKOptions options;

  Aws::InitAPI(options);
  {
    const auto benchmark_result = std::make_shared<BenchmarkResult>(1, 3);

    for (size_t i = 0; i < 3; i++) {
      benchmark_result->RegisterInvocation(0, std::to_string(i));
      benchmark_result->FinishInvocation(0, std::to_string(i), nullptr, true);
      benchmark_result->UpdateSQSMessageBody(0, std::to_string(i), std::to_string(i));
    }

    std::vector<std::tuple<Aws::String, double>> aggregated_numeric_metrics;
    aggregated_numeric_metrics.emplace_back(std::make_tuple("numeric_metric", 0.0));

    std::vector<std::tuple<Aws::String, Aws::String>> aggregated_alphabetic_metrics;
    aggregated_alphabetic_metrics.emplace_back(std::make_tuple("alphabetic_metric", "zero"));

    std::vector<std::function<std::tuple<Aws::String, double>(const InvocationResult&)>>
        extract_numeric_metric_functions{[](const InvocationResult& b) {
                                           return std::make_tuple("numeric_metric_1", std::stod(b.sqs_message_body_));
                                         },
                                         [](const InvocationResult& b) {
                                           return std::make_tuple("numeric_metric_2", static_cast<double>(b.success_));
                                         }};

    std::vector<std::function<std::tuple<Aws::String, Aws::String>(const InvocationResult&)>>
        extract_alphabetic_metric_functions{
            [](const InvocationResult& b) { return std::make_tuple("alphabetic_metric_1", b.sqs_message_body_); },
            [](const InvocationResult& b) {
              return std::make_tuple("alphabetic_metric_2", b.success_ ? "true" : "false");
            }};

    const auto json_value = BenchmarkHelper::GenerateJsonOutput(
        "benchmark", aggregated_numeric_metrics, aggregated_alphabetic_metrics, benchmark_result,
        extract_numeric_metric_functions, extract_alphabetic_metric_functions);
    const auto json_view = json_value.View();

    EXPECT_TRUE(json_view.ValueExists("name"));
    EXPECT_TRUE(json_view.ValueExists("numeric_metric"));
    EXPECT_TRUE(json_view.ValueExists("alphabetic_metric"));
    EXPECT_TRUE(json_view.ValueExists("repetitions"));

    const auto repetitions = json_view.GetArray("repetitions");
    EXPECT_EQ(repetitions.GetLength(), 1);

    EXPECT_TRUE(repetitions[0].ValueExists("repetition"));
    EXPECT_TRUE(repetitions[0].ValueExists("duration_seconds"));
    EXPECT_TRUE(repetitions[0].ValueExists("invocations"));

    const auto invocations = repetitions[0].GetArray("invocations");
    EXPECT_EQ(invocations.GetLength(), 3);

    for (size_t i = 0; i < invocations.GetLength(); i++) {
      EXPECT_TRUE(invocations[i].ValueExists("name"));
      EXPECT_TRUE(invocations[i].ValueExists("numeric_metric_1"));
      EXPECT_TRUE(invocations[i].ValueExists("numeric_metric_2"));
      EXPECT_TRUE(invocations[i].ValueExists("alphabetic_metric_1"));
      EXPECT_TRUE(invocations[i].ValueExists("alphabetic_metric_2"));
    }
  }
  Aws::ShutdownAPI(options);
}

}  // namespace skyrise
