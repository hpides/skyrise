#include "benchmark_helper.hpp"

#include <chrono>
#include <cmath>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/base64/Base64.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <gtest/gtest.h>

#include "benchmark_config.hpp"
#include "benchmark_result.hpp"

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

    std::vector<std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const InvocationResult&)>>
        extract_object_metric_functions{
            [](const InvocationResult& b) {
              return std::make_tuple("object_metric_1", Aws::Utils::Json::JsonValue().AsString(b.sqs_message_body_));
            },
            [](const InvocationResult& b) {
              return std::make_tuple("object_metric_2", Aws::Utils::Json::JsonValue().AsBool(b.success_));
            }};

    const auto json_value = BenchmarkHelper::GenerateJsonOutput(
        "benchmark", aggregated_numeric_metrics, aggregated_alphabetic_metrics, benchmark_result,
        extract_numeric_metric_functions, extract_alphabetic_metric_functions, extract_object_metric_functions);
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
      EXPECT_TRUE(invocations[i].ValueExists("object_metric_1"));
      EXPECT_TRUE(invocations[i].ValueExists("object_metric_2"));
    }
  }
  Aws::ShutdownAPI(options);
}

TEST_F(BenchmarkHelperTest, ExtractLogResultMetric) {
  Aws::SDKOptions options;

  Aws::InitAPI(options);
  {
    const Aws::String log_result =
        "REPORT RequestId: dc5e1ec9-c123-46ce-b72c-6ae7e629eb5a Duration: 238.83 ms Billed Duration: 261 ms Memory "
        "Size: 3008 MB Max Memory Used: 44 MB Init Duration: 22.09 ms";
    Aws::Utils::ByteBuffer log_result_buffer(log_result.size());

    for (size_t i = 0; i < log_result.size(); i++) {
      log_result_buffer[i] = log_result[i];
    }

    const auto log_result_encoded = Aws::Utils::Base64::Base64().Encode(log_result_buffer);

    auto invoke_result = Aws::Lambda::Model::InvokeResult();
    invoke_result.SetLogResult(log_result_encoded);
    invoke_result.SetStatusCode(200);

    const auto time_point = std::chrono::system_clock::now();

    const InvocationResult invocation_result{
        "0",        true,      true, {}, std::make_shared<Aws::Lambda::Model::InvokeResult>(std::move(invoke_result)),
        time_point, time_point};

    const auto init_duration = BenchmarkHelper::ExtractLogResultMetric(invocation_result, "Init Duration");
    EXPECT_EQ(init_duration.value(), 22.09);

    const auto duration = BenchmarkHelper::ExtractLogResultMetric(invocation_result, "Duration");
    EXPECT_EQ(duration.value(), 238.83);

    const auto billed_duration = BenchmarkHelper::ExtractLogResultMetric(invocation_result, "Billed Duration");
    EXPECT_EQ(billed_duration.value(), 261);

    const auto memory_mb_size = BenchmarkHelper::ExtractLogResultMetric(invocation_result, "Memory Size");
    EXPECT_EQ(memory_mb_size.value(), 3008);

    const auto max_memory_used = BenchmarkHelper::ExtractLogResultMetric(invocation_result, "Max Memory Used");
    EXPECT_EQ(max_memory_used.value(), 44);

    const auto xray_trace_id = BenchmarkHelper::ExtractLogResultMetric(invocation_result, "XRAY TraceId");
    EXPECT_FALSE(xray_trace_id.has_value());
  }
  Aws::ShutdownAPI(options);
}

}  // namespace skyrise
