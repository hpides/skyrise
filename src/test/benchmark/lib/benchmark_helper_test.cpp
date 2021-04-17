#include "benchmark_helper.hpp"

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/base64/Base64.h>
#include <aws/core/utils/json/JsonSerializer.h>
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
      benchmark_result->RegisterInvocation(0, i, std::to_string(i));

      const auto payload_value = Aws::Utils::Json::JsonValue().WithBool("success", true);

      // We have to use a raw pointer here as the InvokeResult's ResponseStream will take ownership of this stream
      auto* result_body = new Aws::StringStream;
      *result_body << payload_value.View().WriteCompact();

      Aws::Lambda::Model::InvokeResult invoke_result;
      invoke_result.ReplaceBody(result_body);
      auto outcome =
          Aws::Utils::Outcome<Aws::Lambda::Model::InvokeResult, Aws::Lambda::LambdaError>(std::move(invoke_result));

      const auto sqs_message_body_value = Aws::Utils::Json::JsonValue().WithObject(
          "responsePayload", Aws::Utils::Json::JsonValue().WithBool("success", true));

      benchmark_result->FinishInvocation(0, i, &outcome);
      benchmark_result->UpdateSQSMessageBody(0, i, sqs_message_body_value.View().WriteCompact());
    }

    std::vector<std::tuple<Aws::String, double>> aggregated_numeric_metrics;
    aggregated_numeric_metrics.emplace_back(std::make_tuple("numeric_metric", 0.0));

    std::vector<std::tuple<Aws::String, Aws::String>> aggregated_alphabetic_metrics;
    aggregated_alphabetic_metrics.emplace_back(std::make_tuple("alphabetic_metric", "zero"));

    std::vector<std::function<std::tuple<Aws::String, double>(const InvokeResult&)>> extract_numeric_metric_functions{
        [](const InvokeResult& b) { return std::make_tuple("numeric_metric_1", b.GetDurationMs()); },
        [](const InvokeResult& b) { return std::make_tuple("numeric_metric_2", static_cast<double>(b.IsSuccess())); }};

    std::vector<std::function<std::tuple<Aws::String, Aws::String>(const InvokeResult&)>>
        extract_alphabetic_metric_functions{
            [](const InvokeResult& b) { return std::make_tuple("alphabetic_metric_1", b.GetInvokeId()); },
            [](const InvokeResult& b) {
              return std::make_tuple("alphabetic_metric_2", b.IsSuccess() ? "true" : "false");
            }};

    std::vector<std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const InvokeResult&)>>
        extract_object_metric_functions{
            [](const InvokeResult& b) {
              return std::make_tuple("object_metric_1", Aws::Utils::Json::JsonValue().AsString(b.GetInvokeId()));
            },
            [](const InvokeResult& b) {
              return std::make_tuple("object_metric_2", Aws::Utils::Json::JsonValue().AsBool(b.IsSuccess()));
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
    EXPECT_TRUE(repetitions[0].ValueExists("duration_ms"));
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

}  // namespace skyrise
