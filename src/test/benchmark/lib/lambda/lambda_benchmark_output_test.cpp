#include "lambda/lambda_benchmark_output.hpp"

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

#include "lambda/lambda_benchmark.hpp"
#include "lambda/lambda_benchmark_config.hpp"
#include "lambda/lambda_benchmark_result.hpp"
#include "testing/aws_test.hpp"

namespace skyrise {

class AwsBenchmarkOutputTest : public ::testing::Test {
 private:
  const AwsApi aws_api_;
};

TEST_F(AwsBenchmarkOutputTest, Build) {
  const auto benchmark_result = std::make_shared<LambdaBenchmarkResult>(1, 3);

  for (size_t i = 0; i < 3; ++i) {
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

  const auto benchmark_output =
      LambdaBenchmarkOutput("test_benchmark", benchmark_result)
          .WithBoolArgument("bool_argument", true)
          .WithDoubleArgument("double_argument", 1.0)
          .WithInt64Argument("int64_argument", 1)
          .WithStringArgument("string_argument", "Yes")
          .WithBoolMetric("bool_metric", false)
          .WithDoubleMetric("double_metric", 2.0)
          .WithInt64Metric("int64_metric", 2)
          .WithStringMetric("string_metric", "No")
          .WithBoolInvocationMetric([](const LambdaInvokeResult& b) {
            return std::make_tuple("bool_invocation_metric", static_cast<bool>(b.IsSuccess()));
          })
          .WithDoubleInvocationMetric([](const LambdaInvokeResult& b) {
            return std::make_tuple("double_invocation_metric", static_cast<double>(b.IsSuccess()));
          })
          .WithInt64InvocationMetric([](const LambdaInvokeResult& b) {
            return std::make_tuple("int64_invocation_metric", static_cast<long long>(b.IsSuccess()));
          })
          .WithStringInvocationMetric([](const LambdaInvokeResult& b) {
            return std::make_tuple("string_invocation_metric", b.IsSuccess() ? "Yes" : "No");
          })
          .WithObjectInvocationMetric([](const LambdaInvokeResult& b) {
            return std::make_tuple("object_invocation_metric", Aws::Utils::Json::JsonValue().AsBool(b.IsSuccess()));
          })
          .Build();

  const auto json_view = benchmark_output.View();

  EXPECT_TRUE(json_view.ValueExists("name"));

  EXPECT_TRUE(json_view.ValueExists("arguments"));
  const auto arguments = json_view.GetObject("arguments");

  EXPECT_TRUE(arguments.ValueExists("bool_argument"));
  EXPECT_TRUE(arguments.ValueExists("double_argument"));
  EXPECT_TRUE(arguments.ValueExists("int64_argument"));
  EXPECT_TRUE(arguments.ValueExists("string_argument"));

  EXPECT_TRUE(json_view.ValueExists("metrics"));
  const auto metrics = json_view.GetObject("metrics");

  EXPECT_TRUE(metrics.ValueExists("bool_metric"));
  EXPECT_TRUE(metrics.ValueExists("double_metric"));
  EXPECT_TRUE(metrics.ValueExists("int64_metric"));
  EXPECT_TRUE(metrics.ValueExists("string_metric"));

  EXPECT_TRUE(json_view.ValueExists("repetitions"));
  const auto repetitions = json_view.GetArray("repetitions");
  EXPECT_EQ(repetitions.GetLength(), 1);

  EXPECT_TRUE(repetitions[0].ValueExists("repetition"));
  EXPECT_TRUE(repetitions[0].ValueExists("duration_ms"));
  EXPECT_TRUE(repetitions[0].ValueExists("invocations"));

  const auto invocations = repetitions[0].GetArray("invocations");
  EXPECT_EQ(invocations.GetLength(), 3);

  for (size_t i = 0; i < invocations.GetLength(); ++i) {
    EXPECT_TRUE(invocations[i].ValueExists("name"));
    EXPECT_TRUE(invocations[i].ValueExists("bool_invocation_metric"));
    EXPECT_TRUE(invocations[i].ValueExists("double_invocation_metric"));
    EXPECT_TRUE(invocations[i].ValueExists("int64_invocation_metric"));
    EXPECT_TRUE(invocations[i].ValueExists("string_invocation_metric"));
    EXPECT_TRUE(invocations[i].ValueExists("object_invocation_metric"));
  }
}

}  // namespace skyrise
