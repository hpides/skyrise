#include "benchmark_result.hpp"

#include <chrono>
#include <future>
#include <vector>

#include <aws/core/utils/base64/Base64.h>
#include <gtest/gtest.h>

namespace skyrise {

class BenchmarkResultTest : public ::testing::Test {};

TEST_F(BenchmarkResultTest, InvokeResultAndLogResult) {
  InvokeResult invoke_result_0("0");

  EXPECT_FALSE(invoke_result_0.IsComplete());
  EXPECT_FALSE(invoke_result_0.IsSuccess());
  EXPECT_FALSE(invoke_result_0.HasLogResult());
  EXPECT_EQ(invoke_result_0.GetInvokeId(), "0");

  invoke_result_0.Complete(nullptr);

  EXPECT_TRUE(invoke_result_0.IsComplete());
  EXPECT_FALSE(invoke_result_0.IsSuccess());
  EXPECT_FALSE(invoke_result_0.HasLogResult());
  EXPECT_NE(invoke_result_0.GetStartPoint(), invoke_result_0.GetEndPoint());

  InvokeResult invoke_result_1("1");
  EXPECT_EQ(invoke_result_1.GetInvokeId(), "1");

  // Prepare InvokeOutcome for invoke_result_1
  const auto payload_value = Aws::Utils::Json::JsonValue().WithBool("success", true);

  // We have to use a raw pointer here as the InvokeResult's ResponseStream will take ownership of this stream
  auto* result_body = new Aws::StringStream;
  *result_body << payload_value.View().WriteCompact();

  const Aws::String log_result_decoded =
      "REPORT RequestId: dc5e1ec9-c123-46ce-b72c-6ae7e629eb5a Duration: 238.83 ms Billed Duration: 261 ms Memory "
      "Size: 3008 MB Max Memory Used: 44 MB Init Duration: 22.09 ms";
  Aws::Utils::ByteBuffer log_result_buffer(log_result_decoded.size());

  for (size_t i = 0; i < log_result_decoded.size(); i++) {
    log_result_buffer[i] = log_result_decoded[i];
  }

  const auto log_result_encoded = Aws::Utils::Base64::Base64().Encode(log_result_buffer);

  Aws::Lambda::Model::InvokeResult aws_invoke_result;
  aws_invoke_result.ReplaceBody(result_body);
  aws_invoke_result.SetLogResult(log_result_encoded);

  auto outcome =
      Aws::Utils::Outcome<Aws::Lambda::Model::InvokeResult, Aws::Lambda::LambdaError>(std::move(aws_invoke_result));

  // Complete invoke_result_1 with successful outcome
  invoke_result_1.Complete(&outcome);

  EXPECT_TRUE(invoke_result_1.IsComplete());
  EXPECT_TRUE(invoke_result_1.IsSuccess());
  EXPECT_TRUE(invoke_result_1.HasLogResult());

  EXPECT_TRUE(invoke_result_1.GetResponseBody().KeyExists("success"));
  EXPECT_TRUE(invoke_result_1.GetResponseBody().GetBool("success"));

  const auto log_result = invoke_result_1.GetLogResult();
  EXPECT_EQ(log_result->GetBilledDurationMs(), 261);
  EXPECT_EQ(log_result->GetDurationMs(), 238.83);
  EXPECT_EQ(log_result->GetInitDurationMs(), 22.09);
  EXPECT_EQ(log_result->GetMaxMemoryUsedMb(), 44);
  EXPECT_EQ(log_result->GetMemorySize(), 3008);
  EXPECT_EQ(log_result->GetRequestId(), "dc5e1ec9-c123-46ce-b72c-6ae7e629eb5a");

  EXPECT_TRUE(log_result->HasInitDuration());
  EXPECT_FALSE(log_result->HasXrayTraceId());
  EXPECT_ANY_THROW(log_result->GetXrayTraceId());

  const auto sqs_message_body_value = Aws::Utils::Json::JsonValue().WithObject(
      "responsePayload", Aws::Utils::Json::JsonValue().WithString("sqs_message", "hello"));

  invoke_result_1.UpdateSQSMessageBody(sqs_message_body_value.View().WriteCompact());

  EXPECT_FALSE(invoke_result_1.GetResponseBody().KeyExists("success"));
  EXPECT_TRUE(invoke_result_1.GetResponseBody().KeyExists("sqs_message"));
  EXPECT_EQ(invoke_result_1.GetResponseBody().GetString("sqs_message"), "hello");
}

TEST_F(BenchmarkResultTest, ConcurrencyStressTest) {
  const auto benchmark_start = std::chrono::steady_clock::now();

  const size_t repetition_count = 10;
  const size_t invocation_count = 1000;

  BenchmarkResult result(repetition_count, invocation_count);

  for (size_t i = 0; i < repetition_count; i++) {
    const auto repetition_start = std::chrono::steady_clock::now();
    EXPECT_FALSE(result.HasRepetitionFinished(i));

    std::vector<std::future<void>> registration_futures;

    for (size_t j = 0; j < invocation_count; j++) {
      registration_futures.emplace_back(std::async(
          [&](const size_t repetition, const size_t invocation_id) {
            result.RegisterInvocation(repetition, invocation_id, std::to_string(invocation_id));
            result.FinishInvocation(repetition, invocation_id, nullptr);
          },
          i, j));
    }

    for (const auto& registration_future : registration_futures) {
      registration_future.wait();
    }

    ASSERT_TRUE(result.HasRepetitionFinished(i));
    const auto repetition_end = std::chrono::steady_clock::now();
    const double max_repetition_duration =
        std::chrono::duration<double, std::milli>(repetition_end - repetition_start).count();

    EXPECT_GT(result.GetBenchmarkRepetitions()[i].GetDurationMs(), 0.0);
    EXPECT_LE(result.GetBenchmarkRepetitions()[i].GetDurationMs(), max_repetition_duration);
  }

  ASSERT_TRUE(result.IsComplete());
  const auto benchmark_end = std::chrono::steady_clock::now();
  const double max_benchmark_duration =
      std::chrono::duration<double, std::milli>(benchmark_end - benchmark_start).count();

  EXPECT_GT(result.GetDurationMs(), 0.0);
  EXPECT_LE(result.GetDurationMs(), max_benchmark_duration);

  const auto& benchmark_repetitions = result.GetBenchmarkRepetitions();

  EXPECT_EQ(benchmark_repetitions.size(), repetition_count);

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    EXPECT_EQ(benchmark_repetition.GetInvokeResults().size(), invocation_count);
  }

  for (size_t i = 0; i < 10; i++) {
    EXPECT_NO_THROW(benchmark_repetitions.at(i).GetWarmUpCost());
  }

  const auto sqs_message_body =
      Aws::Utils::Json::JsonValue().WithObject("responsePayload", Aws::Utils::Json::JsonValue().AsString("abc"));

  result.UpdateSQSMessageBody(3, 2, sqs_message_body.View().WriteCompact());
  EXPECT_EQ(benchmark_repetitions[3].GetInvokeResults()[2].GetResponseBody().AsString(), "abc");
}

TEST_F(BenchmarkResultTest, FunctionWarmingCost) {
  const size_t repetition_count = 10;
  const size_t invocation_count = 10;
  BenchmarkResult result(repetition_count, invocation_count);

  for (size_t i = 0; i < repetition_count; i++) {
    for (size_t j = 0; j < invocation_count; j++) {
      result.RegisterInvocation(i, j, std::to_string(j));
      result.FinishInvocation(i, j, nullptr);
    }
    result.SetFunctionWarmUpCost(i, 1);
  }

  for (const auto& benchmark_repetition : result.GetBenchmarkRepetitions()) {
    EXPECT_EQ(benchmark_repetition.GetWarmUpCost(), 1.0L);
  }

  EXPECT_EQ(result.GetWarmUpCost(), 10.0L);
}

}  // namespace skyrise
