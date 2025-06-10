#include "micro_benchmark/lambda/lambda_benchmark_result.hpp"

#include <chrono>
#include <future>
#include <vector>

#include <aws/core/utils/base64/Base64.h>
#include <gtest/gtest.h>

namespace skyrise {

TEST(LambdaBenchmarkResultTest, InvocationResultAndLogResult) {
  LambdaInvocationResult invocation_result_0("0");

  EXPECT_FALSE(invocation_result_0.IsComplete());
  EXPECT_FALSE(invocation_result_0.IsSuccess());
  EXPECT_FALSE(invocation_result_0.HasResultLog());
  EXPECT_EQ(invocation_result_0.GetInstanceId(), "0");

  invocation_result_0.Complete(nullptr);

  EXPECT_TRUE(invocation_result_0.IsComplete());
  EXPECT_FALSE(invocation_result_0.IsSuccess());
  EXPECT_FALSE(invocation_result_0.HasResultLog());
  EXPECT_NE(invocation_result_0.GetStartPoint(), invocation_result_0.GetEndPoint());

  LambdaInvocationResult invocation_result_1("1");
  EXPECT_EQ(invocation_result_1.GetInstanceId(), "1");

  // Prepare InvokeOutcome for invocation_result_1
  const auto payload_value = Aws::Utils::Json::JsonValue().WithBool("success", true);

  // We have to use a raw pointer here as the LambdaInvocationResult's ResponseStream will take ownership of this stream
  auto* result_body = new Aws::StringStream;
  *result_body << payload_value.View().WriteCompact();

  const Aws::String log_result_decoded =
      "REPORT RequestId: dc5e1ec9-c123-46ce-b72c-6ae7e629eb5a Duration: 238.83 ms Billed Duration: 261 ms Memory "
      "Size: 3008 MB Max Memory Used: 44 MB Init Duration: 22.09 ms";
  Aws::Utils::ByteBuffer log_result_buffer(log_result_decoded.size());

  for (size_t i = 0; i < log_result_decoded.size(); ++i) {
    log_result_buffer[i] = log_result_decoded[i];
  }

  const auto log_result_encoded = Aws::Utils::Base64::Base64().Encode(log_result_buffer);

  Aws::Lambda::Model::InvokeResult aws_invocation_result;
  aws_invocation_result.ReplaceBody(result_body);
  aws_invocation_result.SetLogResult(log_result_encoded);

  auto outcome =
      Aws::Utils::Outcome<Aws::Lambda::Model::InvokeResult, Aws::Lambda::LambdaError>(std::move(aws_invocation_result));

  // Complete invocation_result_1 with successful outcome
  invocation_result_1.Complete(&outcome);

  EXPECT_TRUE(invocation_result_1.IsComplete());
  EXPECT_TRUE(invocation_result_1.IsSuccess());
  EXPECT_TRUE(invocation_result_1.HasResultLog());

  EXPECT_TRUE(invocation_result_1.GetResponseBody().KeyExists("success"));
  EXPECT_TRUE(invocation_result_1.GetResponseBody().GetBool("success"));

  const auto log_result = invocation_result_1.GetResultLog();
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

  invocation_result_1.UpdateSQSMessageBody(sqs_message_body_value.View().WriteCompact());

  EXPECT_FALSE(invocation_result_1.GetResponseBody().KeyExists("success"));
  EXPECT_TRUE(invocation_result_1.GetResponseBody().KeyExists("sqs_message"));
  EXPECT_EQ(invocation_result_1.GetResponseBody().GetString("sqs_message"), "hello");
}

TEST(LambdaBenchmarkResultTest, ConcurrencyStressTest) {
  static constexpr size_t kConcurrentInstanceCount = 1000;
  static constexpr size_t kRepetitionCount = 10;

  const auto benchmark_start = std::chrono::steady_clock::now();

  LambdaBenchmarkResult result(kConcurrentInstanceCount, kRepetitionCount, nullptr);

  for (size_t i = 0; i < kRepetitionCount; ++i) {
    const auto repetition_start = std::chrono::steady_clock::now();
    EXPECT_FALSE(result.HasRepetitionFinished(i));

    std::vector<std::future<void>> registration_futures;
    registration_futures.reserve(kConcurrentInstanceCount);

    for (size_t j = 0; j < kConcurrentInstanceCount; ++j) {
      registration_futures.emplace_back(std::async(
          [&](const size_t repetition, const size_t invocation) {
            result.RegisterInvocation(repetition, invocation, std::to_string(invocation));
            result.CompleteInvocation(repetition, invocation, nullptr);
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

    EXPECT_GT(result.GetRepetitionResults()[i].GetDurationMs(), 0.0);
    EXPECT_LE(result.GetRepetitionResults()[i].GetDurationMs(), max_repetition_duration);
  }
  result.SetEndTime();
  ASSERT_TRUE(result.IsResultComplete());
  const auto benchmark_end = std::chrono::steady_clock::now();
  const double max_benchmark_duration =
      std::chrono::duration<double, std::milli>(benchmark_end - benchmark_start).count();
  EXPECT_GT(result.GetDurationMs(), 0.0);
  EXPECT_LE(result.GetDurationMs(), max_benchmark_duration);

  const auto& benchmark_repetitions = result.GetRepetitionResults();

  EXPECT_EQ(benchmark_repetitions.size(), kRepetitionCount);

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    EXPECT_EQ(benchmark_repetition.GetInvocationResults().size(), kConcurrentInstanceCount);
  }

  for (size_t i = 0; i < 10; ++i) {
    EXPECT_NO_THROW(benchmark_repetitions.at(i).GetWarmupCost());
  }

  const auto sqs_message_body =
      Aws::Utils::Json::JsonValue().WithObject("responsePayload", Aws::Utils::Json::JsonValue().AsString("abc"));

  result.UpdateSQSMessageBody(3, 2, sqs_message_body.View().WriteCompact());
  EXPECT_EQ(benchmark_repetitions[3].GetInvocationResults()[2].GetResponseBody().AsString(), "abc");
}

TEST(LambdaBenchmarkResultTest, FunctionWarmingCost) {
  static constexpr size_t kRepetitionCount = 10;
  static constexpr size_t kInvocationCount = 10;
  LambdaBenchmarkResult result(kRepetitionCount, kInvocationCount, nullptr);

  for (size_t i = 0; i < kRepetitionCount; ++i) {
    for (size_t j = 0; j < kInvocationCount; ++j) {
      result.RegisterInvocation(i, j, std::to_string(j));
      result.CompleteInvocation(i, j, nullptr);
    }
    result.SetRepetitionWarmupCost(i, 1);
  }

  for (const auto& benchmark_repetition : result.GetRepetitionResults()) {
    EXPECT_EQ(benchmark_repetition.GetWarmupCost(), 1.0L);
  }

  EXPECT_EQ(result.CalculateWarmupCost(), 10.0L);
}

}  // namespace skyrise
