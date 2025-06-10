#include "monitoring/lambda_segments_analyzer.hpp"

#include <aws/core/utils/crypto/CryptoBuf.h>
#include <aws/iam/model/GetRoleRequest.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <gtest/gtest.h>

#include "client/coordinator_client.hpp"
#include "function/function_utils.hpp"
#include "monitoring/monitoring_test_utils.hpp"
#include "testing/aws_test.hpp"
#include "utils/string.hpp"
#include "utils/time.hpp"

namespace {

const size_t kMemorySize = 128;
const bool kIsLocal = true;
const bool kEnableVpc = false;
const bool kEnableTracing = true;
const std::string kPackageName = "skyriseSimpleFunction";
const std::string kFunctionPath = skyrise::GetFunctionZipFilePath(kPackageName);
const std::string kFunctionName = skyrise::GetUniqueName(kPackageName);

}  // namespace

namespace skyrise {

class AwsLambdaSegmentsAnalyzerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    start_time_ = std::chrono::system_clock::now();

    client_ = std::make_shared<CoordinatorClient>();

    UploadFunctions(client_->GetIamClient(), client_->GetLambdaClient(),
                    std::vector<FunctionConfig>{{kFunctionPath, kFunctionName, kMemorySize, kIsLocal, kEnableVpc}},
                    kEnableTracing);
    const auto time_points = InvokeFunction(client_, kFunctionName);

    lambda_start_time_ = time_points.first;
    lambda_end_time_ = time_points.second;
  }

  void TearDown() override { DeleteFunction(client_, kFunctionName); }

  const AwsApi aws_api_;

  std::chrono::time_point<std::chrono::system_clock> lambda_start_time_;
  std::chrono::time_point<std::chrono::system_clock> lambda_end_time_;
  std::chrono::time_point<std::chrono::system_clock> start_time_;

  std::shared_ptr<CoordinatorClient> client_;
};

TEST_F(AwsLambdaSegmentsAnalyzerTest, DISABLED_GetCalculatedSegments) {
  const auto end_time = std::chrono::system_clock::now();

  LambdaSegmentAnalyzer analyzer(client_->GetXRayClient());
  const auto trace_ids = analyzer.GetTraceIds({kFunctionName}, start_time_, end_time);

  EXPECT_FALSE(trace_ids.at(kFunctionName).empty());

  Aws::XRay::Model::Trace trace;
  for (const auto& trace_id : trace_ids.at(kFunctionName)) {
    trace = analyzer.GetTraces({trace_id})[trace_id];
    EXPECT_FALSE(trace.GetSegments().empty());
  }

  const auto segments = LambdaSegmentAnalyzer::GetSegments(trace);
  EXPECT_FALSE(segments.empty());

  const auto lambda_segments =
      LambdaSegmentAnalyzer::CalculateLambdaSegmentDurations(segments, lambda_start_time_, lambda_end_time_);

  EXPECT_FALSE(lambda_segments.empty());
  // Allow some clock skew between local test machine and remote AWS region.
  EXPECT_GT(lambda_segments.at("total_latency_ms").count(), -0.1);
  EXPECT_GT(lambda_segments.at("function_total_latency_ms").count(), -0.1);
  EXPECT_GT(lambda_segments.at("network_total_latency_ms").count(), -0.1);
  EXPECT_GT(lambda_segments.at("network_call_latency_ms").count(), -0.1);
  EXPECT_GT(lambda_segments.at("network_return_latency_ms").count(), -0.1);
  EXPECT_GT(lambda_segments.at("initialization_total_latency_ms").count(), -0.1);
  EXPECT_GT(lambda_segments.at("initialization_latency_ms").count(), -0.1);
  EXPECT_GT(lambda_segments.at("initialization_remainder_latency_ms").count(), -0.1);
  EXPECT_GT(lambda_segments.at("function_execution_latency_ms").count(), -0.1);
  EXPECT_GT(lambda_segments.at("function_overhead_latency_ms").count(), -0.1);
  EXPECT_GT(lambda_segments.at("function_remainder_latency_ms").count(), -0.1);

  EXPECT_GT(analyzer.GetNumAccessedTraces(), 0);
  EXPECT_GT(analyzer.GetNumScannedTraces(), 0);
}

TEST_F(AwsLambdaSegmentsAnalyzerTest, DISABLED_GetCalculatedSegmentsFail) {
  const auto end_time = std::chrono::system_clock::now();

  LambdaSegmentAnalyzer analyzer(client_->GetXRayClient());
  const auto trace_ids = analyzer.GetTraceIds({}, start_time_, end_time);

  EXPECT_TRUE(trace_ids.empty());

  const std::map<Aws::String, Aws::XRay::Model::Trace> traces = analyzer.GetTraces({});

  EXPECT_TRUE(traces.empty());

  const auto segments = LambdaSegmentAnalyzer::GetSegments(Aws::XRay::Model::Trace{});

  EXPECT_TRUE(segments.empty());

  const auto lambda_segments =
      LambdaSegmentAnalyzer::CalculateLambdaSegmentDurations(segments, lambda_start_time_, lambda_end_time_);

  EXPECT_FALSE(lambda_segments.empty());
  EXPECT_EQ(lambda_segments.at("total_latency_ms").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("function_total_latency_ms").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("network_total_latency_ms").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("network_call_latency_ms").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("network_return_latency_ms").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("initialization_total_latency_ms").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("initialization_latency_ms").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("initialization_remainder_latency_ms").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("function_execution_latency_ms").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("function_overhead_latency_ms").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("function_remainder_latency_ms").count(), 0.0);

  EXPECT_EQ(analyzer.GetNumAccessedTraces(), 0);
  EXPECT_EQ(analyzer.GetNumScannedTraces(), 0);
}

}  // namespace skyrise
