#include "monitoring/function_segments.hpp"

#include <fstream>
#include <functional>

#include <aws/core/Aws.h>
#include <aws/core/utils/crypto/CryptoBuf.h>
#include <aws/iam/model/GetRoleRequest.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/FunctionCode.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <gtest/gtest.h>

#include "client/client.hpp"
#include "function/function_utils.hpp"
#include "monitoring_test_utils.hpp"
#include "testing/aws_test.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

class AwsFunctionSegmentsAnalyzerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    start_time_ = std::chrono::system_clock::now();

    client_ = std::make_shared<skyrise::Client>();

    UploadFunctions(client_->GetIamClient(), client_->GetLambdaClient(),
                    std::vector<FunctionConfig>{{kFunctionPath, kFunctionName, kMemorySize, kIsLocal}}, kEnableTracing);
    const auto time_points = InvokeFunction(client_, kFunctionName);

    lambda_start_time_ = time_points.first;
    lambda_end_time_ = time_points.second;
  }

  void TearDown() override { DeleteFunction(client_, kFunctionName); }

  const AwsApi aws_api_;

  std::chrono::time_point<std::chrono::system_clock> lambda_start_time_;
  std::chrono::time_point<std::chrono::system_clock> lambda_end_time_;
  std::chrono::time_point<std::chrono::system_clock> start_time_;

  std::shared_ptr<skyrise::Client> client_;

  static constexpr size_t kMemorySize = 128;
  static constexpr bool kIsLocal = true;
  static constexpr bool kEnableTracing = true;
  static constexpr std::string_view kPackageName = "skyriseFunctionSimple";
  static inline const std::string kFunctionPath = GetFunctionZipFilePath(kPackageName.data());
  static inline const std::string kFunctionName = kPackageName.data() + RandomString(8);
};

TEST_F(AwsFunctionSegmentsAnalyzerTest, GetCalculatedSegments) {
  const auto end_time = std::chrono::system_clock::now();

  FunctionSegmentsAnalyzer analyzer(client_->GetXRayClient());
  const auto trace_ids = analyzer.GetTraceIds({kFunctionName}, start_time_, end_time);

  EXPECT_FALSE(trace_ids.at(kFunctionName).empty());

  Aws::XRay::Model::Trace trace;
  for (const auto& trace_id : trace_ids.at(kFunctionName)) {
    trace = analyzer.GetTraces({trace_id})[trace_id];
    EXPECT_FALSE(trace.GetSegments().empty());
  }

  const auto segments = FunctionSegmentsAnalyzer::GetSegments(trace);
  EXPECT_FALSE(segments.empty());

  const auto lambda_segments =
      FunctionSegmentsAnalyzer::CalculateLambdaSegmentDurations(segments, lambda_start_time_, lambda_end_time_);

  EXPECT_FALSE(lambda_segments.empty());
  // Allow some clock skew between test machine and data center.
  EXPECT_GT(lambda_segments.at("total").count(), -0.1);
  EXPECT_GT(lambda_segments.at("function_total").count(), -0.1);
  EXPECT_GT(lambda_segments.at("network_total").count(), -0.1);
  EXPECT_GT(lambda_segments.at("network_call").count(), -0.1);
  EXPECT_GT(lambda_segments.at("network_return").count(), -0.1);
  EXPECT_GT(lambda_segments.at("initialization_total").count(), -0.1);
  EXPECT_GT(lambda_segments.at("initialization").count(), -0.1);
  EXPECT_GT(lambda_segments.at("initialization_remainder").count(), -0.1);
  EXPECT_GT(lambda_segments.at("function_execution").count(), -0.1);
  EXPECT_GT(lambda_segments.at("function_overhead").count(), -0.1);
  EXPECT_GT(lambda_segments.at("function_remainder").count(), -0.1);

  EXPECT_GT(analyzer.GetNumAccessedTraces(), 0);
  EXPECT_GT(analyzer.GetNumScannedTraces(), 0);
}

TEST_F(AwsFunctionSegmentsAnalyzerTest, GetCalculatedSegmentsFail) {
  const auto end_time = std::chrono::system_clock::now();

  FunctionSegmentsAnalyzer analyzer(client_->GetXRayClient());
  const auto trace_ids = analyzer.GetTraceIds({}, start_time_, end_time);

  EXPECT_TRUE(trace_ids.empty());

  const std::map<Aws::String, Aws::XRay::Model::Trace> traces = analyzer.GetTraces({});

  EXPECT_TRUE(traces.empty());

  const auto segments = FunctionSegmentsAnalyzer::GetSegments(Aws::XRay::Model::Trace{});

  EXPECT_TRUE(segments.empty());

  const auto lambda_segments =
      FunctionSegmentsAnalyzer::CalculateLambdaSegmentDurations(segments, lambda_start_time_, lambda_end_time_);

  EXPECT_FALSE(lambda_segments.empty());
  EXPECT_EQ(lambda_segments.at("total").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("function_total").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("network_total").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("network_call").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("network_return").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("initialization_total").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("initialization").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("initialization_remainder").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("function_execution").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("function_overhead").count(), 0.0);
  EXPECT_EQ(lambda_segments.at("function_remainder").count(), 0.0);

  EXPECT_EQ(analyzer.GetNumAccessedTraces(), 0);
  EXPECT_EQ(analyzer.GetNumScannedTraces(), 0);
}

}  // namespace skyrise
