#include "monitoring/tracer.hpp"

#include <chrono>
#include <thread>

#include <aws/iam/model/GetRoleRequest.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/FunctionCode.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <gtest/gtest.h>

#include "client/coordinator_client.hpp"
#include "function/function_utils.hpp"
#include "monitoring/lambda_segments_analyzer.hpp"
#include "monitoring/monitoring_types.hpp"
#include "monitoring_test_utils.hpp"
#include "testing/aws_test.hpp"
#include "utils/assert.hpp"

namespace skyrise {

class AwsTracerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_shared<CoordinatorClient>();

    UploadFunctions(client_->GetIamClient(), client_->GetLambdaClient(),
                    std::vector<FunctionConfig>{{kFunctionPath, kFunctionName, kMemorySize, kIsLocal, kEnableVpc}},
                    kEnableTracing);
    const auto time_points = InvokeFunction(client_, kFunctionName);

    start_time_ = time_points.first;
    end_time_ = time_points.second;
  }

  void TearDown() override { DeleteFunction(client_, kFunctionName); }

  const AwsApi aws_api_;

  std::shared_ptr<CoordinatorClient> client_;
  std::chrono::time_point<std::chrono::system_clock> start_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;

  static constexpr size_t kMemorySize = 128;
  static constexpr bool kIsLocal = true;
  static constexpr bool kEnableVpc = false;
  static constexpr bool kEnableTracing = true;
  static constexpr std::string_view kPackageName = "skyriseSimpleFunction";
  static constexpr size_t kSleepSeconds = 5;
  static inline const std::string kFunctionPath = GetFunctionZipFilePath(kPackageName.data());
  static inline const std::string kFunctionName = GetUniqueName(kPackageName.data());
};

TEST_F(AwsTracerTest, DISABLED_GetTrace) {
  LambdaSegmentAnalyzer function_segments_analyzer(client_->GetXRayClient());
  const auto trace_ids = function_segments_analyzer.GetTraceIds({kFunctionName}, start_time_, end_time_);

  const std::string trace_id = *trace_ids.at(kFunctionName).cbegin();
  auto trace = function_segments_analyzer.GetTraces({trace_id})[trace_id];
  auto segments = LambdaSegmentAnalyzer::GetSegments(trace);
  const std::string id = segments.at("Invocation").View().GetString("id");
  const std::string xray_trace_id = "Root=" + trace_id + ";Parent=" + id + ";Sampled=1";

  {
    Tracer tracer(client_->GetXRayClient(), xray_trace_id, SubqueryFragmentIdentifier{});

    tracer.EnterOperator("Operator1");
    tracer.EnterStage("Stage1");
    tracer.EnterStage("Stage2");

    tracer.EnterOperator("Operator2");
    tracer.EnterStage("Stage1");
    tracer.EnterStage("Stage2");
    tracer.EnterStage("Stage3");
  }

  std::this_thread::sleep_for(std::chrono::seconds(kSleepSeconds));

  trace = function_segments_analyzer.GetTraces({trace_id})[trace_id];
  segments = LambdaSegmentAnalyzer::GetSegments(trace);

  EXPECT_NO_THROW(segments.at("Operator1"));
  EXPECT_NO_THROW(segments.at("Operator1_Stage1"));
  EXPECT_NO_THROW(segments.at("Operator1_Stage2"));
  EXPECT_NO_THROW(segments.at("Operator2"));
  EXPECT_NO_THROW(segments.at("Operator2_Stage1"));
  EXPECT_NO_THROW(segments.at("Operator2_Stage2"));
  EXPECT_NO_THROW(segments.at("Operator2_Stage3"));
  EXPECT_ANY_THROW(segments.at("Undefined"));

  EXPECT_TRUE(segments.at("Operator1").View().KeyExists("annotations"));
  EXPECT_TRUE(segments.at("Operator2").View().KeyExists("annotations"));
  EXPECT_FALSE(segments.at("Operator2_Stage3").View().KeyExists("annotations"));
}

}  // namespace skyrise
