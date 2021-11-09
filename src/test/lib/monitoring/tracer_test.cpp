#include "monitoring/tracer.hpp"

#include <chrono>
#include <fstream>
#include <thread>

#include <aws/core/Aws.h>
#include <aws/core/utils/crypto/CryptoBuf.h>
#include <aws/iam/model/GetRoleRequest.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/FunctionCode.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <gtest/gtest.h>

#include "client/client.hpp"
#include "lib/testing/aws_test.hpp"
#include "monitoring/function_segments.hpp"
#include "monitoring_test_utils.hpp"
#include "utils/assert.hpp"

namespace skyrise {

class AwsTracerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_shared<skyrise::Client>();

    UploadFunction(client_, kPackageName, function_name_, kRoleName, kEnableTracing);
    const auto time_points = InvokeFunction(client_, function_name_);

    start_time_ = time_points.first;
    end_time_ = time_points.second;
  }

  void TearDown() override { DeleteFunction(client_, function_name_); }

  const AwsApi aws_api_;

  std::shared_ptr<skyrise::Client> client_;
  std::chrono::time_point<std::chrono::system_clock> start_time_;
  std::chrono::time_point<std::chrono::system_clock> end_time_;

  static constexpr bool kEnableTracing = true;
  static constexpr size_t kSleepSeconds = 5;
  inline static const std::string kPackageName = "skyriseFunctionSimple";
  inline static const std::string kRoleName = "AWSLambda";
  const std::string function_name_ = kPackageName + RandomString(8);
};

TEST_F(AwsTracerTest, GetTrace) {
  FunctionSegmentsAnalyzer function_segments_analyzer(client_->GetXRayClient());
  const auto trace_ids = function_segments_analyzer.GetTraceIds({function_name_}, start_time_, end_time_);

  const std::string trace_id = *trace_ids.at(function_name_).cbegin();
  auto trace = function_segments_analyzer.GetTraces({trace_id})[trace_id];
  auto segments = FunctionSegmentsAnalyzer::GetSegments(trace);
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
  segments = FunctionSegmentsAnalyzer::GetSegments(trace);

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
