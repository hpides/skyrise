#include "function_segments.hpp"

#include <fstream>
#include <functional>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/crypto/CryptoBuf.h>
#include <aws/iam/IAMClient.h>
#include <aws/iam/model/GetRoleRequest.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/FunctionCode.h>
#include <aws/lambda/model/InvokeRequest.h>

#include "client/client.hpp"
#include "gtest/gtest.h"
#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

class FunctionSegmentAnalyzerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::function<void()> api_executable = [&]() {
      const Aws::String function_path = "./pkg/" + kPackageName + ".zip";
      std::ifstream infile(function_path, std::ios::in | std::ios::binary);

      if (!infile) {
        Fail(function_path + " could not be opened.");
      }

      const std::string file_buffer = StreamToString(&infile);

      Aws::Utils::ByteBuffer byte_buffer(reinterpret_cast<const unsigned char*>(file_buffer.c_str()),
                                         file_buffer.length());

      Aws::IAM::Model::GetRoleRequest get_role_request;
      get_role_request.WithRoleName(kRoleName);

      client_ = std::make_shared<skyrise::Client>();
      const auto role = client_->GetIAMClient().GetRole(get_role_request).GetResult().GetRole();

      Aws::Lambda::Model::CreateFunctionRequest create_function_request;
      create_function_request.WithFunctionName(kFunctionName)
          .WithHandler("functionHandler")
          .WithRole(role.GetArn())
          .WithCode(Aws::Lambda::Model::FunctionCode().WithZipFile(byte_buffer))
          .WithRuntime(Aws::Lambda::Model::Runtime::provided_al2)
          .WithTracingConfig(Aws::Lambda::Model::TracingConfig().WithMode(Aws::Lambda::Model::TracingMode::Active));

      client_->GetLambdaClient().CreateFunction(create_function_request);

      Aws::Lambda::Model::InvokeRequest invoke_request;
      invoke_request.WithFunctionName(kFunctionName);

      lambda_start_time_ = std::chrono::system_clock::now();
      client_->GetLambdaClient().Invoke(invoke_request);
      lambda_end_time_ = std::chrono::system_clock::now();
    };

    start_time_ = std::chrono::system_clock::now();
    ExecuteInsideAPI(api_executable);
  }

  void TearDown() override {
    std::function<void()> api_executable = [&]() {
      Aws::Lambda::Model::DeleteFunctionRequest delete_request;
      delete_request.WithFunctionName(kFunctionName);
      client_->GetLambdaClient().DeleteFunction(delete_request);
    };

    ExecuteInsideAPI(api_executable);
  }

  static void ExecuteInsideAPI(const std::function<void()>& function) {
    Aws::SDKOptions options;

    Aws::InitAPI(options);
    { function(); }
    Aws::ShutdownAPI(options);
  }

  std::chrono::time_point<std::chrono::system_clock> lambda_start_time_;
  std::chrono::time_point<std::chrono::system_clock> lambda_end_time_;
  std::chrono::time_point<std::chrono::system_clock> start_time_;

 public:
  std::shared_ptr<skyrise::Client> client_;

  const std::string kPackageName = "skyriseFunctionMinimal";
  const std::string kFunctionName = kPackageName + RandomString(8);
  const std::string kRoleName = "AWSLambda";
};

TEST_F(FunctionSegmentAnalyzerTest, GetCalculatedSegments) {
  std::function<void()> api_function = [&]() {
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
    EXPECT_GT(lambda_segments.at("total").count(), 0.0);
    EXPECT_GT(lambda_segments.at("function_total").count(), 0.0);
    EXPECT_GT(lambda_segments.at("network_total").count(), 0.0);
    EXPECT_GT(lambda_segments.at("network_call").count(), 0.0);
    EXPECT_GT(lambda_segments.at("network_return").count(), 0.0);
    EXPECT_GT(lambda_segments.at("initialization_total").count(), 0.0);
    EXPECT_GT(lambda_segments.at("initialization").count(), 0.0);
    EXPECT_GT(lambda_segments.at("initialization_remainder").count(), 0.0);
    EXPECT_GT(lambda_segments.at("function_execution").count(), 0.0);
    EXPECT_GT(lambda_segments.at("function_overhead").count(), 0.0);
    EXPECT_GT(lambda_segments.at("function_remainder").count(), 0.0);

    EXPECT_GT(analyzer.GetNumAccessedTraces(), 0);
    EXPECT_GT(analyzer.GetNumScannedTraces(), 0);
  };

  ExecuteInsideAPI(api_function);
}

}  // namespace skyrise
