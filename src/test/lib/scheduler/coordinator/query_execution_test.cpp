#include <thread>

#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/InvokeResult.h>

#include "client/coordinator_client.hpp"
#include "compiler/pipeline_generator/tpch_compiler.hpp"
#include "coordinator.hpp"
#include "function/function_utils.hpp"
#include "gtest/gtest.h"
#include "scheduler/coordinator/pqp_pipeline_scheduler.hpp"
#include "testing/aws_test.hpp"
#include "testing/load_table.hpp"

namespace skyrise {

class AwsQueryExecutionTest : public ::testing::Test {
 public:
  void SetUp() override {
    client_ = std::make_shared<CoordinatorClient>();
    coordinator_function_name_ = GetUniqueName(kCoordinatorFunctionName);
    worker_function_name_ = GetUniqueName(kWorkerFunctionName);

    UploadFunctions(client_->GetIamClient(), client_->GetLambdaClient(),
                    std::vector<FunctionConfig>{
                        {GetFunctionZipFilePath(kCoordinatorBinaryName), coordinator_function_name_, 2000, true},
                        {GetFunctionZipFilePath(kWorkerBinaryName), worker_function_name_, 2000, true}},
                    false);

    executor_ = std::make_shared<LambdaExecutor>(client_, worker_function_name_);
  }

  void TearDown() override {
    DeleteFunction(client_->GetLambdaClient(), worker_function_name_);
    DeleteFunction(client_->GetLambdaClient(), coordinator_function_name_);
  }

 protected:
  const AwsApi aws_api_;
  std::shared_ptr<CoordinatorClient> client_;
  std::string worker_function_name_;
  std::string coordinator_function_name_;
  std::shared_ptr<AbstractPqpPipelineFragmentExecutor> executor_;
};

TEST_F(AwsQueryExecutionTest, Q6) {
  const auto compiler = TpchCompilerConfig(TpchCompiler::kQ6, TpchCompiler::kSF1,
                                           ObjectReference(kSkyriseTestBucket, GetUniqueName("queryExecution")))
                            .GenerateCompiler();
  auto pipelines = compiler->Generate();

  Coordinator coordinator(client_);
  const auto [result, statistics] = coordinator.RunPipelines(executor_, pipelines);

  EXPECT_TRUE(compiler->VerifyResult(client_, *result.get()));
}

TEST_F(AwsQueryExecutionTest, Q6CoordinatorFunction) {
  const auto compiler_config = TpchCompilerConfig(TpchCompiler::kQ6, TpchCompiler::kSF1,
                                                  ObjectReference(kSkyriseTestBucket, GetUniqueName("queryExecution")));

  Aws::Lambda::Model::InvokeRequest invoke_request;
  invoke_request.WithFunctionName(coordinator_function_name_)
      .WithInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse)
      .SetLogType(Aws::Lambda::Model::LogType::None);

  Aws::Utils::Json::JsonValue payload =
      Aws::Utils::Json::JsonValue()
          .WithObject(kCoordinatorRequestCompilerConfigAttribute, compiler_config.ToJson())
          .WithString(kCoordinatorRequestWorkerFunctionAttribute, worker_function_name_);

  const auto payload_stream = std::make_shared<std::stringstream>(payload.View().WriteCompact());
  invoke_request.SetBody(payload_stream);

  auto result = client_->GetLambdaClient()->Invoke(invoke_request);
  EXPECT_TRUE(result.IsSuccess());
  auto& result_payload_stream = result.GetResult().GetPayload();
  auto result_payload = Aws::Utils::Json::JsonValue(result_payload_stream);
  ObjectReference target_object =
      ObjectReference::FromJson(result_payload.View().GetObject(kCoordinatorResponseResultObjectAttribute));

  compiler_config.GenerateCompiler()->VerifyResult(client_, target_object);

  EXPECT_TRUE(compiler_config.GenerateCompiler()->VerifyResult(client_, target_object));
}

TEST_F(AwsQueryExecutionTest, Q12) {
  const auto compiler = TpchCompilerConfig(TpchCompiler::kQ12, TpchCompiler::kSF1,
                                           ObjectReference(kSkyriseTestBucket, GetUniqueName("queryExecution") + "/"))
                            .GenerateCompiler();
  auto pipelines = compiler->Generate();

  Coordinator coordinator(client_);
  const auto [result, statistics] = coordinator.RunPipelines(executor_, pipelines);
  EXPECT_TRUE(compiler->VerifyResult(client_, *result.get()));
}

}  // namespace skyrise
