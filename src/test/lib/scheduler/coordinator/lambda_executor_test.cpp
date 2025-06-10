#include "scheduler/coordinator/lambda_executor.hpp"

#include <mutex>

#include <gtest/gtest.h>

#include "client/coordinator_client.hpp"
#include "compiler/pqp_generator/etl_pqp_generator.hpp"
#include "constants.hpp"
#include "function/function_utils.hpp"
#include "testing/aws_test.hpp"
#include "utils/time.hpp"

namespace skyrise {

/**
 * This test only covers the scheduling of Lambda-based workers and not result correctness.
 */
class LambdaExecutorTest : public ::testing::Test {
 public:
  void SetUp() override {
    client_ = std::make_shared<CoordinatorClient>();
    worker_function_name_ = GetUniqueName(kWorkerFunctionName);
    const size_t vcpus_per_function_count = 5;

    UploadFunctions(
        client_->GetIamClient(), client_->GetLambdaClient(),
        std::vector<FunctionConfig>{{GetFunctionZipFilePath(kWorkerBinaryName), worker_function_name_,
                                     static_cast<size_t>(vcpus_per_function_count * kLambdaVcpuEquivalentMemorySizeMb),
                                     true, false}},
        false);

    executor_ = std::make_shared<LambdaExecutor>(client_, worker_function_name_);
    results_.clear();
  }

  void TearDown() override { DeleteFunction(client_->GetLambdaClient(), worker_function_name_); }

 protected:
  void WaitForResults(size_t result_count) {
    const auto result_polling_start = std::chrono::system_clock::now();
    while (true) {
      {
        std::lock_guard<std::mutex> lock(this->result_mutex_);
        if (result_count == results_.size()) {
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(kStatePollingIntervalMilliseconds));
      Assert(std::chrono::duration<double>(std::chrono::system_clock::now() - result_polling_start).count() <
                 kStatePollingTimeoutSeconds,
             "Timeout during result polling.");
    }
  }

  const AwsApi aws_api_;
  std::shared_ptr<CoordinatorClient> client_;
  std::string worker_function_name_;
  std::shared_ptr<LambdaExecutor> executor_;
  std::mutex result_mutex_;
  std::vector<std::shared_ptr<PqpPipelineFragmentExecutionResult>> results_;
};

TEST_F(LambdaExecutorTest, DirectInvocation) {
  const std::string output_prefix = GetUniqueName("LambdaExecutorTest.DirectInvocation");
  const auto compiler_config =
      EtlPqpGeneratorConfig(CompilerName::kEtl, QueryId::kEtlCopyTpchOrders, ScaleFactor::kSf100,
                            ObjectReference(kSkyriseTestBucket, output_prefix));
  const auto compiler = compiler_config.GenerateCompiler();
  const auto copy_pqp = compiler->GeneratePqp();

  executor_->Execute(copy_pqp.front(), [this](std::shared_ptr<PqpPipelineFragmentExecutionResult> result) {
    std::lock_guard<std::mutex> lock(this->result_mutex_);
    results_.push_back(std::move(result));
  });

  const size_t fragment_count = 25;
  EXPECT_GT(kWorkerRecursiveInvocationThreshold, fragment_count);
  WaitForResults(fragment_count);
  EXPECT_EQ(results_.size(), fragment_count);
}

TEST_F(LambdaExecutorTest, RecursiveInvocation) {
  const std::string output_prefix = GetUniqueName("LambdaExecutorTest.RecursiveInvocation");
  const auto compiler_config =
      EtlPqpGeneratorConfig(CompilerName::kEtl, QueryId::kEtlCopyTpchOrders, ScaleFactor::kSf1000,
                            ObjectReference(kSkyriseTestBucket, output_prefix));
  const auto compiler = compiler_config.GenerateCompiler();
  const auto copy_pqp = compiler->GeneratePqp();

  executor_->Execute(copy_pqp.front(), [this](std::shared_ptr<PqpPipelineFragmentExecutionResult> result) {
    std::lock_guard<std::mutex> lock(this->result_mutex_);
    results_.push_back(std::move(result));
  });

  const size_t fragment_count = 249;
  EXPECT_LT(kWorkerRecursiveInvocationThreshold, fragment_count);
  WaitForResults(fragment_count);
  EXPECT_EQ(results_.size(), fragment_count);
}

}  // namespace skyrise
