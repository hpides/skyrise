#include "system_benchmark_runner.hpp"

#include <aws/lambda/model/InvokeRequest.h>
#include <magic_enum/magic_enum.hpp>

#include "function/function_utils.hpp"

namespace skyrise {

SystemBenchmarkRunner::SystemBenchmarkRunner(std::shared_ptr<const Aws::IAM::IAMClient> iam_client,
                                             std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client,
                                             std::shared_ptr<const Aws::S3::S3Client> s3_client,
                                             std::shared_ptr<const CostCalculator> cost_calculator,
                                             const Aws::String& client_region, const bool metering,
                                             const bool introspection)
    : AbstractBenchmarkRunner(metering, introspection),
      iam_client_(std::move(iam_client)),
      lambda_client_(std::move(lambda_client)),
      s3_client_(std::move(s3_client)),
      cost_calculator_(std::move(cost_calculator)),
      client_region_(client_region) {}

std::shared_ptr<SystemBenchmarkResult> SystemBenchmarkRunner::RunSystemConfig(
    const std::shared_ptr<SystemBenchmarkConfig>& config) {
  return std::dynamic_pointer_cast<SystemBenchmarkResult>(RunConfig(config));
}

void SystemBenchmarkRunner::Setup() {
  coordinator_function_name_ =
      config_->GetBenchmarkTimestamp() + "-" + kCoordinatorFunctionName + "-" + config_->GetBenchmarkId();
  worker_function_name_ =
      config_->GetBenchmarkTimestamp() + "-" + kWorkerFunctionName + "-" + config_->GetBenchmarkId();
  shuffle_storage_identifier_ =
      config_->GetBenchmarkTimestamp() + "-" + "systemBenchmark" + "-" + config_->GetBenchmarkId();
  const size_t vcpus_per_worker_function_count = 4;

  UploadFunctions(
      iam_client_, lambda_client_,
      std::vector<FunctionConfig>{
          {GetFunctionZipFilePath(kCoordinatorBinaryName), coordinator_function_name_, kLambdaMaximumMemorySizeMb, true,
           false},
          {GetFunctionZipFilePath(kWorkerBinaryName), worker_function_name_,
           static_cast<size_t>(vcpus_per_worker_function_count * kLambdaVcpuEquivalentMemorySizeMb), true, false}},
      false);
}

void SystemBenchmarkRunner::Teardown() {
  DeleteFunction(lambda_client_, worker_function_name_);
  DeleteFunction(lambda_client_, coordinator_function_name_);
}

std::shared_ptr<AbstractBenchmarkResult> SystemBenchmarkRunner::OnRunConfig() {
  typed_config_ = std::dynamic_pointer_cast<SystemBenchmarkConfig>(config_);
  Assert(typed_config_, "SystemBenchmarkRunner can only consume SystemBenchmarkConfigs.");

  size_t results_count = typed_config_->GetRepetitionCount();
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> results(results_count);
  for (size_t i = 0; i < results_count; ++i) {
    auto invoke_request = Aws::Lambda::Model::InvokeRequest()
                              .WithFunctionName(coordinator_function_name_)
                              .WithInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
    Aws::Utils::Json::JsonValue payload =
        Aws::Utils::Json::JsonValue()
            .WithString(kCoordinatorRequestCompilerNameAttribute,
                        std::string(magic_enum::enum_name(typed_config_->GetCompilerName())))
            .WithString(kCoordinatorRequestQueryPlanAttribute,
                        std::string(magic_enum::enum_name(typed_config_->GetQueryId())))
            .WithString(kCoordinatorRequestScaleFactorAttribute,
                        std::string(magic_enum::enum_name(typed_config_->GetScaleFactor())))
            .WithObject(kCoordinatorRequestShuffleStorageAttribute,
                        ObjectReference(kSkyriseBenchmarkContainer,
                                        shuffle_storage_identifier_ + "/repetition-" + std::to_string(i))
                            .ToJson())
            .WithString(kCoordinatorRequestWorkerFunctionAttribute, worker_function_name_);
    const auto request_payload_stream = std::make_shared<std::stringstream>(payload.View().WriteCompact());
    invoke_request.SetBody(request_payload_stream);

    const auto outcome = lambda_client_->Invoke(invoke_request);
    if (outcome.IsSuccess()) {
      auto& response_payload_stream = outcome.GetResult().GetPayload();
      results[i] = Aws::Utils::Json::JsonValue(response_payload_stream);
    } else {
      AWS_LOGSTREAM_ERROR(kBenchmarkTag.c_str(), outcome.GetError().GetMessage());
    }
  }
  return std::make_shared<SystemBenchmarkResult>(results);
}

}  // namespace skyrise
