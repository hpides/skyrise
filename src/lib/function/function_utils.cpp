#include "function_utils.hpp"

#include <regex>
#include <thread>

#include <aws/core/internal/AWSHttpResourceClient.h>
#include <aws/iam/model/GetRoleRequest.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/GetFunctionConfigurationRequest.h>
#include <aws/lambda/model/PublishVersionRequest.h>
#include <sys/utsname.h>

#include "configuration.hpp"
#include "utils/assert.hpp"
#include "utils/filesystem.hpp"
#include "utils/self_name.hpp"

namespace skyrise {

std::string GetProjectDirectoryPath() {
  const std::string path_name = GetAbsolutePathOfSelf();

  // Return the absolute project directory path by removing the path to the executable.
  return path_name.substr(0, path_name.rfind("bin"));
}

std::string GetFunctionZipFilePath(const std::string& zip_file_name) {
  return GetProjectDirectoryPath() + "pkg/" + zip_file_name + ".zip";
}

Aws::Utils::CryptoBuffer OpenFunctionZipFile(const std::string& zip_file_path) {
  const std::string file_buffer = ReadFileToString(zip_file_path);
  return Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(file_buffer.c_str()), file_buffer.length());
}

Aws::Lambda::Model::FunctionCode GetRemoteFunctionCode(const Aws::String& function_path,
                                                       const Aws::String& function_name) {
  Aws::Lambda::Model::FunctionCode code;

  // Extract function name suffix after "S3_".
  const std::regex s3_function_name_regex("S3_([^-]*)");
  std::smatch matches;

  const auto s3_function_name_found = std::regex_search(function_name, matches, s3_function_name_regex);
  Assert(s3_function_name_found, "S3 object key could not be extracted from function name " + function_name + ".");

  code.WithS3Bucket(function_path).WithS3Key(matches[1]);

  return code;
}

bool IsActive(const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client, const std::string& function_name) {
  const auto get_function_configuration_outcome = lambda_client->GetFunctionConfiguration(
      Aws::Lambda::Model::GetFunctionConfigurationRequest().WithFunctionName(function_name));
  Assert(get_function_configuration_outcome.IsSuccess(), get_function_configuration_outcome.GetError().GetMessage());
  return get_function_configuration_outcome.GetResult().GetState() == Aws::Lambda::Model::State::Active;
}

void UploadFunctions(const std::shared_ptr<const Aws::IAM::IAMClient>& iam_client,
                     const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                     const std::vector<FunctionDeployable>& function_deployables, const bool enable_tracing) {
  const auto get_role_request = Aws::IAM::Model::GetRoleRequest().WithRoleName(kLambdaFunctionRoleName.data());
  const auto get_role_outcome = iam_client->GetRole(get_role_request);
  Assert(get_role_outcome.IsSuccess(), get_role_outcome.GetError().GetMessage());
  const auto role_arn = get_role_outcome.GetResult().GetRole().GetArn();

  // We deploy functions based on the host system's processor architecture.
  utsname cpu_information{};
  uname(&cpu_information);
  auto function_architecture = Aws::Lambda::Model::Architecture::x86_64;
  if (cpu_information.machine == std::string("aarch64")) {
    function_architecture = Aws::Lambda::Model::Architecture::arm64;
  }

  std::vector<Aws::Lambda::Model::CreateFunctionOutcomeCallable> create_function_outcome_callables;
  create_function_outcome_callables.reserve(function_deployables.size());
  for (const auto& function_deployable : function_deployables) {
    auto create_function_request =
        Aws::Lambda::Model::CreateFunctionRequest()
            .WithFunctionName(function_deployable.name)
            .WithRuntime(Aws::Lambda::Model::Runtime::provided_al2023)
            .WithArchitectures(Aws::Vector<Aws::Lambda::Model::Architecture>{function_architecture})
            .WithRole(role_arn)
            .WithHandler(kLambdaFunctionHandler.data())
            .WithCode(function_deployable.code.View())
            .WithDescription(function_deployable.description)
            .WithTimeout(kLambdaFunctionTimeoutSeconds)
            .WithMemorySize(function_deployable.memory_size)
            .WithTracingConfig(
                enable_tracing ? Aws::Lambda::Model::TracingConfig().WithMode(Aws::Lambda::Model::TracingMode::Active)
                               : Aws::Lambda::Model::TracingConfig());

    // Optionally configure VPC and EFS connections.
    // TODO(tobodner): For a given EFS ARN, automatically detect subnet ID and security group.
    if (function_deployable.is_vpc_connected || !function_deployable.efs_arn.empty()) {
      create_function_request.WithVpcConfig(Aws::Lambda::Model::VpcConfig()
                                                .WithSubnetIds({kDefaultSubnet})
                                                .WithSecurityGroupIds({kDefaultSecurityGroup}));
    }
    if (!function_deployable.efs_arn.empty()) {
      create_function_request.WithFileSystemConfigs(Aws::Vector<Aws::Lambda::Model::FileSystemConfig>{
          Aws::Lambda::Model::FileSystemConfig().WithArn(function_deployable.efs_arn).WithLocalMountPath("/mnt/efs")});
    }

    create_function_outcome_callables.emplace_back(lambda_client->CreateFunctionCallable(create_function_request));
  }

  for (auto& create_function_outcome_callable : create_function_outcome_callables) {
    const auto& create_function_outcome = create_function_outcome_callable.get();
    Assert(create_function_outcome.IsSuccess(), create_function_outcome.GetError().GetMessage());

    const auto function_state_polling_start = std::chrono::system_clock::now();
    auto function_state_polling_end = function_state_polling_start;

    while (!IsActive(lambda_client, create_function_outcome.GetResult().GetFunctionName())) {
      std::this_thread::sleep_for(std::chrono::milliseconds(kStatePollingIntervalMilliseconds));
      function_state_polling_end = std::chrono::system_clock::now();
      Assert(std::chrono::duration<double>(function_state_polling_end - function_state_polling_start).count() <
                 kLambdaFunctionStatePollingTimeoutSeconds,
             "Function did not become active before the timeout was reached.");
    }

    const auto publish_version_outcome =
        lambda_client->PublishVersion(Aws::Lambda::Model::PublishVersionRequest().WithFunctionName(
            create_function_outcome.GetResult().GetFunctionName()));
    Assert(publish_version_outcome.IsSuccess(), publish_version_outcome.GetError().GetMessage());
  }
}

void UploadFunctions(const std::shared_ptr<const Aws::IAM::IAMClient>& iam_client,
                     const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                     const std::vector<FunctionConfig>& function_configs, const bool enable_tracing) {
  std::vector<FunctionDeployable> function_deployables;
  function_deployables.reserve(function_configs.size());

  for (const auto& function_config : function_configs) {
    Aws::Lambda::Model::FunctionCode function_code;
    if (function_config.is_local_code) {
      function_code.WithZipFile(OpenFunctionZipFile(function_config.path));
    } else {
      function_code = GetRemoteFunctionCode(function_config.path, function_config.name);
    }
    function_deployables.emplace_back(function_config.name, function_code.Jsonize(), function_config.memory_size,
                                      function_config.is_vpc_connected, function_config.description,
                                      function_config.efs_arn);
  }

  UploadFunctions(iam_client, lambda_client, function_deployables, enable_tracing);
}

void DeleteFunction(const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                    const std::string& function_name) {
  const auto delete_request = Aws::Lambda::Model::DeleteFunctionRequest().WithFunctionName(function_name);
  const auto delete_outcome = lambda_client->DeleteFunction(delete_request);
  Assert(delete_outcome.IsSuccess(), delete_outcome.GetError().GetMessage());
}

}  // namespace skyrise
