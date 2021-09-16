#include "monitoring_test_utils.hpp"

#include <fstream>

#include <aws/core/Aws.h>
#include <aws/core/utils/crypto/CryptoBuf.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/iam/model/GetRoleRequest.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/FunctionCode.h>
#include <aws/lambda/model/InvokeRequest.h>

#include "utils/assert.hpp"

namespace skyrise {

// TODO(anyone): Consolidate common utility functions in lib/utils
void UploadFunction(const std::shared_ptr<Client>& client, const std::string& package_name,
                    const std::string& function_name, const std::string& role_name, bool enable_tracing) {
  // TODO(anyone): Use GetProjectDirPath() currently residing in LambdaBenchmarkConfig for more robustness
  const Aws::String function_path = "./pkg/" + package_name + ".zip";
  std::ifstream infile(function_path, std::ios::in | std::ios::binary);

  if (!infile) {
    Fail(function_path + " could not be opened.");
  }

  const std::string file_buffer = StreamToString(&infile);
  Aws::Utils::ByteBuffer byte_buffer(reinterpret_cast<const unsigned char*>(file_buffer.c_str()), file_buffer.length());

  Aws::IAM::Model::GetRoleRequest get_role_request;
  get_role_request.WithRoleName(role_name);
  const auto get_role_outcome = client->GetIAMClient()->GetRole(get_role_request);
  const auto role = get_role_outcome.GetResult().GetRole();

  Assert(get_role_outcome.IsSuccess(), get_role_outcome.GetError().GetMessage());

  Aws::Lambda::Model::CreateFunctionRequest create_function_request;
  create_function_request.WithFunctionName(function_name)
      .WithHandler("FunctionHandler")
      .WithRole(role.GetArn())
      .WithCode(Aws::Lambda::Model::FunctionCode().WithZipFile(byte_buffer))
      .WithRuntime(Aws::Lambda::Model::Runtime::provided_al2)
      .WithTracingConfig(enable_tracing
                             ? Aws::Lambda::Model::TracingConfig().WithMode(Aws::Lambda::Model::TracingMode::Active)
                             : Aws::Lambda::Model::TracingConfig());

  const auto create_function_outcome = client->GetLambdaClient()->CreateFunction(create_function_request);

  Assert(create_function_outcome.IsSuccess(), create_function_outcome.GetError().GetMessage());
}

std::pair<std::chrono::time_point<std::chrono::system_clock>, std::chrono::time_point<std::chrono::system_clock>>
InvokeFunction(const std::shared_ptr<Client>& client, const std::string& function_name) {
  Aws::Lambda::Model::InvokeRequest invoke_request;
  invoke_request.WithFunctionName(function_name);

  const auto start_time = std::chrono::system_clock::now();
  const auto invoke_function_outcome = client->GetLambdaClient()->Invoke(invoke_request);
  const auto end_time = std::chrono::system_clock::now();

  Assert(invoke_function_outcome.IsSuccess(), invoke_function_outcome.GetError().GetMessage());

  return {start_time, end_time};
}

void DeleteFunction(const std::shared_ptr<Client>& client, const std::string& function_name) {
  Aws::Lambda::Model::DeleteFunctionRequest delete_request;
  delete_request.WithFunctionName(function_name);
  const auto delete_function_outcome = client->GetLambdaClient()->DeleteFunction(delete_request);

  Assert(delete_function_outcome.IsSuccess(), delete_function_outcome.GetError().GetMessage());
}

}  // namespace skyrise
