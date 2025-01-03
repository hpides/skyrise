#include "monitoring_test_utils.hpp"

#include <fstream>

#include <aws/iam/model/GetRoleRequest.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/FunctionCode.h>
#include <aws/lambda/model/InvokeRequest.h>

#include "client/base_client.hpp"
#include "utils/assert.hpp"

namespace skyrise {

std::pair<std::chrono::time_point<std::chrono::system_clock>, std::chrono::time_point<std::chrono::system_clock>>
InvokeFunction(const std::shared_ptr<BaseClient>& client, const std::string& function_name) {
  Aws::Lambda::Model::InvokeRequest invoke_request;
  invoke_request.WithFunctionName(function_name);

  const auto start_time = std::chrono::system_clock::now();
  const auto invoke_function_outcome = client->GetLambdaClient()->Invoke(invoke_request);
  const auto end_time = std::chrono::system_clock::now();

  Assert(invoke_function_outcome.IsSuccess(), invoke_function_outcome.GetError().GetMessage());

  return {start_time, end_time};
}

void DeleteFunction(const std::shared_ptr<BaseClient>& client, const std::string& function_name) {
  Aws::Lambda::Model::DeleteFunctionRequest delete_request;
  delete_request.WithFunctionName(function_name);
  const auto delete_function_outcome = client->GetLambdaClient()->DeleteFunction(delete_request);

  Assert(delete_function_outcome.IsSuccess(), delete_function_outcome.GetError().GetMessage());
}

}  // namespace skyrise
