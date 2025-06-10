#pragma once

#include <string>

#include <aws/core/Aws.h>
#include <aws/iam/IAMClient.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/lambda/model/FunctionCode.h>

#include "function/function_config.hpp"

namespace skyrise {

std::string GetProjectDirectoryPath();

std::string GetFunctionZipFilePath(const std::string& zip_file_name);

Aws::Utils::CryptoBuffer OpenFunctionZipFile(const std::string& zip_file_path);

Aws::Lambda::Model::FunctionCode GetRemoteFunctionCode(const std::string& function_path,
                                                       const std::string& function_name);

bool IsActive(const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client, const std::string& function_name);

void UploadFunctions(const std::shared_ptr<const Aws::IAM::IAMClient>& iam_client,
                     const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                     const std::vector<FunctionDeployable>& function_deployables, const bool enable_tracing);

void UploadFunctions(const std::shared_ptr<const Aws::IAM::IAMClient>& iam_client,
                     const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                     const std::vector<FunctionConfig>& function_configs, const bool enable_tracing);

void DeleteFunction(const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                    const std::string& function_name);

}  // namespace skyrise
