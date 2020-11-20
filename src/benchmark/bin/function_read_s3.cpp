#include <chrono>
#include <memory>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/lambda-runtime/runtime.h>
#include <aws/s3/S3Client.h>
#include <magic_enum.hpp>

#include "storage/backend/storage_s3.hpp"

namespace skyrise {

const std::string kTag = "SKYRISE/BENCHMARK/WORKER/READ_S3";

std::tuple<StorageError, double> GetObjectsS3(const std::shared_ptr<Aws::S3::S3Client>& client,
                                              const Aws::String& bucket,
                                              const Aws::Utils::Array<Aws::Utils::Json::JsonView>& keys) {
  std::vector<std::future<StorageError>> read_object_result_futures;
  read_object_result_futures.reserve(keys.GetLength());

  const auto start = std::chrono::steady_clock::now();

  for (size_t i = 0; i < keys.GetLength(); i++) {
    read_object_result_futures.emplace_back(std::async(
        [&](const size_t i) {
          return S3ObjectReader(client, bucket, keys[i].AsString())
              .Read(0, S3ObjectReader::kLastByteInFile, [](const char* /*data*/, size_t /*length*/) {});
        },
        i));
  }

  for (auto& read_object_result_future : read_object_result_futures) {
    const auto& read_object_result = read_object_result_future.get();

    if (read_object_result.GetType() != StorageErrorType::kNoError) {
      AWS_LOGSTREAM_ERROR(kTag.c_str(), read_object_result.GetMessage());
      return {read_object_result, 0.0};
    }
  }

  const auto end = std::chrono::steady_clock::now();

  return {StorageError::Success(), std::chrono::duration<double, std::milli>(end - start).count()};
}

}  // namespace skyrise

aws::lambda_runtime::invocation_response HandlerFunction(const aws::lambda_runtime::invocation_request& request,
                                                         const std::shared_ptr<Aws::S3::S3Client>& s3_client) {
  const auto json_value = Aws::Utils::Json::JsonValue(request.payload);
  const auto json_view = json_value.View();

  const bool is_warmup = json_view.GetBool("is_warmup");

  if (is_warmup) {
    const auto response = Aws::Utils::Json::JsonValue().WithBool("is_warmup", true).View();
    return aws::lambda_runtime::invocation_response::success(response.WriteCompact(), "application/json");
  }

  const Aws::String s3_bucket = json_view.GetString("s3_bucket");
  const auto s3_keys = json_view.GetArray("s3_keys");

  const auto& [error, duration_ms] = skyrise::GetObjectsS3(s3_client, s3_bucket, s3_keys);

  if (error.GetType() != skyrise::StorageErrorType::kNoError) {
    return aws::lambda_runtime::invocation_response::failure(error.GetMessage(),
                                                             std::string(magic_enum::enum_name(error.GetType())));
  }

  const auto response_value = Aws::Utils::Json::JsonValue()
                                  .WithDouble("duration_ms", duration_ms)
                                  .WithInteger("num_s3_requests_tier_1", 0)
                                  .WithInteger("num_s3_requests_tier_2", s3_keys.GetLength())
                                  .WithInt64("s3_storage_used_bytes", 0);
  return aws::lambda_runtime::invocation_response::success(response_value.View().WriteCompact(), "application/json");
}

int main() {
  Aws::SDKOptions options;
  options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
  options.loggingOptions.logger_create_fn = [] {
    return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_logger",
                                                                  Aws::Utils::Logging::LogLevel::Info);
  };

  Aws::InitAPI(options);
  {
    Aws::Client::ClientConfiguration client_config;
    client_config.region = Aws::Environment::GetEnv("AWS_REGION");
    client_config.caFile = "/etc/pki/tls/certs/ca-bundle.crt";

    const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();

    const auto s3_client = std::make_shared<Aws::S3::S3Client>(
        credentials_provider, client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always, false);

    const auto handler_function = [&s3_client](const aws::lambda_runtime::invocation_request& request) {
      return HandlerFunction(request, s3_client);
    };

    aws::lambda_runtime::run_handler(handler_function);
  }
  Aws::ShutdownAPI(options);

  return 0;
}
