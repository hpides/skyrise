#include "function_read_s3.hpp"

#include <chrono>
#include <memory>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/lambda-runtime/runtime.h>
#include <aws/s3/S3Client.h>
#include <magic_enum.hpp>

#include "storage/backend/storage_s3.hpp"

namespace skyrise {

const std::string kTag = "SKYRISE/BENCHMARK/WORKER/READ_S3";

aws::lambda_runtime::invocation_response FunctionReadS3::OnHandleRequest(
    const Aws::Utils::Json::JsonView& request) const {
  const Aws::String bucket = request.GetString("s3_bucket");
  const auto keys = request.GetArray("s3_keys");
  const size_t batch_size = request.GetInteger("batch_size");

  Aws::Client::ClientConfiguration client_config;
  client_config.caFile = "/etc/pki/tls/certs/ca-bundle.crt";
  const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();
  const auto client = std::make_shared<Aws::S3::S3Client>(credentials_provider, client_config);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> ms_durations(batch_size);

  for (size_t i = 0; i < batch_size; i++) {
    std::vector<std::future<StorageError>> read_object_result_futures;
    read_object_result_futures.reserve(keys.GetLength());

    const auto start = std::chrono::steady_clock::now();

    for (size_t j = 0; j < keys.GetLength(); j++) {
      read_object_result_futures.emplace_back(std::async(
          [&](const size_t i) {
            return S3ObjectReader(client, bucket, keys[i].AsString())
                .Read(0, S3ObjectReader::kLastByteInFile, [](const char* /*data*/, size_t /*length*/) {});
          },
          j));
    }

    for (auto& read_object_result_future : read_object_result_futures) {
      const auto& read_object_result = read_object_result_future.get();

      if (read_object_result.GetType() != StorageErrorType::kNoError) {
        AWS_LOGSTREAM_ERROR(kTag.c_str(), read_object_result.GetMessage());
        return aws::lambda_runtime::invocation_response::failure(
            read_object_result.GetMessage(), std::string(magic_enum::enum_name(read_object_result.GetType())));
      }
    }

    const auto end = std::chrono::steady_clock::now();

    ms_durations[i] =
        Aws::Utils::Json::JsonValue().AsDouble(std::chrono::duration<double, std::milli>(end - start).count());
  }

  const auto response_value =
      Aws::Utils::Json::JsonValue()
          .WithArray("ms_durations", ms_durations)
          .WithInteger("num_s3_requests_tier_1", 0)
          .WithInteger("num_s3_requests_tier_2", static_cast<size_t>(keys.GetLength() * batch_size))
          .WithInt64("s3_storage_used_bytes", 0);
  return aws::lambda_runtime::invocation_response::success(response_value.View().WriteCompact(), "application/json");
}

}  // namespace skyrise

int main() {
  skyrise::FunctionReadS3 function_read_s3;
  function_read_s3.HandleRequest();

  return 0;
}
