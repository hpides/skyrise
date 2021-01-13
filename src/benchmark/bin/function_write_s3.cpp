#include "function_write_s3.hpp"

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
#include "utils/string.hpp"

namespace skyrise {

const std::string kTag = "SKYRISE/BENCHMARK/WORKER/WRITE_S3";

aws::lambda_runtime::invocation_response FunctionWriteS3::OnHandleRequest(
    const Aws::Utils::Json::JsonView& request) const {
  const Aws::String bucket = request.GetString("s3_bucket");
  const auto keys = request.GetArray("s3_keys");
  const size_t num_bytes = request.GetInteger("object_byte_size");
  const size_t batch_size = request.GetInteger("batch_size");

  Aws::Client::ClientConfiguration client_config;
  client_config.caFile = "/etc/pki/tls/certs/ca-bundle.crt";
  const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();
  const auto client = std::make_shared<Aws::S3::S3Client>(credentials_provider, client_config);

  const std::string s3_object = RandomString(num_bytes);
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> ms_durations(batch_size);

  for (size_t i = 0; i < batch_size; i++) {
    std::vector<std::future<StorageError>> write_object_result_futures;
    write_object_result_futures.reserve(keys.GetLength());

    const auto start = std::chrono::steady_clock::now();

    for (size_t j = 0; j < keys.GetLength(); j++) {
      write_object_result_futures.emplace_back(std::async(
          [&](const size_t i) {
            S3ObjectWriter object_writer(client, bucket, keys[i].AsString() + std::to_string(i));
            object_writer.Write(s3_object.c_str(), num_bytes);

            return object_writer.Close();
          },
          j));
    }

    for (auto& write_object_result_future : write_object_result_futures) {
      const auto& write_object_result = write_object_result_future.get();

      if (write_object_result.GetType() != StorageErrorType::kNoError) {
        AWS_LOGSTREAM_ERROR(kTag.c_str(), write_object_result.GetMessage());
        return aws::lambda_runtime::invocation_response::failure(
            write_object_result.GetMessage(), std::string(magic_enum::enum_name(write_object_result.GetType())));
      }
    }

    const auto end = std::chrono::steady_clock::now();

    ms_durations[i] =
        Aws::Utils::Json::JsonValue().AsDouble(std::chrono::duration<double, std::milli>(end - start).count());
  }

  const auto response_value =
      Aws::Utils::Json::JsonValue()
          .WithArray("ms_durations", ms_durations)
          .WithInteger("num_s3_requests_tier_1", static_cast<size_t>(keys.GetLength() * batch_size))
          .WithInteger("num_s3_requests_tier_2", 0)
          .WithInt64("s3_storage_used_bytes", static_cast<int64_t>(num_bytes * keys.GetLength() * batch_size));

  return aws::lambda_runtime::invocation_response::success(response_value.View().WriteCompact(), "application/json");
}

}  // namespace skyrise

int main() {
  skyrise::FunctionWriteS3 function_write_s3;
  function_write_s3.HandleRequest();

  return 0;
}
