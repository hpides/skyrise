#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
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
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

namespace skyrise {

struct OperationS3Result {
  double latency_ms;
  size_t num_bytes;
};

struct FunctionReadWriteS3Result {
  double get_object_duration_ms;
  double put_object_duration_ms;
  size_t num_s3_requests_tier_1;
  size_t num_s3_requests_tier_2;
  size_t s3_storage_used_bytes;
};

OperationS3Result GetObjectsS3(const Aws::S3::S3Client& client, const Aws::String& bucket,
                               const Aws::Utils::Array<Aws::Utils::Json::JsonView>& keys) {
  std::vector<Aws::S3::Model::GetObjectRequest> requests;
  requests.reserve(keys.GetLength());

  std::vector<Aws::S3::Model::GetObjectOutcomeCallable> get_object_callables;
  get_object_callables.reserve(keys.GetLength());

  for (size_t i = 0; i < keys.GetLength(); i++) {
    requests.emplace_back(Aws::S3::Model::GetObjectRequest().WithBucket(bucket).WithKey(keys[i].AsString()));
  }

  const auto start = std::chrono::steady_clock::now();

  for (const auto& request : requests) {
    get_object_callables.emplace_back(client.GetObjectCallable(request));
  }

  for (const auto& callable : get_object_callables) {
    callable.wait();
  }

  const auto end = std::chrono::steady_clock::now();

  const size_t num_bytes = std::accumulate(get_object_callables.begin(), get_object_callables.end(), 0,
                                           [](const size_t a, Aws::S3::Model::GetObjectOutcomeCallable& b) {
                                             return a + b.get().GetResult().GetContentLength();
                                           });

  return {std::chrono::duration<double, std::milli>(end - start).count(), num_bytes};
}

OperationS3Result PutObjectsS3(const Aws::S3::S3Client& client, const Aws::String& bucket_read,
                               const Aws::Utils::Array<Aws::Utils::Json::JsonView>& keys_read,
                               const Aws::String& bucket_write,
                               const Aws::Utils::Array<Aws::Utils::Json::JsonView>& keys_write) {
  std::vector<Aws::S3::Model::PutObjectRequest> requests;
  requests.reserve(keys_read.GetLength());

  std::vector<Aws::S3::Model::GetObjectResult> buffers;
  buffers.reserve(keys_read.GetLength());

  size_t num_bytes_read = 0;

  for (size_t i = 0; i < keys_read.GetLength(); i++) {
    auto outcome =
        client.GetObject(Aws::S3::Model::GetObjectRequest().WithBucket(bucket_read).WithKey(keys_read[i].AsString()));

    num_bytes_read += outcome.GetResult().GetContentLength();

    buffers.emplace_back(outcome.GetResultWithOwnership());

    auto put_request = Aws::S3::Model::PutObjectRequest().WithBucket(bucket_write).WithKey(keys_write[i].AsString());
    put_request.SetBody(std::make_shared<Aws::IOStream>(buffers[i].GetBody().rdbuf()));
    requests.emplace_back(put_request);
  }

  std::vector<Aws::S3::Model::PutObjectOutcomeCallable> put_object_callables;
  put_object_callables.reserve(keys_read.GetLength());

  const auto start = std::chrono::steady_clock::now();

  for (const auto& request : requests) {
    put_object_callables.emplace_back(client.PutObjectCallable(request));
  }

  for (const auto& callable : put_object_callables) {
    callable.wait();
  }

  const auto end = std::chrono::steady_clock::now();

  return {std::chrono::duration<double, std::milli>(end - start).count(), num_bytes_read};
}

Aws::Utils::Json::JsonValue CreateResponseJson(const FunctionReadWriteS3Result& result) {
  return Aws::Utils::Json::JsonValue()
      .WithDouble("get_object_duration_ms", result.get_object_duration_ms)
      .WithDouble("put_object_duration_ms", result.put_object_duration_ms)
      .WithInteger("num_s3_requests_tier_1", result.num_s3_requests_tier_1)
      .WithInteger("num_s3_requests_tier_2", result.num_s3_requests_tier_2)
      .WithInt64("s3_storage_used_bytes", result.s3_storage_used_bytes);
}

}  // namespace skyrise

aws::lambda_runtime::invocation_response HandlerFunction(const aws::lambda_runtime::invocation_request& request,
                                                         const Aws::S3::S3Client& s3_client) {
  const Aws::Utils::Json::JsonValue json_value(request.payload);
  const auto json_view = json_value.View();

  const bool is_warmup = json_view.GetBool("is_warmup");

  if (is_warmup) {
    const auto response_value = Aws::Utils::Json::JsonValue().WithBool("is_warmup", true);
    const auto response_view = response_value.View();
    return aws::lambda_runtime::invocation_response::success(response_view.WriteCompact(), "application/json");
  }

  const Aws::String s3_bucket_read = json_view.GetString("s3_bucket_read");
  const Aws::String s3_bucket_write = json_view.GetString("s3_bucket_write");
  const auto s3_keys_read = json_view.GetArray("s3_keys_read");
  const auto s3_keys_write = json_view.GetArray("s3_keys_write");

  if (s3_keys_read.GetLength() != s3_keys_write.GetLength()) {
    return aws::lambda_runtime::invocation_response::failure("s3KeysRead and s3KeysWrite must have the same length.",
                                                             "InputError");
  }

  auto get_results = skyrise::GetObjectsS3(s3_client, s3_bucket_read, s3_keys_read);
  auto put_results = skyrise::PutObjectsS3(s3_client, s3_bucket_read, s3_keys_read, s3_bucket_write, s3_keys_write);
  const skyrise::FunctionReadWriteS3Result result{get_results.latency_ms, put_results.latency_ms,
                                                  s3_keys_write.GetLength(), s3_keys_read.GetLength() * 2,
                                                  put_results.num_bytes};

  const auto response_value = CreateResponseJson(result);
  const auto response_view = response_value.View();
  return aws::lambda_runtime::invocation_response::success(response_view.WriteCompact(), "application/json");
}

int main() {
  Aws::SDKOptions options;
  options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
  options.loggingOptions.logger_create_fn = [] {
    return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_logger",
                                                                  Aws::Utils::Logging::LogLevel::Trace);
  };

  Aws::InitAPI(options);
  {
    Aws::Client::ClientConfiguration client_config;
    client_config.region = Aws::Environment::GetEnv("AWS_REGION");
    client_config.caFile = "/etc/pki/tls/certs/ca-bundle.crt";

    const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();

    Aws::S3::S3Client s3_client(credentials_provider, client_config,
                                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always, false);

    const auto handler_function = [&s3_client](const aws::lambda_runtime::invocation_request& request) {
      return HandlerFunction(request, s3_client);
    };

    aws::lambda_runtime::run_handler(handler_function);
  }
  Aws::ShutdownAPI(options);

  return 0;
}
