#include "storage_io_function.hpp"

#include <chrono>
#include <filesystem>
#include <mutex>
#include <thread>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/dynamodb/model/DescribeEndpointsRequest.h>
#include <aws/dynamodb/model/GetItemRequest.h>
#include <aws/dynamodb/model/PutItemRequest.h>
#include <aws/lambda-runtime/runtime.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <magic_enum/magic_enum.hpp>

#include "client/base_client.hpp"
#include "constants.hpp"
#include "micro_benchmark/lambda/lambda_benchmark_types.hpp"
#include "storage/backend/dynamodb_utils.hpp"
#include "storage/backend/errors.hpp"
#include "storage/backend/filesystem_storage.hpp"
#include "storage/backend/s3_storage.hpp"
#include "utils/concurrent/concurrent_queue.hpp"
#include "utils/string.hpp"
#include "utils/synchronize_instances.hpp"
#include "utils/time.hpp"

namespace skyrise {

aws::lambda_runtime::invocation_response StorageIoFunction::OnHandleRequest(
    const Aws::Utils::Json::JsonView& request) const {
  const std::string invocation_start_timestamp = GetFormattedTimestamp("%Y%m%dT%H%M%S");

  const int concurrent_instance_count = request.GetInteger("concurrent_instance_count");
  const size_t object_size_bytes = request.GetInteger("object_size_bytes");
  const int object_count = request.GetInteger("object_count");
  const auto operation_type = magic_enum::enum_cast<StorageOperation>(request.GetString("operation_type"));
  const auto storage_type = magic_enum::enum_cast<StorageSystem>(request.GetString("storage_type"));
  const std::string container_name = request.GetString("container_name");
  const std::string object_prefix = request.GetString("object_prefix");
  const std::string sqs_queue_url = request.GetString("sqs_queue_url");
  const int repetition = request.GetInteger("repetition");
  const int invocation = request.GetInteger("invocation");
  const int thread_count = 32;

  std::string object_content;
  if (operation_type == StorageOperation::kWrite) {
    // A call to RandomString for 100 MB takes over 10 seconds. Do not include this time in measurements.
    object_content = RandomString(object_size_bytes);
  }

  bool is_success = true;
  std::string error_message;
  std::string error_type;

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> request_latencies_ms(object_count);
  std::mutex array_mutex;
  ConcurrentQueue<std::future<StorageError>> work_queue;
  std::vector<std::thread> thread_pool(thread_count);

  const auto base_client = std::make_shared<BaseClient>();
  const auto dynamodb_client = base_client->GetDynamoDbClient();
  const auto s3_client = base_client->GetS3Client();

  // Coordinate the start of the benchmark across multiple invocations.
  if (!sqs_queue_url.empty() && concurrent_instance_count > 1) {
    AwaitInstances(sqs_queue_url, base_client->GetSqsClient(), concurrent_instance_count, "id");
  }

  const std::string measurement_start_timestamp = GetFormattedTimestamp("%Y%m%dT%H%M%S");
  auto measurement_start = std::chrono::system_clock::now();

  for (int i = 0; i < object_count; ++i) {
    const auto callback = [&, i]() {
      const std::string object_id = object_prefix + "/repetition-" + std::to_string(repetition) + "/invocation-" +
                                    std::to_string(invocation) + "/object-" + std::to_string(i);

      std::chrono::time_point<std::chrono::steady_clock> request_start;
      // DynamoDB.
      if (storage_type == StorageSystem::kDynamoDb) {
        DynamoDbItem item;
        item.emplace("key", Aws::DynamoDB::Model::AttributeValue().SetN(std::to_string(i)));
        const DynamoDbItem item_key = item;
        item.emplace("value", Aws::DynamoDB::Model::AttributeValue().SetS(object_content));
        if (operation_type == StorageOperation::kWrite) {
          request_start = std::chrono::steady_clock::now();
          Aws::DynamoDB::Model::PutItemRequest request;
          request.WithTableName(container_name).WithItem(item);
          const auto outcome = dynamodb_client->PutItem(request);
          if (!outcome.IsSuccess()) {
            const auto& error = outcome.GetError();
            return StorageError(StorageErrorType::kIOError, error.GetMessage());
          }
        } else if (operation_type == StorageOperation::kRead) {
          request_start = std::chrono::steady_clock::now();
          Aws::DynamoDB::Model::GetItemRequest request;
          request.WithTableName(container_name)
              .WithKey(item_key)
              .WithAttributesToGet({"value"})
              .WithConsistentRead(true);
          const auto outcome = dynamodb_client->GetItem(request);
          if (!outcome.IsSuccess()) {
            const auto& error = outcome.GetError();
            return StorageError(StorageErrorType::kIOError, error.GetMessage());
          }
          DynamoDbItem result = outcome.GetResult().GetItem();
        } else {
          return StorageError(StorageErrorType::kOperationNotSupported);
        }
        // EFS.
      } else if (storage_type == StorageSystem::kEfs) {
        std::filesystem::path path(kEfsMountPoint.data() + object_id);
        std::filesystem::create_directories(path.parent_path());
        if (operation_type == StorageOperation::kWrite) {
          std::ofstream stream;
          request_start = std::chrono::steady_clock::now();
          stream.open(path, std::ios::out | std::ios::binary);
          auto pos_before_write = stream.tellp();
          if (pos_before_write == std::ofstream::pos_type(-1)) {
            return StorageError(StorageErrorType::kIOError);
          }
          stream.write(object_content.c_str(), object_size_bytes);
          auto pos_after_write = stream.tellp();
          if (std::cmp_not_equal((pos_after_write - pos_before_write), object_size_bytes)) {
            return StorageError(StorageErrorType::kIOError);
          }
          stream.close();
        } else if (operation_type == StorageOperation::kRead) {
          const auto buffer = std::make_shared<ByteBuffer>(object_size_bytes);
          std::ifstream stream;
          request_start = std::chrono::steady_clock::now();
          stream.open(path, std::ios::in | std::ios::binary);
          if (!stream.is_open()) {
            return StorageError(StorageErrorType::kNotFound);
          }
          stream.clear();
          const std::ifstream::pos_type pos_before_read = stream.tellg();
          if (pos_before_read == std::ifstream::pos_type(-1)) {
            return StorageError(StorageErrorType::kIOError);
          }
          if (pos_before_read != std::ifstream::pos_type(0)) {
            stream.seekg(std::ifstream::pos_type(0));
          }
          if (!stream.good()) {
            return StorageError(StorageErrorType::kIOError);
          }
          buffer->Resize(object_size_bytes);
          stream.read(buffer->CharData(), object_size_bytes);
          if (std::cmp_not_equal(stream.gcount(), object_size_bytes) || !stream.good()) {
            return StorageError(StorageErrorType::kIOError);
          }
          stream.close();
        } else {
          return StorageError(StorageErrorType::kOperationNotSupported);
        }
        // S3.
      } else if (storage_type == StorageSystem::kS3) {
        if (operation_type == StorageOperation::kWrite) {
          const auto stream = std::make_shared<Aws::StringStream>(object_content);
          Aws::S3::Model::PutObjectRequest request;
          request.SetBucket(container_name);
          request.SetKey(object_id);
          request.SetBody(stream);
          request_start = std::chrono::steady_clock::now();
          const auto outcome = s3_client->PutObject(request);
          if (!outcome.IsSuccess()) {
            const auto& error = outcome.GetError();
            return StorageError(TranslateS3Error(error.GetErrorType()), error.GetMessage());
          }
        } else if (operation_type == StorageOperation::kRead) {
          const auto buffer = std::make_shared<ByteBuffer>(object_size_bytes);
          const auto stream = std::make_shared<DelegateStreamBuffer>();
          Aws::S3::Model::GetObjectRequest request;
          request.SetBucket(container_name);
          request.SetKey(object_id);
          request.SetResponseStreamFactory([buffer, stream]() {
            buffer->Resize(0);
            stream->Reset(buffer.get());
            return new std::iostream(stream.get());
          });
          request_start = std::chrono::steady_clock::now();
          const auto outcome = s3_client->GetObject(request);
          if (!outcome.IsSuccess()) {
            const auto& error = outcome.GetError();
            return StorageError(TranslateS3Error(error.GetErrorType()), error.GetMessage());
          }
        } else {
          return StorageError(StorageErrorType::kOperationNotSupported);
        }
      }
      Aws::Utils::Json::JsonValue request_latency = Aws::Utils::Json::JsonValue().AsDouble(
          std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - request_start).count());
      {
        std::lock_guard<std::mutex> guard(array_mutex);
        request_latencies_ms[i] = request_latency;
      }
      return StorageError::Success();
    };
    if (object_size_bytes > 1_KB) {
      work_queue.Push(std::async(std::launch::deferred, callback));
    } else {
      StorageError result = callback();
      if (result.GetType() != StorageErrorType::kNoError) {
        AWS_LOGSTREAM_ERROR(kBenchmarkTag.c_str(), result.GetMessage())
        error_message = result.GetMessage();
        error_type = std::string(magic_enum::enum_name(result.GetType()));
        is_success = false;
      }
    }
  }

  for (int j = 0; j < thread_count; ++j) {
    thread_pool.emplace_back([&]() {
      std::future<StorageError> work;
      bool has_work = work_queue.TryPop(&work);
      while (has_work) {
        const auto& result = work.get();

        if (result.GetType() != StorageErrorType::kNoError) {
          AWS_LOGSTREAM_ERROR(kBenchmarkTag.c_str(), result.GetMessage())
          error_message = result.GetMessage();
          error_type = std::string(magic_enum::enum_name(result.GetType()));
          is_success = false;
        }

        has_work = work_queue.TryPop(&work);
      }
    });
  }

  for (auto& thread : thread_pool) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  work_queue.Close();

  auto measurement_end = std::chrono::system_clock::now();
  double measurement_duration_ms =
      std::chrono::duration<double, std::milli>(measurement_end - measurement_start).count();
  const std::string measurement_end_timestamp = GetFormattedTimestamp("%Y%m%dT%H%M%S");

  if (is_success) {
    const auto response =
        Aws::Utils::Json::JsonValue()
            .WithArray("request_latencies_ms", request_latencies_ms)
            .WithString("invocation_start_timestamp", invocation_start_timestamp)
            .WithString("measurement_start_timestamp", measurement_start_timestamp)
            .WithDouble("measurement_start_ms", measurement_start.time_since_epoch() / std::chrono::milliseconds(1))
            .WithString("measurement_end_timestamp", measurement_end_timestamp)
            .WithDouble("measurement_end_ms", measurement_end.time_since_epoch() / std::chrono::milliseconds(1))
            .WithDouble("measurement_duration_ms", measurement_duration_ms);
    return aws::lambda_runtime::invocation_response::success(response.View().WriteCompact(), "application/json");
  } else {
    return aws::lambda_runtime::invocation_response::failure(error_message, error_type);
  }
}

}  // namespace skyrise

int main() {
  const skyrise::StorageIoFunction storage_io_function;
  storage_io_function.HandleRequest();

  return 0;
}
