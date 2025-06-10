#include "function.hpp"

#include <chrono>
#include <thread>

#include <aws/core/utils/base64/Base64.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <malloc.h>

#include "constants.hpp"
#include "metering/request_tracker/request_tracker.hpp"
#include "utils/assert.hpp"
#include "utils/compression.hpp"
#include "utils/profiling/function_host_information.hpp"
#include "utils/signal_handler.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

void Function::HandleRequest() const {
  RegisterSignalHandler();
  std::set_new_handler(MemoryAllocationExceptionHandler);

  Aws::SDKOptions sdk_options;
  const Aws::Utils::Logging::LogLevel log_level{Aws::Utils::Logging::LogLevel::Info};
  const Aws::Utils::Logging::LogLevel crt_log_level{Aws::Utils::Logging::LogLevel::Warn};
  sdk_options.httpOptions.installSigPipeHandler = true;
  sdk_options.loggingOptions.logLevel = log_level;
  sdk_options.loggingOptions.logger_create_fn = [log_level]() {
    return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_log_system", log_level);
  };
  sdk_options.loggingOptions.crt_logger_create_fn = [crt_log_level]() {
    return Aws::MakeShared<Aws::Utils::Logging::DefaultCRTLogSystem>("default_crt_log_system", crt_log_level);
  };

  auto run_handler_callback = [&](const aws::lambda_runtime::invocation_request& request) {
    std::string payload = request.payload;
    auto request_json = Aws::Utils::Json::JsonValue(payload);
    auto request_json_view = request_json.View();

    const bool is_url_request = request_json_view.KeyExists(kLambdaFunctionUrlParametersAttribute);
    if (is_url_request) {
      payload = request_json_view.GetObject(kLambdaFunctionUrlParametersAttribute).WriteReadable();
      request_json = Aws::Utils::Json::JsonValue(payload);
      request_json_view = request_json.View();
    }

    const bool is_compression_enabled = request_json_view.KeyExists(kRequestCompressedAttribute) &&
                                        request_json_view.KeyExists(kRequestDecompressedSizeAttribute);
    if (is_compression_enabled) {
      // Decompression involves three steps:
      // Decoding, moving the data to a string, decompression.
      // Decoding:
      std::string encoded_payload = request_json_view.GetString(kRequestCompressedAttribute);
      size_t decompressed_size = request_json_view.GetInt64(kRequestDecompressedSizeAttribute);
      auto buffer = Aws::Utils::Base64::Base64().Decode(encoded_payload);

      // Moving data to a string:
      std::string compressed_payload;
      compressed_payload.resize(buffer.GetSize());
      std::memcpy(compressed_payload.data(), buffer.GetUnderlyingData(), buffer.GetSize());
      // Decompression:
      payload = Decompress(compressed_payload, decompressed_size);
      request_json = Aws::Utils::Json::JsonValue(payload);
      request_json_view = request_json.View();
    }

    // If compression or anything else is used on the request, we have to make a non-const copy before changing it.
    auto request_copy = request;
    request_copy.payload = payload;

    const std::string response_sqs_queue = request_json_view.KeyExists(kRequestResponseQueueUrlAttribute)
                                               ? request_json_view.GetString(kRequestResponseQueueUrlAttribute)
                                               : "";
    const bool is_introspection_enabled = request_json_view.KeyExists(kRequestEnableIntrospection)
                                              ? request_json_view.GetBool(kRequestEnableIntrospection)
                                              : false;
    const bool is_metering_enabled =
        request_json_view.KeyExists(kRequestEnableMetering) ? request_json_view.GetBool(kRequestEnableMetering) : true;

    auto request_tracker = std::make_shared<RequestTracker>();
    if (is_metering_enabled) {
      request_tracker->Install(&sdk_options);
    }

    Aws::InitAPI(sdk_options);

    auto response = aws::lambda_runtime::invocation_response::failure("", "application/json");
    try {
      response = HandlerFunction(request_copy);
      /**
       * If HandlerFunction() returns a response that is a valid JSON object,
       * the response is extended by introspection and (optional) metering information.
       */
      Aws::Utils::Json::JsonValue response_json(response.get_payload());
      if (response_json.WasParseSuccessful()) {
        // Introspection.
        if (is_introspection_enabled) {
          FunctionHostInformationCollector collector;
          const auto environment_id = collector.CollectInformationIdentification().id;
          Aws::Utils::Json::JsonValue introspection_json;
          introspection_json.WithString("environment_id", environment_id);
          response_json.WithObject("introspection", introspection_json);
        }
        // The key kResponseMeteringAttribute contains nested keys with counts for every kind of request made with the
        // AWS SDK. The counts will get reset after each invocation.
        if (is_metering_enabled) {
          if (!response_sqs_queue.empty()) {
            // If we later send the response to SQS, we already have to add the SQS-request to the metering statistics
            // before creating the final response.
            request_tracker->RegisterRequestSucceeded("SQS", "SendMessage");
            request_tracker->RegisterRequestFinished("SQS", "SendMessage");
          }
          request_tracker->WriteSummaryToJson(&response_json);
          request_tracker->Reset();
        }
        response = aws::lambda_runtime::invocation_response(response_json.View().WriteCompact(),
                                                            response.get_content_type(), response.is_success());
      }
    } catch (const std::exception& exception) {
      Aws::Utils::Json::JsonValue message;
      message.WithString(kResponseMessageAttribute, exception.what()).WithInteger(kResponseIsSuccessAttribute, 0);
      response = aws::lambda_runtime::invocation_response::failure(message.View().WriteCompact(), "application/json");
    }

    if (!response_sqs_queue.empty()) {
      const auto outcome = Aws::SQS::SQSClient().SendMessage(Aws::SQS::Model::SendMessageRequest()
                                                                 .WithQueueUrl(response_sqs_queue)
                                                                 .WithMessageBody(response.get_payload()));
      DebugAssert(outcome.IsSuccess(), outcome.GetError().GetMessage());
    }

    /* We observe a growth in memory usage for consecutive invocations.
     We tracked this down to large chunks in the heap not being released properly after free(), since they are not on
     top of the heap. malloc_trim() takes care of this.
    */
    malloc_trim(0);
    return response;
  };

  {
    aws::lambda_runtime::run_handler(run_handler_callback);
  }
  Aws::ShutdownAPI(sdk_options);
  DeregisterSignalHandler();
}

void Function::MemoryAllocationExceptionHandler() {
  std::set_new_handler(nullptr);

  // We forward the output to stderr instead of AWS logging to prevent further dynamic memory allocation.
  std::cerr << kBaseTag << "\tMemory allocation failed\n";

  FunctionHostInformationCollector collector;
  const auto resource_usage = collector.CollectInformationResourceUsage();

  std::cerr << kBaseTag << "\tSystem Available Memory: " << KBToMB(resource_usage.system_available_memory_kb) << " MB"
            << "\tSystem Free Memory " << KBToMB(resource_usage.system_free_memory_kb) << " MB\n"
            << "\tProcess-used Memory: " << KBToMB(resource_usage.process_used_memory_kb) << " MB"
            << "\tProcess-used Memory in RAM: " << KBToMB(resource_usage.process_used_memory_in_ram_kb) << " MB";
}

aws::lambda_runtime::invocation_response Function::HandlerFunction(
    const aws::lambda_runtime::invocation_request& request) const {
  const auto json_value = Aws::Utils::Json::JsonValue(request.payload);
  const auto json_view = json_value.View();

  if (json_view.KeyExists("warmup") && json_view.KeyExists("concurrency_duration_seconds")) {
    std::this_thread::sleep_for(std::chrono::seconds(json_view.GetInteger("concurrency_duration_seconds")));
    return aws::lambda_runtime::invocation_response::success(json_view.WriteCompact(), "application/json");
  }

  return OnHandleRequest(json_view);
}

}  // namespace skyrise
