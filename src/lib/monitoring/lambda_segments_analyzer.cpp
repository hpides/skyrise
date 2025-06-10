#include "lambda_segments_analyzer.hpp"

#include <algorithm>
#include <thread>

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/xray/model/BatchGetTracesRequest.h>
#include <aws/xray/model/GetTraceSummariesRequest.h>

#include "constants.hpp"

namespace skyrise {

namespace {

constexpr size_t kRetries = 10;
constexpr size_t kSleepRetryMs = 1000;

}  // namespace

std::map<Aws::String, std::unordered_set<Aws::String>> LambdaSegmentAnalyzer::GetTraceIds(
    const std::vector<Aws::String>& function_names,
    const std::chrono::time_point<std::chrono::system_clock>& start_time,
    const std::chrono::time_point<std::chrono::system_clock>& end_time) {
  std::map<Aws::String, std::unordered_set<Aws::String>> trace_ids;

  if (function_names.empty()) {
    return trace_ids;
  }

  for (const auto& function_name : function_names) {
    trace_ids.emplace(function_name, std::unordered_set<Aws::String>());
  }

  Aws::XRay::Model::GetTraceSummariesRequest get_trace_summaries_request;

  for (size_t i = 0; i < kRetries; ++i) {
    Aws::String next_token;

    do {
      get_trace_summaries_request.WithStartTime(start_time).WithEndTime(end_time).WithNextToken(next_token);
      const auto outcome = xray_client_->GetTraceSummaries(get_trace_summaries_request);

      if (!outcome.IsSuccess()) {
        AWS_LOGSTREAM_ERROR(kBenchmarkTag.c_str(), outcome.GetError().GetMessage());
        return trace_ids;
      }

      const auto trace_summaries = outcome.GetResult().GetTraceSummaries();
      next_token = outcome.GetResult().GetNextToken();

      for (const auto& trace_summary : trace_summaries) {
        trace_ids[trace_summary.GetEntryPoint().GetName()].emplace(trace_summary.GetId());
      }

      num_scanned_traces_ += trace_summaries.size();
    } while (!next_token.empty());

    if (std::ranges::find_if(trace_ids, [](const auto& it) { return !it.second.empty(); }) == trace_ids.end()) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepRetryMs));
  }

  return trace_ids;
}

std::map<Aws::String, Aws::XRay::Model::Trace> LambdaSegmentAnalyzer::GetTraces(
    const std::vector<Aws::String>& trace_ids) {
  std::map<Aws::String, Aws::XRay::Model::Trace> traces;

  if (std::ranges::find_if(trace_ids, [](const auto& it) { return !it.empty(); }) == trace_ids.cend()) {
    return traces;
  }

  std::vector<Aws::String> remaining_trace_ids(trace_ids);

  for (size_t i = 0; i < kRetries; ++i) {
    Aws::XRay::Model::BatchGetTracesRequest batch_get_traces_request;
    batch_get_traces_request.WithTraceIds(remaining_trace_ids);

    const auto outcome = xray_client_->BatchGetTraces(batch_get_traces_request);

    if (!outcome.IsSuccess()) {
      AWS_LOGSTREAM_ERROR(kBenchmarkTag.c_str(), outcome.GetError().GetMessage());
      return traces;
    }

    const auto batch_traces = outcome.GetResult().GetTraces();
    num_accessed_traces_ += batch_traces.size();

    for (const auto& batch_trace : batch_traces) {
      for (const auto& segment : batch_trace.GetSegments()) {
        const Aws::Utils::Json::JsonValue document_json(segment.GetDocument());

        if (document_json.View().KeyExists("subsegments") && document_json.View().KeyExists("end_time")) {
          traces[batch_trace.GetId()] = batch_trace;
          remaining_trace_ids.erase(std::ranges::find(remaining_trace_ids, batch_trace.GetId()));
          break;
        }
      }
    }

    if (remaining_trace_ids.empty()) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepRetryMs));
  };

  return traces;
}

void LambdaSegmentAnalyzer::FlattenSubsegments(
    const std::shared_ptr<std::map<Aws::String, Aws::Utils::Json::JsonValue>>& unprocessed_lambda_segments,
    const Aws::String& parent, const Aws::Utils::Json::JsonView& json) {
  if (json.KeyExists("subsegments")) {
    const auto subsegments_array = json.GetArray("subsegments");

    for (size_t i = 0; i < subsegments_array.GetLength(); ++i) {
      const auto item = subsegments_array.GetItem(i);
      unprocessed_lambda_segments->emplace(
          (parent.empty() || parent == "Invocation" ? "" : parent + "_") + item.GetString("name"), item.Materialize());

      FlattenSubsegments(unprocessed_lambda_segments, item.GetString("name"), subsegments_array.GetItem(i));
    }
  }
}

std::map<Aws::String, Aws::Utils::Json::JsonValue> LambdaSegmentAnalyzer::GetSegments(
    const Aws::XRay::Model::Trace& trace) {
  const auto unprocessed_lambda_segments = std::make_shared<std::map<Aws::String, Aws::Utils::Json::JsonValue>>();

  if (!trace.IdHasBeenSet()) {
    return *unprocessed_lambda_segments;
  }

  for (const auto& segment : trace.GetSegments()) {
    const Aws::Utils::Json::JsonValue document_json(segment.GetDocument());

    FlattenSubsegments(unprocessed_lambda_segments, "", document_json.View());

    if (document_json.View().KeyExists("origin")) {
      unprocessed_lambda_segments->emplace(document_json.View().GetString("origin"), document_json);
    }
  }

  return *unprocessed_lambda_segments;
}

LambdaSegmentDurations LambdaSegmentAnalyzer::CalculateLambdaSegmentDurations(
    const std::map<Aws::String, Aws::Utils::Json::JsonValue>& unprocessed_lambda_segments,
    const std::chrono::time_point<std::chrono::system_clock>& start_time,
    const std::chrono::time_point<std::chrono::system_clock>& end_time) {
  auto lambda_segment_durations = CreateLambdaSegmentDurations();

  if (unprocessed_lambda_segments.find("AWS::Lambda") == unprocessed_lambda_segments.cend()) {
    return lambda_segment_durations;
  }

  const auto segment_duration = unprocessed_lambda_segments.at("AWS::Lambda").View();

  const auto lambda_start = std::chrono::duration<double>(segment_duration.GetDouble("start_time"));
  const auto lambda_end = std::chrono::duration<double>(segment_duration.GetDouble("end_time"));
  lambda_segment_durations["network_call_latency_ms"] = lambda_start - start_time.time_since_epoch();
  lambda_segment_durations["network_return_latency_ms"] = end_time.time_since_epoch() - lambda_end;

  for (const auto& [subsegment_name, subsegment] : unprocessed_lambda_segments) {
    const auto subsegment_start = std::chrono::duration<double>(subsegment.View().GetDouble("start_time"));
    const auto subsegment_end = std::chrono::duration<double>(subsegment.View().GetDouble("end_time"));

    if (subsegment_name == "Initialization") {
      lambda_segment_durations["initialization_remainder_latency_ms"] = subsegment_start - lambda_start;
      lambda_segment_durations["initialization_latency_ms"] = subsegment_end - subsegment_start;
    } else if (subsegment_name == "Overhead") {
      lambda_segment_durations["function_overhead_latency_ms"] = subsegment_end - subsegment_start;
    } else if (subsegment_name == "Invocation") {
      lambda_segment_durations["initialization_remainder_latency_ms"] =
          lambda_segment_durations["initialization_remainder_latency_ms"].count() > 0.0
              ? lambda_segment_durations["initialization_remainder_latency_ms"]
              : subsegment_start - lambda_start;
      lambda_segment_durations["function_execution_latency_ms"] = subsegment_end - subsegment_start;
    } else if (subsegment_name.starts_with("AWS::")) {
      lambda_segment_durations[subsegment_name] = subsegment_end - subsegment_start;
    }
  }

  lambda_segment_durations["total_latency_ms"] = end_time - start_time;
  lambda_segment_durations["function_total_latency_ms"] = lambda_end - lambda_start;
  lambda_segment_durations["network_total_latency_ms"] =
      lambda_segment_durations["network_call_latency_ms"] + lambda_segment_durations["network_return_latency_ms"];
  lambda_segment_durations["initialization_total_latency_ms"] =
      lambda_segment_durations["initialization_remainder_latency_ms"] +
      lambda_segment_durations["initialization_latency_ms"];
  lambda_segment_durations["function_remainder_latency_ms"] =
      lambda_end - (lambda_start + lambda_segment_durations["initialization_total_latency_ms"] +
                    lambda_segment_durations["function_execution_latency_ms"] +
                    lambda_segment_durations["function_overhead_latency_ms"]);

  return lambda_segment_durations;
}

// Definition of function segments:
//  total: Duration between invocation and return of function
//  network_total: Duration between invocation and function start on AWS plus function end on AWS and return of function
//  network_call: Duration between invocation and function start on AWS
//  network_return: Duration between function end on AWS and return of function
//  function_total: Duration between function start on AWS and function end on AWS
//  initialization_total: Duration between function start on AWS and start of function execution
//  initialization: Duration of XRay initialization segment before function execution
//  initialization_remainder: Duration between function start on AWS and XRay initialization segment start
//  function_execution: Duration of XRay invocation segment
//  function_overhead: Duration of XRay overhead segment
//  function_remainder: Time not accounted for
// All other segments are created with our tracer class to monitor Skyrise operators and stages.
LambdaSegmentDurations LambdaSegmentAnalyzer::CreateLambdaSegmentDurations() {
  return {{"total_latency_ms", std::chrono::duration<double>(0)},
          {"network_total_latency_ms", std::chrono::duration<double>(0)},
          {"network_call_latency_ms", std::chrono::duration<double>(0)},
          {"network_return_latency_ms", std::chrono::duration<double>(0)},
          {"function_total_latency_ms", std::chrono::duration<double>(0)},
          {"initialization_total_latency_ms", std::chrono::duration<double>(0)},
          {"initialization_latency_ms", std::chrono::duration<double>(0)},
          {"initialization_remainder_latency_ms", std::chrono::duration<double>(0)},
          {"function_execution_latency_ms", std::chrono::duration<double>(0)},
          {"function_overhead_latency_ms", std::chrono::duration<double>(0)},
          {"function_remainder_latency_ms", std::chrono::duration<double>(0)}};
}

}  // namespace skyrise
