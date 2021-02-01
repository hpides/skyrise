#include "function_segments.hpp"

#include <algorithm>
#include <iomanip>
#include <tuple>

#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/xray/model/BatchGetTracesRequest.h>
#include <aws/xray/model/GetTraceSummariesRequest.h>

namespace skyrise {

std::map<Aws::String, std::set<Aws::String>> FunctionSegmentsAnalyzer::GetTraceIds(
    const std::vector<Aws::String>& function_names,
    const std::chrono::time_point<std::chrono::system_clock>& start_time,
    const std::chrono::time_point<std::chrono::system_clock>& end_time, size_t num_ids_expected) {
  std::map<Aws::String, std::set<Aws::String>> trace_ids;

  if (function_names.empty()) {
    return trace_ids;
  }

  for (const auto& function_name : function_names) {
    trace_ids.emplace(function_name, std::set<Aws::String>{});
  }

  Aws::XRay::Model::GetTraceSummariesRequest get_trace_summaries_request;

  for (size_t i = 0; i < kRetries; ++i) {
    Aws::String next_token;

    do {
      get_trace_summaries_request.WithStartTime(start_time).WithEndTime(end_time).WithNextToken(next_token);
      const auto outcome = client_.GetTraceSummaries(get_trace_summaries_request);

      if (!outcome.IsSuccess()) {
        AWS_LOGSTREAM_ERROR(kTag.c_str(), outcome.GetError().GetMessage());
        return trace_ids;
      }

      const auto trace_summaries = outcome.GetResult().GetTraceSummaries();
      next_token = outcome.GetResult().GetNextToken();

      for (const auto& trace_summary : trace_summaries) {
        if (trace_ids.find(trace_summary.GetEntryPoint().GetName()) != trace_ids.cend()) {
          trace_ids[trace_summary.GetEntryPoint().GetName()].emplace(trace_summary.GetId());
        }
      }

      num_scanned_traces_ += trace_summaries.size();
    } while (!next_token.empty());

    if (std::find_if(trace_ids.cbegin(), trace_ids.cend(), [&num_ids_expected](const auto& it) {
          return it.second.size() < num_ids_expected;
        }) == trace_ids.cend()) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(kSleepRetryMs));
  }

  return trace_ids;
}

std::map<Aws::String, Aws::XRay::Model::Trace> FunctionSegmentsAnalyzer::GetTraces(
    const std::vector<Aws::String>& trace_ids) {
  std::map<Aws::String, Aws::XRay::Model::Trace> traces;

  if (std::find_if(trace_ids.cbegin(), trace_ids.cend(), [](const auto& it) { return !it.empty(); }) ==
      trace_ids.cend()) {
    return traces;
  }

  std::vector<Aws::String> remaining_trace_ids(trace_ids);

  for (size_t i = 0; i < kRetries; i++) {
    Aws::XRay::Model::BatchGetTracesRequest batch_get_traces_request;
    batch_get_traces_request.WithTraceIds(remaining_trace_ids);

    const auto outcome = client_.BatchGetTraces(batch_get_traces_request);

    if (!outcome.IsSuccess()) {
      AWS_LOGSTREAM_ERROR(kTag.c_str(), outcome.GetError().GetMessage());
      return traces;
    }

    const auto batch_traces = outcome.GetResult().GetTraces();
    num_accessed_traces_ += batch_traces.size();

    for (const auto& batch_trace : batch_traces) {
      for (const auto& segment : batch_trace.GetSegments()) {
        Aws::Utils::Json::JsonValue document_json(segment.GetDocument());

        if (document_json.View().KeyExists("subsegments") && document_json.View().KeyExists("end_time")) {
          traces[batch_trace.GetId()] = batch_trace;
          remaining_trace_ids.erase(
              std::find(remaining_trace_ids.cbegin(), remaining_trace_ids.cend(), batch_trace.GetId()));
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

std::map<Aws::String, std::pair<std::chrono::duration<double>, std::chrono::duration<double>>>
FunctionSegmentsAnalyzer::GetSegments(const Aws::XRay::Model::Trace& trace) {
  std::map<Aws::String, std::pair<std::chrono::duration<double>, std::chrono::duration<double>>>
      unprocessed_lambda_segments;

  if (!trace.IdHasBeenSet()) {
    return unprocessed_lambda_segments;
  }

  for (const auto& segment : trace.GetSegments()) {
    Aws::Utils::Json::JsonValue document_json(segment.GetDocument());

    if (document_json.View().KeyExists("subsegments")) {
      const auto subsegments = document_json.View().GetArray("subsegments");

      for (size_t i = 0; i < subsegments.GetLength(); ++i) {
        const auto subsegment = subsegments.GetItem(i);
        unprocessed_lambda_segments.emplace(
            subsegment.GetString("name"),
            std::make_pair(std::chrono::duration<double>(subsegment.GetDouble("start_time")),
                           std::chrono::duration<double>(subsegment.GetDouble("end_time"))));
      }
    } else if (document_json.View().KeyExists("origin")) {
      unprocessed_lambda_segments.emplace(
          document_json.View().GetString("origin"),
          std::make_pair(std::chrono::duration<double>(document_json.View().GetDouble("start_time")),
                         std::chrono::duration<double>(document_json.View().GetDouble("end_time"))));
    }
  }
  return unprocessed_lambda_segments;
}

LambdaSegmentDurations FunctionSegmentsAnalyzer::CalculateLambdaSegmentDurations(
    const std::map<Aws::String, std::pair<std::chrono::duration<double>, std::chrono::duration<double>>>&
        unprocessed_lambda_segments,
    const std::chrono::time_point<std::chrono::system_clock>& start_time,
    const std::chrono::time_point<std::chrono::system_clock>& end_time) {
  auto lambda_segment_durations = CreateLambdaSegmentDurations();

  if (unprocessed_lambda_segments.find("AWS::Lambda") == unprocessed_lambda_segments.cend()) {
    return lambda_segment_durations;
  }

  const auto segment_duration = unprocessed_lambda_segments.at("AWS::Lambda");

  const auto lambda_start = segment_duration.first;
  const auto lambda_end = segment_duration.second;
  lambda_segment_durations["network_call"] = lambda_start - start_time.time_since_epoch();
  lambda_segment_durations["network_return"] = end_time.time_since_epoch() - lambda_end;

  for (const auto& [subsegment_name, subsegment_duration] : unprocessed_lambda_segments) {
    if (subsegment_name == "Initialization") {
      lambda_segment_durations["initialization_remainder"] = subsegment_duration.first - lambda_start;
      lambda_segment_durations["initialization"] = subsegment_duration.second - subsegment_duration.first;
    } else if (subsegment_name == "Overhead") {
      lambda_segment_durations["function_overhead"] = subsegment_duration.second - subsegment_duration.first;
    } else if (subsegment_name == "Invocation") {
      lambda_segment_durations["initialization_remainder"] =
          lambda_segment_durations["initialization_remainder"].count() > 0.0
              ? lambda_segment_durations["initialization_remainder"]
              : subsegment_duration.first - lambda_start;
      lambda_segment_durations["function_execution"] = subsegment_duration.second - subsegment_duration.first;
    }
  }

  lambda_segment_durations["total"] = end_time - start_time;
  lambda_segment_durations["function_total"] = lambda_end - lambda_start;
  lambda_segment_durations["network_total"] =
      lambda_segment_durations["network_call"] + lambda_segment_durations["network_return"];
  lambda_segment_durations["initialization_total"] =
      lambda_segment_durations["initialization_remainder"] + lambda_segment_durations["initialization"];
  lambda_segment_durations["function_remainder"] =
      lambda_end - (lambda_start + lambda_segment_durations["initialization_total"] +
                    lambda_segment_durations["function_execution"] + lambda_segment_durations["function_overhead"]);

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
LambdaSegmentDurations FunctionSegmentsAnalyzer::CreateLambdaSegmentDurations() {
  return {{"total", std::chrono::duration<double>(0)},
          {"network_total", std::chrono::duration<double>(0)},
          {"network_call", std::chrono::duration<double>(0)},
          {"network_return", std::chrono::duration<double>(0)},
          {"function_total", std::chrono::duration<double>(0)},
          {"initialization_total", std::chrono::duration<double>(0)},
          {"initialization", std::chrono::duration<double>(0)},
          {"initialization_remainder", std::chrono::duration<double>(0)},
          {"function_execution", std::chrono::duration<double>(0)},
          {"function_overhead", std::chrono::duration<double>(0)},
          {"function_remainder", std::chrono::duration<double>(0)}};
}

}  // namespace skyrise
