#pragma once

#include <aws/xray/XRayClient.h>

#include "benchmark_runner.hpp"

namespace skyrise {

using LambdaSegmentDurations = std::map<Aws::String, std::chrono::duration<double>>;

class FunctionSegmentsAnalyzer {
 public:
  FunctionSegmentsAnalyzer(const Aws::XRay::XRayClient& client)
      : client_(client), num_accessed_traces_(0), num_scanned_traces_(0){};

  std::map<Aws::String, std::set<Aws::String>> GetTraceIds(
      const std::vector<Aws::String>& function_names,
      const std::chrono::time_point<std::chrono::system_clock>& start_time,
      const std::chrono::time_point<std::chrono::system_clock>& end_time, size_t num_ids_expected = 1);
  std::map<Aws::String, Aws::XRay::Model::Trace> GetTraces(const std::vector<Aws::String>& trace_ids);

  static std::map<Aws::String, std::pair<std::chrono::duration<double>, std::chrono::duration<double>>> GetSegments(
      const Aws::XRay::Model::Trace& trace);
  static LambdaSegmentDurations CalculateLambdaSegmentDurations(
      const std::map<Aws::String, std::pair<std::chrono::duration<double>, std::chrono::duration<double>>>&
          unprocessed_lambda_segments,
      const std::chrono::time_point<std::chrono::system_clock>& start_time,
      const std::chrono::time_point<std::chrono::system_clock>& end_time);
  static LambdaSegmentDurations CreateLambdaSegmentDurations();

  size_t GetNumAccessedTraces() { return num_accessed_traces_; };
  size_t GetNumScannedTraces() { return num_scanned_traces_; };

 private:
  Aws::XRay::XRayClient client_;
  size_t num_accessed_traces_;
  size_t num_scanned_traces_;

  const size_t kRetries = 10;
  const size_t kSleepRetryMs = 1000;
  const Aws::String kTag = "SKYRISE/BENCHMARK/FUNCTION_SEGMENTS";
};

}  // namespace skyrise
