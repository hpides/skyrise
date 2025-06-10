#pragma once

#include <unordered_set>

#include <aws/xray/XRayClient.h>

namespace skyrise {

using LambdaSegmentDurations = std::map<Aws::String, std::chrono::duration<double>>;

class LambdaSegmentAnalyzer {
 public:
  explicit LambdaSegmentAnalyzer(std::shared_ptr<const Aws::XRay::XRayClient> xray_client)
      : xray_client_(std::move(xray_client)) {};

  std::map<Aws::String, std::unordered_set<Aws::String>> GetTraceIds(
      const std::vector<Aws::String>& function_names,
      const std::chrono::time_point<std::chrono::system_clock>& start_time,
      const std::chrono::time_point<std::chrono::system_clock>& end_time);
  std::map<Aws::String, Aws::XRay::Model::Trace> GetTraces(const std::vector<Aws::String>& trace_ids);

  static void FlattenSubsegments(
      const std::shared_ptr<std::map<Aws::String, Aws::Utils::Json::JsonValue>>& unprocessed_lambda_segments,
      const Aws::String& parent, const Aws::Utils::Json::JsonView& json);
  static std::map<Aws::String, Aws::Utils::Json::JsonValue> GetSegments(const Aws::XRay::Model::Trace& trace);
  static LambdaSegmentDurations CalculateLambdaSegmentDurations(
      const std::map<Aws::String, Aws::Utils::Json::JsonValue>& unprocessed_lambda_segments,
      const std::chrono::time_point<std::chrono::system_clock>& start_time,
      const std::chrono::time_point<std::chrono::system_clock>& end_time);
  static LambdaSegmentDurations CreateLambdaSegmentDurations();

  size_t GetNumAccessedTraces() const { return num_accessed_traces_; };
  size_t GetNumScannedTraces() const { return num_scanned_traces_; };

 private:
  const std::shared_ptr<const Aws::XRay::XRayClient> xray_client_;

  size_t num_accessed_traces_ = 0;
  size_t num_scanned_traces_ = 0;
};

}  // namespace skyrise
