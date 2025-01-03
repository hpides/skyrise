#pragma once

#include <string>

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/xray/XRayClient.h>

#include "monitoring/monitoring_types.hpp"
#include "types.hpp"

namespace skyrise {

struct StageInformation {
  std::string stage;
  std::chrono::time_point<std::chrono::system_clock> timestamp;
};

struct Subsegment {
  Aws::Utils::Json::JsonValue ToJson() const {
    Aws::Utils::Json::JsonValue subsegment;

    subsegment.WithString("id", id)
        .WithString("name", name)
        .WithDouble("start_time", start_time)
        .WithDouble("end_time", end_time)
        .WithString("trace_id", trace_id)
        .WithString("parent_id", parent_id)
        .WithString("type", "subsegment");

    return subsegment;
  }

  std::string id;
  std::string name;
  double start_time;
  double end_time;
  std::string trace_id;
  std::string parent_id;
};

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class Tracer {
 public:
  Tracer(std::shared_ptr<const Aws::XRay::XRayClient> xray_client, const std::string& xray_trace_id,
         const SubqueryFragmentIdentifier& subquery_fragment_identifier);
  ~Tracer();

  void EnterOperator(const std::string& operator_id);
  void EnterStage(const std::string& stage);

 private:
  void SendTrace();

  const std::shared_ptr<const Aws::XRay::XRayClient> xray_client_;
  const SubqueryFragmentIdentifier subquery_fragment_identifier_;

  std::string current_operator_id_;
  std::string trace_id_, parent_id_, is_sampled_;
  std::map<std::string, std::vector<StageInformation>> subsegments_;
};

}  // namespace skyrise
