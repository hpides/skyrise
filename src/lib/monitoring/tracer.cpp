#include "tracer.hpp"

#include <chrono>
#include <regex>

#include <aws/core/utils/logging/LogMacros.h>
#include <aws/xray/model/PutTraceSegmentsRequest.h>

#include "constants.hpp"
#include "utils/string.hpp"

namespace skyrise {

namespace {

const std::string kTraceIdRegex{"^Root=(.+);Parent=(.+);Sampled=(\\d)"};

}  // namespace

Tracer::Tracer(std::shared_ptr<const Aws::XRay::XRayClient> xray_client, const std::string& xray_trace_id,
               const SubqueryFragmentIdentifier& subquery_fragment_identifier)
    : xray_client_(std::move(xray_client)),
      subquery_fragment_identifier_(subquery_fragment_identifier),
      current_operator_id_("Undefined") {
  const std::regex trace_id_regex(kTraceIdRegex);
  std::smatch matches;

  if (std::regex_match(xray_trace_id, matches, trace_id_regex)) {
    trace_id_ = matches[1];
    parent_id_ = matches[2];
    is_sampled_ = matches[3];
  }

  if (is_sampled_ == "0") {
    AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "Function instance not sampled.");
  }
}

Tracer::~Tracer() {
  subsegments_[current_operator_id_].emplace_back(StageInformation{"OperatorExit", std::chrono::system_clock::now()});
  SendTrace();
}

void Tracer::EnterOperator(const std::string& operator_id) {
  if (!subsegments_.empty()) {
    subsegments_[current_operator_id_].emplace_back(StageInformation{"OperatorExit", std::chrono::system_clock::now()});
  }

  current_operator_id_ = operator_id;
}

void Tracer::EnterStage(const std::string& stage) {
  subsegments_[current_operator_id_].emplace_back(StageInformation{stage, std::chrono::system_clock::now()});
}

void Tracer::SendTrace() {
  Aws::XRay::Model::PutTraceSegmentsRequest put_trace_segments_request;

  for (const auto& [operator_id, subsegment_information] : subsegments_) {
    const auto operator_subsegment_id = RandomString(16, kCharacterSetHex);

    const Subsegment operator_subsegment{
        operator_subsegment_id,
        operator_id,
        std::chrono::duration<double>(subsegment_information.front().timestamp.time_since_epoch()).count(),
        std::chrono::duration<double>(subsegment_information.back().timestamp.time_since_epoch()).count(),
        trace_id_,
        parent_id_};

    for (size_t i = 0; i < subsegment_information.size() - 1; ++i) {
      const auto stage_subsegment_id = RandomString(16, kCharacterSetHex);

      const Subsegment stage_subsegment{
          stage_subsegment_id,
          subsegment_information[i].stage,
          std::chrono::duration<double>(subsegment_information[i].timestamp.time_since_epoch()).count(),
          std::chrono::duration<double>(subsegment_information[i + 1].timestamp.time_since_epoch()).count(),
          trace_id_,
          operator_subsegment_id};

      put_trace_segments_request.AddTraceSegmentDocuments(stage_subsegment.ToJson().View().WriteCompact());
    }

    put_trace_segments_request.AddTraceSegmentDocuments(
        operator_subsegment.ToJson()
            .WithObject("annotations", subquery_fragment_identifier_.ToJson())
            .View()
            .WriteCompact());
  }

  const auto outcome = xray_client_->PutTraceSegments(put_trace_segments_request);

  if (!outcome.IsSuccess()) {
    AWS_LOGSTREAM_ERROR(kWorkerTag.c_str(), outcome.GetError().GetMessage());
  }
}

}  // namespace skyrise
