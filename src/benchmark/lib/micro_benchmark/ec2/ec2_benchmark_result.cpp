#include "ec2_benchmark_result.hpp"

#include <numeric>

#include "utils/assert.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

namespace {

const double kEc2MinimumBillingDuration = 60000.00;

}  // namespace

Ec2BenchmarkResult::Ec2BenchmarkResult(const Aws::EC2::Model::InstanceType& instance_type,
                                       const size_t concurrent_instance_count, const size_t repetition_count)
    : concurrent_instance_count_(concurrent_instance_count),
      repetitions_(repetition_count, Ec2BenchmarkRepetitionResult(instance_type)),
      instance_type_(instance_type) {}

void Ec2BenchmarkResult::RegisterInstanceColdstart(const size_t repetition, const Aws::String& instance_id,
                                                   const double coldstart_duration_ms) {
  Assert(repetitions_[repetition].invocation_results[instance_id].instance_transitions.coldstart_duration_ms == 0,
         "Registered more instance coldstarts than expected.");
  repetitions_[repetition].invocation_results[instance_id].instance_transitions.coldstart_duration_ms =
      coldstart_duration_ms;
}

void Ec2BenchmarkResult::RegisterInstanceStop(const size_t repetition, const Aws::String& instance_id,
                                              const double stop_duration_ms) {
  Assert(!repetitions_[repetition].invocation_results[instance_id].instance_transitions.stop_duration_ms.has_value(),
         "Stop of this instance has been registered before.");
  repetitions_[repetition].invocation_results[instance_id].instance_transitions.stop_duration_ms = stop_duration_ms;
}

void Ec2BenchmarkResult::RegisterInstanceWarmstart(const size_t repetition, const Aws::String& instance_id,
                                                   const double warmstart_duration_ms) {
  Assert(
      !repetitions_[repetition].invocation_results[instance_id].instance_transitions.warmstart_duration_ms.has_value(),
      "Warmstart of this instance has been registered before.");
  repetitions_[repetition].invocation_results[instance_id].instance_transitions.warmstart_duration_ms =
      warmstart_duration_ms;
}

void Ec2BenchmarkResult::RegisterInstanceTermination(const size_t repetition, const Aws::String& instance_id,
                                                     const double termination_duration_ms) {
  Assert(!repetitions_[repetition]
              .invocation_results[instance_id]
              .instance_transitions.termination_duration_ms.has_value(),
         "Termination of this instance has been registered before.");
  repetitions_[repetition].invocation_results[instance_id].instance_transitions.termination_duration_ms =
      termination_duration_ms;
}

bool Ec2BenchmarkResult::ContainsInstanceColdstartDuration(const size_t repetition,
                                                           const Aws::String& instance_id) const {
  const auto& launch_durations = repetitions_[repetition].invocation_results;
  return launch_durations.find(instance_id) != launch_durations.cend();
}

bool Ec2BenchmarkResult::ContainsInstanceStopDuration(const size_t repetition, const Aws::String& instance_id) const {
  Assert(repetitions_[repetition].invocation_results.find(instance_id) !=
             repetitions_[repetition].invocation_results.cend(),
         "Startup of this instance has not been registered.");
  return repetitions_[repetition].invocation_results.at(instance_id).instance_transitions.stop_duration_ms.has_value();
}

bool Ec2BenchmarkResult::ContainsInstanceWarmstartDuration(const size_t repetition,
                                                           const Aws::String& instance_id) const {
  Assert(repetitions_[repetition].invocation_results.find(instance_id) !=
             repetitions_[repetition].invocation_results.cend(),
         "Startup of this instance has not been registered.");
  return repetitions_[repetition]
      .invocation_results.at(instance_id)
      .instance_transitions.warmstart_duration_ms.has_value();
}

bool Ec2BenchmarkResult::ContainsInstanceTerminationDuration(const size_t repetition,
                                                             const Aws::String& instance_id) const {
  Assert(repetitions_[repetition].invocation_results.find(instance_id) !=
             repetitions_[repetition].invocation_results.cend(),
         "Startup of this instance has not been registered.");
  return repetitions_[repetition]
      .invocation_results.at(instance_id)
      .instance_transitions.termination_duration_ms.has_value();
}

void Ec2BenchmarkResult::FinalizeRepetition(const size_t repetition, const double duration_ms) {
  Assert(IsRepetitionComplete(repetition), "Some instance transitions in this repetition have not been registered.");
  repetitions_[repetition].duration_ms = duration_ms;
}

void Ec2BenchmarkResult::FinalizeResult(const double duration_ms) {
  Assert(IsResultComplete(), "All repetitions must be finalized before the result can be finalized.");
  duration_ms_ = duration_ms;
}

double Ec2BenchmarkResult::GetDurationMs() const {
  Assert(IsResultFinalized(), "Result must be finalized to have a duration.");
  return duration_ms_.value();
}

bool Ec2BenchmarkResult::IsRepetitionComplete(const size_t repetition) const {
  for (const auto& [instance_id, transition_duration] : repetitions_[repetition].invocation_results) {
    if (!transition_duration.instance_transitions.termination_duration_ms.has_value()) {
      return false;
    }
  }

  return repetitions_[repetition].invocation_results.size() == concurrent_instance_count_;
}

bool Ec2BenchmarkResult::IsRepetitionFinalized(const size_t repetition) const {
  return IsRepetitionComplete(repetition) && repetitions_[repetition].duration_ms.has_value();
}

const std::vector<Ec2BenchmarkRepetitionResult>& Ec2BenchmarkResult::GetRepetitionResults() const {
  return repetitions_;
}

bool Ec2BenchmarkResult::IsResultComplete() const {
  for (size_t i = 0; i < repetitions_.size(); ++i) {
    if (!IsRepetitionFinalized(i)) {
      return false;
    }
  }

  return true;
}

bool Ec2BenchmarkResult::IsResultFinalized() const { return IsResultComplete() && duration_ms_.has_value(); }

long double Ec2BenchmarkResult::CalculateWarmstartCost(const std::shared_ptr<CostCalculator>& cost_calculator) {
  if (repetition_warmstart_costs_.empty()) {
    repetition_warmstart_costs_.reserve(repetitions_.size());
    for (const auto& repetition : repetitions_) {
      repetition_warmstart_costs_.push_back(0);
      for (const auto& transition_duration : repetition.invocation_results) {
        double& repetition_cost = repetition_warmstart_costs_.back();
        if (!transition_duration.second.instance_transitions.warmstart_duration_ms.has_value()) {
          repetition_cost += 0;
          continue;
        }
        repetition_cost += cost_calculator->CalculateCostEc2(
            transition_duration.second.instance_transitions.running_state.warmstart_running_state_duration_ms,
            instance_type_);
      }
    }
  }

  return std::accumulate(repetition_warmstart_costs_.begin(), repetition_warmstart_costs_.end(), 0.0);
}

long double Ec2BenchmarkResult::CalculateColdstartCost(const std::shared_ptr<CostCalculator>& cost_calculator) {
  if (repetition_coldstart_costs_.empty()) {
    repetition_coldstart_costs_.reserve(repetitions_.size());
    for (const auto& repetition : repetitions_) {
      repetition_coldstart_costs_.push_back(0);
      for (const auto& transition_duration : repetition.invocation_results) {
        double& repetition_cost = repetition_coldstart_costs_.back();
        repetition_cost += cost_calculator->CalculateCostEc2(
            transition_duration.second.instance_transitions.running_state.coldstart_running_state_duration_ms,
            instance_type_);
      }
    }
  }

  return std::accumulate(repetition_coldstart_costs_.begin(), repetition_coldstart_costs_.end(), 0.0);
}

long double Ec2BenchmarkResult::GetRepetitionWarmstartCost(const size_t repetition_index) const {
  Assert(!repetition_warmstart_costs_.empty(), "Costs must be calculated before.");
  return repetition_warmstart_costs_[repetition_index];
}

long double Ec2BenchmarkResult::GetRepetitionColdstartCost(const size_t repetition_index) const {
  Assert(!repetition_coldstart_costs_.empty(), "Costs must be calculated before.");
  return repetition_coldstart_costs_[repetition_index];
}

void Ec2BenchmarkResult::RegisterInstanceBillingStarts(const size_t repetition, const Aws::String& instance_id,
                                                       const std::chrono::steady_clock::time_point timestamp,
                                                       const bool is_warmstart) {
  Ec2InstanceRunningState& running_state =
      repetitions_[repetition].invocation_results[instance_id].instance_transitions.running_state;
  if (is_warmstart && !running_state.warmstart_running_timestamp.has_value()) {
    repetitions_[repetition]
        .invocation_results[instance_id]
        .instance_transitions.running_state.warmstart_running_timestamp = timestamp;
  } else if (!running_state.coldstart_running_timestamp.has_value()) {
    repetitions_[repetition]
        .invocation_results[instance_id]
        .instance_transitions.running_state.coldstart_running_timestamp = timestamp;
  }
}

void Ec2BenchmarkResult::RegisterInstanceBillingStops(const size_t repetition, const Aws::String& instance_id,
                                                      const std::chrono::steady_clock::time_point timestamp) {
  Ec2InstanceRunningState& running_state =
      repetitions_[repetition].invocation_results[instance_id].instance_transitions.running_state;
  if (running_state.warmstart_running_timestamp.has_value()) {
    running_state.warmstart_running_state_duration_ms =
        std::chrono::duration<double, std::milli>(timestamp - running_state.warmstart_running_timestamp.value())
            .count();
    running_state.warmstart_running_timestamp = std::nullopt;
  } else {
    running_state.coldstart_running_state_duration_ms =
        std::chrono::duration<double, std::milli>(timestamp - running_state.coldstart_running_timestamp.value())
            .count();
  }
}

double Ec2BenchmarkResult::GetBilledDurationMs() const {
  double billed_duration = 0.0;
  for (const auto& repetition : repetitions_) {
    for (const auto& transition_duration : repetition.invocation_results) {
      billed_duration +=
          std::max(kEc2MinimumBillingDuration,
                   transition_duration.second.instance_transitions.running_state.coldstart_running_state_duration_ms);
      if (transition_duration.second.instance_transitions.warmstart_duration_ms.has_value()) {
        billed_duration +=
            std::max(kEc2MinimumBillingDuration,
                     transition_duration.second.instance_transitions.running_state.warmstart_running_state_duration_ms);
      }
    }
  }
  return billed_duration;
}

void Ec2BenchmarkResult::SetInvocationResults(const Aws::Utils::Json::JsonValue& invocation_results,
                                              const size_t repetition_index) {
  if (!invocation_results.View().KeyExists("invocations")) {
    return;
  }
  const auto invocations = invocation_results.View().GetArray("invocations");
  int index = 0;
  for (auto& [instance_id, invocation_result] : repetitions_[repetition_index].invocation_results) {
    invocation_result.result_json = invocations.GetItem(index).Materialize();
    index++;
  }
}

}  // namespace skyrise
