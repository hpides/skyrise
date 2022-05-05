#include "ec2_benchmark_result.hpp"

#include "utils/assert.hpp"

namespace skyrise {

Ec2BenchmarkResult::Ec2BenchmarkResult(const size_t repetition_count, const size_t invocation_count)
    : repetitions_(repetition_count, Ec2BenchmarkRepetition{}), invocation_count_(invocation_count) {}

void Ec2BenchmarkResult::RegisterInstanceLaunch(const size_t repetition, const Aws::String& instance_id,
                                                const double duration_ms) {
  Assert(repetitions_[repetition].launch_durations.size() < invocation_count_,
         "Detected more instance launches than allowed.");
  repetitions_[repetition].launch_durations[instance_id] = Ec2BenchmarkLaunchDuration{duration_ms, std::nullopt};
}

void Ec2BenchmarkResult::UpdateCooldown(const size_t repetition, const Aws::String& instance_id,
                                        const double duration_ms) {
  Assert(!repetitions_[repetition].launch_durations[instance_id].cooldown_ms.has_value(),
         "Cooldown for this instance has been updated before.");
  repetitions_[repetition].launch_durations[instance_id].cooldown_ms = duration_ms;
}

bool Ec2BenchmarkResult::ContainsLaunchDuration(const size_t repetition, const Aws::String& instance_id) const {
  const auto& launch_durations = repetitions_[repetition].launch_durations;
  return launch_durations.find(instance_id) != launch_durations.cend();
}

bool Ec2BenchmarkResult::LaunchDurationHasCooldown(const size_t repetition, const Aws::String& instance_id) const {
  Assert(
      repetitions_[repetition].launch_durations.find(instance_id) != repetitions_[repetition].launch_durations.cend(),
      "Launch duration has not been registered.");
  return repetitions_[repetition].launch_durations.at(instance_id).cooldown_ms.has_value();
}

void Ec2BenchmarkResult::FinalizeRepetition(const size_t repetition, const double duration_ms) {
  Assert(IsRepetitionComplete(repetition), "Not all instances were registered and updated.");
  repetitions_[repetition].duration_ms = duration_ms;
}

void Ec2BenchmarkResult::FinalizeResult(const double duration_ms) {
  Assert(IsResultComplete(), "Result must contain all instance launches before it can be finalized.");
  duration_ms_ = duration_ms;
}

double Ec2BenchmarkResult::GetDurationMs() const {
  Assert(IsResultFinalized(), "Result must be finalized before it has a duration.");
  return duration_ms_.value();
}

bool Ec2BenchmarkResult::IsRepetitionComplete(const size_t repetition) const {
  for (const auto& [instance_id, launch_duration] : repetitions_[repetition].launch_durations) {
    if (!launch_duration.cooldown_ms.has_value()) {
      return false;
    }
  }

  return repetitions_[repetition].launch_durations.size() == invocation_count_;
}

bool Ec2BenchmarkResult::IsRepetitionFinalized(const size_t repetition) const {
  return IsRepetitionComplete(repetition) && repetitions_[repetition].duration_ms.has_value();
}

const std::vector<Ec2BenchmarkRepetition>& Ec2BenchmarkResult::GetRepetitions() const { return repetitions_; }

bool Ec2BenchmarkResult::IsResultComplete() const {
  for (size_t i = 0; i < repetitions_.size(); i++) {
    if (!IsRepetitionFinalized(i)) {
      return false;
    }
  }

  return true;
}

bool Ec2BenchmarkResult::IsResultFinalized() const { return IsResultComplete() && duration_ms_.has_value(); }

}  // namespace skyrise
