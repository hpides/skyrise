#include "ec2_benchmark_result.hpp"

#include "utils/assert.hpp"

namespace skyrise {

Ec2BenchmarkResult::Ec2BenchmarkResult(const size_t repetition_count, const size_t invocation_count)
    : repetitions_(repetition_count, Ec2BenchmarkRepetition{}), invocation_count_(invocation_count) {}

void Ec2BenchmarkResult::RegisterInstanceLaunch(const Aws::String& instance_id, const double duration_ms,
                                                const size_t repetition) {
  Assert(repetitions_[repetition].launch_durations.size() < invocation_count_,
         "Detected more instance launches than allowed.");
  repetitions_[repetition].launch_durations.push_back(Ec2BenchmarkLaunchDuration{instance_id, duration_ms});
}

void Ec2BenchmarkResult::FinalizeRepetition(const double duration_ms, const size_t repetition) {
  Assert(IsRepetitionComplete(repetition), "Not all instances were registered.");
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
