#pragma once

#include <optional>

#include <aws/core/Aws.h>

#include "abstract_benchmark_result.hpp"

namespace skyrise {

struct Ec2BenchmarkLaunchDuration {
  double duration_ms;
  std::optional<double> cooldown_ms;
};

struct Ec2BenchmarkRepetition {
  std::unordered_map<Aws::String, Ec2BenchmarkLaunchDuration> launch_durations;
  std::optional<double> duration_ms;
};

class Ec2BenchmarkResult : public AbstractBenchmarkResult {
 public:
  Ec2BenchmarkResult(const size_t repetition_count, const size_t invocation_count);

  void RegisterInstanceLaunch(const size_t repetition, const Aws::String& instance_id, const double duration_ms);
  void UpdateCooldown(const size_t repetition, const Aws::String& instance_id, const double duration_ms);

  bool ContainsLaunchDuration(const size_t repetition, const Aws::String& instance_id) const;
  bool LaunchDurationHasCooldown(const size_t repetition, const Aws::String& instance_id) const;

  void FinalizeRepetition(const size_t repetition, const double duration_ms);
  void FinalizeResult(const double duration_ms);

  double GetDurationMs() const override;
  const std::vector<Ec2BenchmarkRepetition>& GetRepetitions() const;

  // Returns true if every instance launch [in the given repetition] was registered.
  bool IsRepetitionComplete(const size_t repetition) const;
  bool IsResultComplete() const override;

  // Returns true if the [repetition] duration was set.
  bool IsRepetitionFinalized(const size_t repetition) const;
  bool IsResultFinalized() const;

 private:
  std::vector<Ec2BenchmarkRepetition> repetitions_;
  const double invocation_count_;

  std::optional<double> duration_ms_;
};

}  // namespace skyrise
