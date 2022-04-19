#pragma once

#include <optional>

#include <aws/core/Aws.h>

#include "abstract_benchmark_result.hpp"

namespace skyrise {

struct Ec2BenchmarkLaunchDuration {
  Aws::String instance_id;
  double duration_ms;
};

struct Ec2BenchmarkRepetition {
  std::vector<Ec2BenchmarkLaunchDuration> launch_durations;
  std::optional<double> duration_ms;
};

class Ec2BenchmarkResult : public AbstractBenchmarkResult {
 public:
  Ec2BenchmarkResult(const size_t repetition_count, const size_t invocation_count);

  void RegisterInstanceLaunch(const Aws::String& instance_id, const double duration_ms, const size_t repetition);
  // TODO(d-justen): Add a way to register the instance termination time as well.
  void FinalizeRepetition(const double duration_ms, const size_t repetition);
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
