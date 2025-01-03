#pragma once

#include <optional>

#include <aws/core/Aws.h>
#include <aws/ec2/model/InstanceType.h>

#include "abstract_benchmark_result.hpp"
#include "ec2_benchmark_types.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

struct Ec2InstanceRunningState {
  std::optional<std::chrono::steady_clock::time_point> coldstart_running_timestamp = std::nullopt;
  std::optional<std::chrono::steady_clock::time_point> warmstart_running_timestamp = std::nullopt;
  double coldstart_running_state_duration_ms = 0;
  double warmstart_running_state_duration_ms = 0;
};

struct InstanceStateTransition {
  Ec2InstanceRunningState running_state;
  double coldstart_duration_ms = 0;
  std::optional<double> stop_duration_ms = std::nullopt;
  std::optional<double> warmstart_duration_ms = std::nullopt;
  std::optional<double> termination_duration_ms = std::nullopt;
};

struct Ec2BenchmarkInvocationResult {
  InstanceStateTransition instance_transitions;
  Aws::Utils::Json::JsonValue result_json;
};

struct Ec2BenchmarkRepetitionResult {
  Aws::EC2::Model::InstanceType instance_type;
  std::unordered_map<Aws::String, Ec2BenchmarkInvocationResult> invocation_results;
  std::optional<double> duration_ms;
  explicit Ec2BenchmarkRepetitionResult(Aws::EC2::Model::InstanceType type) : instance_type(type){};
};

class Ec2BenchmarkResult : public AbstractBenchmarkResult {
 public:
  Ec2BenchmarkResult(const Aws::EC2::Model::InstanceType& instance_type, const size_t concurrent_instance_count,
                     const size_t repetition_count);

  void RegisterInstanceBillingStarts(const size_t repetition, const Aws::String& instance_id,
                                     const std::chrono::steady_clock::time_point timestamp, const bool is_warmstart);
  void RegisterInstanceBillingStops(const size_t repetition, const Aws::String& instance_id,
                                    const std::chrono::steady_clock::time_point timestamp);
  void RegisterInstanceColdstart(const size_t repetition, const Aws::String& instance_id,
                                 const double coldstart_duration_ms);
  void RegisterInstanceStop(const size_t repetition, const Aws::String& instance_id, const double stop_duration_ms);
  void RegisterInstanceWarmstart(const size_t repetition, const Aws::String& instance_id,
                                 const double warmstart_duration_ms);
  void RegisterInstanceTermination(const size_t repetition, const Aws::String& instance_id,
                                   const double termination_duration_ms);

  bool ContainsInstanceColdstartDuration(const size_t repetition, const Aws::String& instance_id) const;
  bool ContainsInstanceStopDuration(const size_t repetition, const Aws::String& instance_id) const;
  bool ContainsInstanceWarmstartDuration(const size_t repetition, const Aws::String& instance_id) const;
  bool ContainsInstanceTerminationDuration(const size_t repetition, const Aws::String& instance_id) const;

  void FinalizeRepetition(const size_t repetition, const double duration_ms);
  void FinalizeResult(const double duration_ms);

  double GetBilledDurationMs() const;
  double GetDurationMs() const override;
  const std::vector<Ec2BenchmarkRepetitionResult>& GetRepetitionResults() const;
  long double CalculateWarmstartCost(const std::shared_ptr<CostCalculator>& cost_calculator);
  long double CalculateColdstartCost(const std::shared_ptr<CostCalculator>& cost_calculator);
  long double GetRepetitionWarmstartCost(const size_t repetition_index) const;
  long double GetRepetitionColdstartCost(const size_t repetition_index) const;

  // Returns true if every instance launch [in the given repetition] was registered.
  bool IsRepetitionComplete(const size_t repetition) const;
  bool IsResultComplete() const override;

  // Returns true if the [repetition] duration was set.
  bool IsRepetitionFinalized(const size_t repetition) const;
  bool IsResultFinalized() const;

  void SetInvocationResults(const Aws::Utils::Json::JsonValue& invocation_results, const size_t repetition_index);

 private:
  const double concurrent_instance_count_;
  std::vector<Ec2BenchmarkRepetitionResult> repetitions_;

  std::optional<double> duration_ms_;
  const Aws::EC2::Model::InstanceType instance_type_;

  std::vector<double> repetition_warmstart_costs_;
  std::vector<double> repetition_coldstart_costs_;
};

}  // namespace skyrise
