#pragma once

#include "benchmark_output.hpp"
#include "client/coordinator_client.hpp"
#include "ec2_benchmark_result.hpp"
#include "ec2_benchmark_types.hpp"

namespace skyrise {

class Ec2BenchmarkOutput : public BenchmarkOutput<Ec2BenchmarkParameters, Ec2BenchmarkResult,
                                                  Ec2BenchmarkRepetitionResult, Ec2BenchmarkInvocationResult> {
 public:
  Ec2BenchmarkOutput(Aws::String name, const Ec2BenchmarkParameters& parameters,
                     std::shared_ptr<Ec2BenchmarkResult> result);

  Aws::Utils::Json::JsonValue Build() const override;

 private:
  std::shared_ptr<CoordinatorClient> client_;
  std::shared_ptr<CostCalculator> cost_calculator_;
};

}  // namespace skyrise
