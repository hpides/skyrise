#pragma once

#include <vector>

#include "abstract_benchmark.hpp"
#include "ec2_benchmark_config.hpp"
#include "ec2_benchmark_result.hpp"

namespace skyrise {

struct Ec2InvocationBenchmarkParameters {
  size_t concurrent_invocation_count;
  Ec2InstanceType instance_type;
  size_t repetition_count;
};

class Ec2InvocationBenchmark : public AbstractBenchmark {
 public:
  Ec2InvocationBenchmark(const std::vector<size_t>& concurrent_invocation_counts,
                         const std::vector<Ec2InstanceType>& instance_types, size_t repetition_count);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) override;

 private:
  static Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<Ec2BenchmarkResult>& benchmark_result,
                                                          const Ec2InvocationBenchmarkParameters& parameters);

 protected:
  const Aws::String& Name() const override;

 private:
  std::vector<std::pair<std::shared_ptr<Ec2BenchmarkConfig>, Ec2InvocationBenchmarkParameters>> benchmark_configs_;
  std::vector<std::shared_ptr<Ec2BenchmarkResult>> benchmark_results_;
};

}  // namespace skyrise
