#pragma once

#include <vector>

#include "abstract_benchmark.hpp"
#include "ec2_benchmark_config.hpp"
#include "ec2_benchmark_result.hpp"

namespace skyrise {

class Ec2StartupBenchmark : public AbstractBenchmark {
 public:
  Ec2StartupBenchmark(const std::vector<Ec2InstanceType>& instance_types,
                      const std::vector<size_t>& concurrent_instance_counts, size_t repetition_count,
                      const bool warmup_flag);

  const Aws::String& Name() const override;

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) override;

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<Ec2BenchmarkResult>& result,
                                                   const Ec2StartupBenchmarkParameters& parameters) const;

  std::vector<std::pair<std::shared_ptr<Ec2BenchmarkConfig>, Ec2StartupBenchmarkParameters>> benchmark_configs_;
  std::vector<std::shared_ptr<Ec2BenchmarkResult>> benchmark_results_;
};

}  // namespace skyrise
