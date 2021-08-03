#pragma once

#include <string>

#include <cxxopts.hpp>

#include "benchmark.hpp"
#include "benchmark_helper.hpp"
#include "benchmark_runner.hpp"
#include "client/client.hpp"
#include "utils/costs/cost_calculator.hpp"

class BenchmarkExecutable {
 public:
  explicit BenchmarkExecutable(const std::string& executable_name, const std::string& benchmark_name);

  cxxopts::OptionAdder& GetOptionAdder();
  cxxopts::ParseResult& GetParseResult(int argc, char* argv[]);

  const skyrise::Client& GetClient() const;

  std::shared_ptr<const skyrise::CostCalculator> GetCostCalculator() const;
  std::shared_ptr<const skyrise::BenchmarkHelper> GetBenchmarkHelper() const;

  std::shared_ptr<skyrise::BenchmarkRunner> GetBenchmarkRunner() const;

  void ExecuteBenchmark(const std::shared_ptr<skyrise::Benchmark>& benchmark);

 private:
  void InitializeClients();
  void DeinitializeClients();

  cxxopts::Options cli_options_;
  cxxopts::OptionAdder cli_option_adder_;
  cxxopts::ParseResult cli_parse_result_;

  Aws::SDKOptions sdk_options_;

  std::shared_ptr<skyrise::Client> client_;

  std::shared_ptr<const skyrise::CostCalculator> cost_calculator_;
  std::shared_ptr<const skyrise::BenchmarkHelper> benchmark_helper_;

  std::shared_ptr<skyrise::BenchmarkRunner> benchmark_runner_;
};
