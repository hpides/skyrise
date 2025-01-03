#pragma once

#include <string>

#include <cxxopts.hpp>

#include "abstract_benchmark.hpp"
#include "client/coordinator_client.hpp"
#include "metering/request_tracker/request_tracker.hpp"
#include "micro_benchmark/ec2/ec2_benchmark_runner.hpp"
#include "micro_benchmark/lambda/lambda_benchmark_runner.hpp"
#include "micro_benchmark/micro_benchmark_utils.hpp"
#include "system_benchmark/system_benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

class BenchmarkExecutable {
 public:
  explicit BenchmarkExecutable(const std::string& executable_name, const std::string& benchmark_name);

  cxxopts::OptionAdder& GetOptionAdder();
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,hicpp-avoid-c-arrays,modernize-avoid-c-arrays)
  cxxopts::ParseResult& GetParseResult(int argc, char* argv[]);

  const skyrise::CoordinatorClient& GetClient() const;

  std::shared_ptr<const skyrise::CostCalculator> GetCostCalculator() const;

  std::shared_ptr<skyrise::Ec2BenchmarkRunner> GetEc2BenchmarkRunner() const;
  std::shared_ptr<skyrise::LambdaBenchmarkRunner> GetLambdaBenchmarkRunner() const;

  void ExecuteBenchmark(const std::shared_ptr<skyrise::AbstractBenchmark>& benchmark);

 private:
  void InitializeClients();
  void DeinitializeClients();
  void PrintRequestSummary();

  cxxopts::Options cli_options_;
  cxxopts::OptionAdder cli_option_adder_;
  cxxopts::ParseResult cli_parse_result_;

  Aws::SDKOptions sdk_options_;

  std::shared_ptr<skyrise::CoordinatorClient> client_;

  std::shared_ptr<const skyrise::CostCalculator> cost_calculator_;

  std::shared_ptr<skyrise::Ec2BenchmarkRunner> ec2_benchmark_runner_;
  std::shared_ptr<skyrise::LambdaBenchmarkRunner> lambda_benchmark_runner_;
  std::shared_ptr<skyrise::SystemBenchmarkRunner> system_benchmark_runner_;
  std::shared_ptr<skyrise::RequestTracker> request_tracker_;
  bool enable_metering_ = false;
};
