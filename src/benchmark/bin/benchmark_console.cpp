#include <ctime>
#include <iostream>
#include <map>
#include <string>
#include <utility>

#include <aws/core/Aws.h>
#include <cxxopts.hpp>

#include "benchmark.hpp"
#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "client/client_aws.hpp"
#include "utils/array.hpp"
#include "utils/benchmark_helper.hpp"
#include "utils/filesystem.hpp"
#include "utils/git_metadata.hpp"
#include "utils/map.hpp"
#include "utils/random.hpp"
#include "utils/string.hpp"
#include "utils/time.hpp"
#include "utils/vector.hpp"

namespace skyrise {

// TODO(maltenbergert): Example for test purposes; remove and integrate all Benchmarks
class TestBenchmark : public Benchmark {
 public:
  explicit TestBenchmark(size_t /*test_count*/) {}
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<BenchmarkRunner>& /*benchmark_runner*/) override {
    Aws::Utils::Array<Aws::Utils::Json::JsonValue> results(2);
    results[0] = Aws::Utils::Json::JsonValue().WithDouble("key", 0.42);
    results[1] = Aws::Utils::Json::JsonValue().WithString("id", "myID");
    return results;
  }
};

}  // namespace skyrise

class BenchmarkRegistry {
 public:
  void RegisterBenchmark(const std::string& name, std::unique_ptr<skyrise::Benchmark> benchmark) {
    benchmarks_.emplace(name, std::move(benchmark));
  }

  [[nodiscard]] std::vector<std::string> GetRegisteredBenchmarkNames() const { return ExtractMapKeys(benchmarks_); }

  [[nodiscard]] const std::unique_ptr<skyrise::Benchmark>& GetBenchmark(const std::string& name) const {
    return benchmarks_.at(name);
  }

 private:
  std::map<std::string, std::unique_ptr<skyrise::Benchmark>> benchmarks_;
};

int main(int argc, char* argv[]) {
  try {
    // Parse the command line arguments
    cxxopts::Options cli_options("skyriseBenchmarkConsole", "Console for running Skyrise benchmarks");

    cxxopts::OptionAdder cli_options_adder = cli_options.add_options();
    cli_options_adder("output", "The output file <file.json>", cxxopts::value<std::string>());
    cli_options_adder("filter", "The benchmark filter <benchmark_1,...,benchmark_n>",
                      cxxopts::value<std::vector<std::string>>());
    cli_options_adder("shuffle", "Shuffle the benchmark execution order", cxxopts::value<bool>());
    cli_options_adder("help", "Print the usage overview", cxxopts::value<bool>());

    cli_options.parse_positional({"output"});
    cli_options.positional_help("OUTPUT");

    cxxopts::ParseResult cli_arguments = cli_options.parse(argc, argv);

    if (cli_arguments.count("help") > 0) {
      std::cout << cli_options.help();
      exit(0);
    }

    if (cli_arguments.count("output") == 0) {
      throw cxxopts::option_required_exception("OUTPUT");
    }

    // Initialize the clients
    Aws::SDKOptions sdk_options;

    Aws::InitAPI(sdk_options);

    const auto aws_client = std::make_shared<skyrise::ClientAws>();
    const auto benchmark_runner = std::make_shared<skyrise::BenchmarkRunner>(aws_client);

    // Register the benchmarks
    BenchmarkRegistry benchmark_registry;

    benchmark_registry.RegisterBenchmark("TestBenchmark", std::make_unique<skyrise::TestBenchmark>(42));

    // Filter the benchmarks (optional)
    std::vector<std::string> benchmark_names = benchmark_registry.GetRegisteredBenchmarkNames();
    if (cli_arguments.count("filter") > 0) {
      const auto filter_names = cli_arguments["filter"].as<std::vector<std::string>>();

      if (!skyrise::IsSubset(filter_names, benchmark_names)) {
        std::cout << "Option ‘filter’ has an invalid value\n\n"
                  << "Possible values are subsets of " << skyrise::VectorToString(benchmark_names, ", ") << "\n";
      }

      benchmark_names = filter_names;
    }

    // Shuffle the benchmarks (optional)
    if (cli_arguments.count("shuffle") > 0) {
      std::shuffle(benchmark_names.begin(), benchmark_names.end(), skyrise::RandomGenerator<std::mt19937>());
    }

    // Run the benchmarks
    Aws::Utils::Array<Aws::Utils::Array<Aws::Utils::Json::JsonValue>> benchmark_results(benchmark_names.size());

    for (size_t i = 0; i < benchmark_names.size(); ++i) {
      benchmark_results[i] = benchmark_registry.GetBenchmark(benchmark_names[i])->Run(benchmark_runner);
    }

    // Generate the output
    const auto output =
        Aws::Utils::Json::JsonValue()
            .WithObject("context", Aws::Utils::Json::JsonValue()
                                       .WithString("date", skyrise::GetFormattedTimestamp("%Y/%m/%d-%H:%M:%S"))
                                       .WithString("commit", GitMetadata::CommitSha1()))
            .WithArray("benchmarks", skyrise::FlattenArrays(&benchmark_results));

    // Save the output
    skyrise::WriteStringToFile(output.View().WriteReadable(), cli_arguments["output"].as<std::string>());

    // TODO(maltenbergert): Print a status report to the command line

    Aws::ShutdownAPI(sdk_options);

  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";
    exit(1);
  }

  return 0;
}
