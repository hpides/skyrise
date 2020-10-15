#include <aws/core/Aws.h>

#include "benchmark_runner.hpp"

namespace skyrise {

class Benchmark {
 public:
  Benchmark() = default;

  Benchmark(const Benchmark&) = delete;
  Benchmark& operator=(const Benchmark&) = delete;

  virtual ~Benchmark() = default;

  virtual Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<BenchmarkRunner>& benchmark_runner) = 0;

  // TODO(maltenbergert): Move parts of BenchmarkHelper here
};

}  // namespace skyrise
