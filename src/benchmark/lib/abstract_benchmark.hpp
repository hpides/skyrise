#pragma once

#include <aws/core/utils/json/JsonSerializer.h>

#include "abstract_benchmark_runner.hpp"
#include "types.hpp"

namespace skyrise {

class AbstractBenchmark : private Noncopyable {
 public:
  virtual ~AbstractBenchmark() = default;
  virtual const Aws::String& Name() const = 0;
  virtual Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) = 0;
};

}  // namespace skyrise
