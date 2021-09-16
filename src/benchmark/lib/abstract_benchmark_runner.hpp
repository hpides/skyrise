#pragma once

#include <memory>

#include <aws/core/utils/logging/LogMacros.h>

#include "abstract_benchmark_config.hpp"
#include "abstract_benchmark_result.hpp"

namespace skyrise {
class AbstractBenchmarkRunner {
 public:
  virtual ~AbstractBenchmarkRunner() = default;

  virtual std::shared_ptr<AbstractBenchmarkResult> RunConfig(const std::shared_ptr<AbstractBenchmarkConfig>& config) {
    config_ = config;

    try {
      Setup();
      auto result = OnRunConfig();
      Teardown();

      return result;
    } catch (const std::exception& e) {
      AWS_LOGSTREAM_ERROR(kTag.c_str(), e.what());

      Teardown();
    }

    return nullptr;
  }

 protected:
  virtual void Setup() = 0;
  virtual void Teardown() = 0;
  virtual std::shared_ptr<AbstractBenchmarkResult> OnRunConfig() = 0;

  std::shared_ptr<AbstractBenchmarkConfig> config_;
  const std::string kTag = "SKYRISE/BENCHMARK/BENCHMARK_RUNNER";
};

}  // namespace skyrise
