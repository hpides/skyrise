#pragma once

#include <memory>

#include <aws/core/utils/logging/LogMacros.h>

#include "abstract_benchmark_config.hpp"
#include "abstract_benchmark_result.hpp"
#include "constants.hpp"

namespace skyrise {
class AbstractBenchmarkRunner {
 public:
  AbstractBenchmarkRunner(const bool enable_metering, const bool enable_introspection)
      : enable_metering_(enable_metering), enable_introspection_(enable_introspection){};
  virtual ~AbstractBenchmarkRunner() = default;

  virtual std::shared_ptr<AbstractBenchmarkResult> RunConfig(const std::shared_ptr<AbstractBenchmarkConfig>& config) {
    config_ = config;
    try {
      Setup();
      auto result = OnRunConfig();
      Teardown();
      return result;
    } catch (const std::exception& e) {
      AWS_LOGSTREAM_ERROR(kBenchmarkTag.c_str(), e.what());
      Teardown();
      return nullptr;
    }
  }

  bool IsMeteringEnabled() const { return enable_metering_; }
  bool IsIntrospectionEnabled() const { return enable_introspection_; }

 protected:
  virtual void Setup() = 0;
  virtual void Teardown() = 0;
  virtual std::shared_ptr<AbstractBenchmarkResult> OnRunConfig() = 0;

  std::shared_ptr<AbstractBenchmarkConfig> config_;
  const bool enable_metering_;
  const bool enable_introspection_;
};

}  // namespace skyrise
