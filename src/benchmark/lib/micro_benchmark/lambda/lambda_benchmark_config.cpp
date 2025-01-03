#include "lambda_benchmark_config.hpp"

#include <memory>
#include <string>

#include <magic_enum/magic_enum.hpp>

#include "function/function_utils.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

LambdaBenchmarkConfig::LambdaBenchmarkConfig(const Aws::String& function_package_name,
                                             const Aws::String& s3_bucket_name, const Warmup warmup,
                                             const DistinctFunctionPerRepetition distinct_function_per_repetition,
                                             const enum EventQueue use_event_queue, const bool enable_tracing,
                                             const size_t function_memory_size_mb, const std::string& efs_arn,
                                             const size_t concurrent_instance_count, const size_t repetition_count,
                                             const std::vector<std::function<void()>>& after_repetition_callbacks,
                                             const bool enable_vpc)
    : AbstractBenchmarkConfig(concurrent_instance_count, repetition_count, after_repetition_callbacks),
      warmup_type_(warmup),
      distinct_function_per_repetition_(distinct_function_per_repetition),
      use_event_queue_(use_event_queue),
      enable_tracing_(enable_tracing) {
  Assert(after_repetition_callbacks_.size() == repetition_count_,
         "The number of repetition callbacks and the repetition count must be equal.");

  switch (warmup_type_) {
    case Warmup::kNo: {
      warmup_strategy_ = nullptr;
      break;
    }
    case Warmup::kYesOnInitialRepetition: {
      warmup_strategy_ = std::make_shared<ConfigurableWarmupStrategy>();
      break;
    }
    case Warmup::kYesOnEveryRepetition: {
      warmup_strategy_ = std::make_shared<ConfigurableWarmupStrategy>(false);
      break;
    }
    default:
      warmup_strategy_ = nullptr;
      break;
  }

  const bool is_local = function_package_name.find("S3_") != 0;
  const Aws::String function_path =
      is_local ? GetProjectDirectoryPath() + "pkg/" + function_package_name + ".zip" : s3_bucket_name;
  Aws::StringStream function_name_base;
  function_name_base << benchmark_timestamp_ << "-" << function_package_name << "-" << benchmark_id_;

  if (distinct_function_per_repetition_ == DistinctFunctionPerRepetition::kYes) {
    for (size_t i = 0; i < repetition_count_; ++i) {
      function_configs_.emplace_back(function_path, function_name_base.str() + "-" + std::to_string(i),
                                     function_memory_size_mb, is_local, enable_vpc, "", efs_arn);
    }
  } else {
    function_configs_.emplace_back(function_path, function_name_base.str(), function_memory_size_mb, is_local,
                                   enable_vpc, "", efs_arn);
  }

  auto empty_payload = std::make_shared<Aws::StringStream>();

  for (size_t i = 0; i < repetition_count_; ++i) {
    std::vector<LambdaInvocationConfig> invocation_configs;
    invocation_configs.reserve(concurrent_instance_count_);

    const Aws::String function_name = distinct_function_per_repetition_ == DistinctFunctionPerRepetition::kYes
                                          ? function_configs_[i].name
                                          : function_name_base.str();

    for (size_t j = 0; j < concurrent_instance_count_; ++j) {
      invocation_configs.emplace_back(LambdaInvocationConfig{
          function_name, function_name_base.str() + "-" + std::to_string(i) + "-" + std::to_string(j), empty_payload});
    }

    repetition_configs_.emplace_back(invocation_configs);
  }
}

Warmup LambdaBenchmarkConfig::GetWarmupType() const { return warmup_type_; }

EventQueue LambdaBenchmarkConfig::UseEventQueue() const { return use_event_queue_; }

std::shared_ptr<WarmupStrategy> LambdaBenchmarkConfig::GetWarmupStrategy() const { return warmup_strategy_; }

void LambdaBenchmarkConfig::SetPayloads(const std::vector<std::shared_ptr<Aws::IOStream>>& payloads) {
  Assert(payloads.size() == concurrent_instance_count_,
         "The number of payloads and the concurrent invocation count must be equal.");

  for (size_t i = 0; i < repetition_count_; ++i) {
    for (size_t j = 0; j < concurrent_instance_count_; ++j) {
      repetition_configs_[i][j].request_body = payloads[j];
    }
  }
}

void LambdaBenchmarkConfig::SetOnePayloadForAllFunctions(const std::shared_ptr<Aws::IOStream>& payload) {
  for (auto& repetition_config : repetition_configs_) {
    for (auto& invocation_config : repetition_config) {
      invocation_config.request_body = payload;
    }
  }
}

void LambdaBenchmarkConfig::SetWarmupStrategy(std::shared_ptr<WarmupStrategy> warmup_strategy) {
  warmup_strategy_ = std::move(warmup_strategy);
}

}  // namespace skyrise
