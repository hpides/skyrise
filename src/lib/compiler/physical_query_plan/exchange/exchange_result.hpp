#pragma once

#include <memory>
#include <optional>
#include <string>

#include "compiler/physical_query_plan/partition_operator_proxy.hpp"
#include "compiler/physical_query_plan/pipeline_fragment_template.hpp"
#include "types.hpp"

namespace skyrise {

struct ExchangeResult {
  explicit ExchangeResult(
      std::vector<PipelineFragmentDefinition> init_pipeline_fragment_definitions,
      std::vector<ObjectReference> init_target_objects, const size_t init_target_partition_count,
      const size_t init_target_worker_count,
      std::optional<std::shared_ptr<PartitionOperatorProxy>> init_pipeline_partition_proxy = std::nullopt)
      : pipeline_fragment_definitions(init_pipeline_fragment_definitions),
        target_objects(init_target_objects),
        target_partition_count(init_target_partition_count),
        target_worker_count(init_target_worker_count),
        pipeline_partition_proxy(init_pipeline_partition_proxy) {}

  /**
   * TODO(julianmenzler)
   */
  std::vector<PipelineFragmentDefinition> pipeline_fragment_definitions;
  std::vector<ObjectReference> target_objects;
  const size_t target_partition_count;
  const size_t target_worker_count;

  /**
   * TODO(julianmenzler)
   */
  std::optional<std::shared_ptr<PartitionOperatorProxy>> pipeline_partition_proxy;
};

}  // namespace skyrise
