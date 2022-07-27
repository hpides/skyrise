#include "pqp_exchange_strategy.hpp"

namespace {
using namespace skyrise;  // NOLINT(google-build-using-namespace)

// std::vector<std::vector<PipelineFragmentDefinition>> GetPipelineFragmentDefinitions(
//     const std::shared_ptr<ImportOperatorProxy>& import_proxy, size_t max_fragment_count) {
//   // Ideally, an PipelineFragmentDefinition contains a single object key. However, if the number of object keys
//   exceeds
//   // @param max_fragment_count, object keys must be scattered across the maximum number of
//   PipelineFragmentDefinitions. size_t chunk_size = 1; if (import_proxy->ObjectReferences().size() >
//   max_fragment_count) {
//     const double res = static_cast<double>(import_proxy->ObjectReferences().size()) / max_fragment_count;
//     const double ceiled = ceil(res);
//     chunk_size = size_t(ceiled);
//   }
//   auto pipeline_object_keys_by_fragment = SplitVectorIntoChunks(import_proxy->ObjectReferences(), chunk_size);
//
//   // Create import definition for each fragment
//   std::vector<std::vector<PipelineFragmentDefinition>> pipeline_fragment_definitions;
//   pipeline_fragment_definitions.reserve(pipeline_object_keys_by_fragment.size());
//   for (auto& fragment_object_keys : pipeline_object_keys_by_fragment) {
//     std::vector<PipelineFragmentDefinition> fragment_import_definitions;
//     fragment_import_definitions.emplace_back(import_proxy->Identity(), import_proxy->BucketName(),
//                                              std::move(fragment_object_keys));
//     pipeline_fragment_definitions.emplace_back(fragment_import_definitions);
//   }
//   Assert(pipeline_fragment_definitions.size() <= max_fragment_count, "Expected lower number of import definitions.");
//   return pipeline_fragment_definitions;
// }
//
// std::vector<std::string> GetPipelineExportKeys(const std::string& key_prefix, const std::string& key_suffix,
//                                                size_t fragment_instance_count) {
//   std::vector<std::string> export_keys;
//   export_keys.reserve(fragment_instance_count);
//
//   if (fragment_instance_count == 1) {
//     export_keys.emplace_back(key_prefix + "_result" + key_suffix);
//     return export_keys;
//   }
//
//   for (size_t i = 1; i <= fragment_instance_count; ++i) {
//     std::stringstream target_key;
//     target_key << key_prefix;
//     target_key << "_result";
//     target_key << std::setfill('0') << std::setw(2) << i;
//     target_key << key_suffix;
//     export_keys.emplace_back(target_key.str());
//   }
//
//   return export_keys;
// }

}  // namespace

namespace skyrise {

AbstractExchangeStrategy::AbstractExchangeStrategy(const ExchangeStrategyType type, const size_t output_objects_count)
    : type_(type) {}

const ExchangeStrategyType AbstractExchangeStrategy::GetExchangeStrategyType() const { return type_; }

MergeExchangeStrategy::MergeExchangeStrategy(size_t output_objects_count)
    : AbstractExchangeStrategy(output_objects_count == 1 ? ExchangeStrategyType::kFullMerge
                                                         : ExchangeStrategyType::kPartialMerge),
      output_objects_count_(output_objects_count) {
  Assert(output_objects_count_ > 0, "Zero is an illegal count of output objects.");
}

size_t MergeExchangeStrategy::OutputObjectsCount(size_t /* input_objects_count */) const {
  return output_objects_count_;
}

size_t MergeExchangeStrategy::OutputPartitionsCount() const { return 1; }

ExchangeResult MergeExchangeStrategy::ComputeExchangeResult(
    const size_t pipeline_id, const std::shared_ptr<CompilationContext>& compilation_context,
    const std::vector<std::shared_ptr<ImportOperatorProxy>>& import_proxies) {

  /**
   * TODOs
   * 1) Use Interface in ExchangeProxy
   * 2) Use Interface in Pipeline Slicer
   * 2) Build ExchangeStrategy tests
   * 3) Implement this function
   */

  std::shared_ptr<ImportOperatorProxy> next_pipeline_import_proxy;
  std::vector<PipelineFragmentDefinition> fragment_definitions;
  return ExchangeResult(next_pipeline_import_proxy, fragment_definitions);
}

}  // namespace skyrise
