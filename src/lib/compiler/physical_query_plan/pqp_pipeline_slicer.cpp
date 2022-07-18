#include "pqp_pipeline_slicer.hpp"

#include <algorithm>
#include <iomanip>
#include <numeric>

#include "compiler/plan_utils.hpp"
#include "pqp_utils.hpp"
#include "utils/assert.hpp"
#include "utils/vector.hpp"

namespace {
using namespace skyrise;  // NOLINT(google-build-using-namespace)

std::vector<std::vector<PipelineFragmentDefinition>> GetPipelineFragmentDefinitions(
    const std::shared_ptr<ImportOperatorProxy>& import_proxy, size_t max_fragment_count) {
  // Ideally, an PipelineFragmentDefinition contains a single object key. However, if the number of object keys exceeds
  // @param max_fragment_count, object keys must be scattered across the maximum number of PipelineFragmentDefinitions.
  size_t chunk_size = 1;
  if (import_proxy->ObjectReferences().size() > max_fragment_count) {
    const double res = static_cast<double>(import_proxy->ObjectReferences().size()) / max_fragment_count;
    const double ceiled = ceil(res);
    chunk_size = size_t(ceiled);
  }
  auto pipeline_object_keys_by_fragment = SplitVectorIntoChunks(import_proxy->ObjectReferences(), chunk_size);

  // Create import definition for each fragment
  std::vector<std::vector<PipelineFragmentDefinition>> pipeline_fragment_definitions;
  pipeline_fragment_definitions.reserve(pipeline_object_keys_by_fragment.size());
  for (auto& fragment_object_keys : pipeline_object_keys_by_fragment) {
    std::vector<PipelineFragmentDefinition> fragment_import_definitions;
    fragment_import_definitions.emplace_back(import_proxy->Identity(), import_proxy->BucketName(),
                                             std::move(fragment_object_keys));
    pipeline_fragment_definitions.emplace_back(fragment_import_definitions);
  }
  Assert(pipeline_fragment_definitions.size() <= max_fragment_count, "Expected lower number of import definitions.");
  return pipeline_fragment_definitions;
}

std::vector<std::string> GetPipelineExportKeys(const std::string& key_prefix, const std::string& key_suffix,
                                               size_t fragment_instance_count) {
  std::vector<std::string> export_keys;
  export_keys.reserve(fragment_instance_count);

  if (fragment_instance_count == 1) {
    export_keys.emplace_back(key_prefix + "_result" + key_suffix);
    return export_keys;
  }

  for (size_t i = 1; i <= fragment_instance_count; ++i) {
    std::stringstream target_key;
    target_key << key_prefix;
    target_key << "_result";
    target_key << std::setfill('0') << std::setw(2) << i;
    target_key << key_suffix;
    export_keys.emplace_back(target_key.str());
  }

  return export_keys;
}

}  // namespace

namespace skyrise {

PqpPipelineSlicer::PqpPipelineSlicer(std::shared_ptr<AbstractOperatorProxy> pqp,
                                     std::shared_ptr<QueryContext> query_context)
    : pqp_(std::move(pqp)), query_context_(std::move(query_context)) {
  Assert(pqp_.use_count() == 1, "PqpPipelineSlicer should have exclusive ownership of the given PQP.");

  // Validate input PQP
  Assert(pqp_->Type() == OperatorType::kExport, "PQP root should have OperatorType::kExport.");
  std::vector<std::shared_ptr<AbstractOperatorProxy>> pqp_leaves = PqpFindLeaves(pqp_);
  for (const auto& pqp_leaf : pqp_leaves) {
    Assert(pqp_leaf->Type() == OperatorType::kImport, "PQP leaf should have OperatorType::kImport.");
    Assert(pqp_leaf->OutputNodeCount() == 1, "PQP leaf should have a single output only.");
  }
}

const std::vector<std::shared_ptr<PqpPipeline>>& PqpPipelineSlicer::GetPipelines() {
  std::vector<std::shared_ptr<AbstractOperatorProxy>> pqp_leaves;

  while (pqp_) {
    pqp_leaves = PqpFindLeaves(pqp_);
    std::vector<std::shared_ptr<ImportOperatorProxy>> consumed_imports;  // TODO(julianmenzler): Rename?
    // For each PQP leaf, cut off next pipeline, if possible.
    for (const auto& pqp_leaf : pqp_leaves) {
      Assert(pqp_leaf->Type() == OperatorType::kImport, "Leaf operator proxy should have OperatorType::kImport.");
      Assert(pqp_leaf->InputNodeCount() == 0, "Leaf operator proxy should have no inputs.");
      const auto import_proxy = std::static_pointer_cast<ImportOperatorProxy>(pqp_leaf);

      if (std::find(consumed_imports.cbegin(), consumed_imports.cend(), import_proxy) != consumed_imports.end()) {
        // Import is already part of another pipeline.
        continue;
      }

      std::shared_ptr<PqpPipeline> pipeline = CutNextPipelineFragment(import_proxy, consumed_imports);
      if (pipeline) {
        pipelines_.emplace_back(std::move(pipeline));
      }
    }
  }

  return pipelines_;
}

std::shared_ptr<PqpPipeline> PqpPipelineSlicer::CutNextPipelineFragment(  // TODO(julianmenzler): CutOffNextPipeline
    const std::shared_ptr<ImportOperatorProxy>& primary_import_proxy,
    std::vector<std::shared_ptr<ImportOperatorProxy>>& consumed_imports) {
  consumed_imports.push_back(primary_import_proxy);
  std::vector<std::shared_ptr<ImportOperatorProxy>> secondary_import_proxies;

  /**
   * (1) CHECK FOR PIPELINE PREDECESSOR
   */
  std::vector<std::shared_ptr<PqpPipeline>> current_pipeline_predecessors;
  {
    auto pipeline_predecessor = FindPipelinePredecessor(primary_import_proxy);
    if (pipeline_predecessor) {
      current_pipeline_predecessors.emplace_back(std::move(pipeline_predecessor));
    }
  }

  /**
   * (2) DETERMINE PIPELINE FRAGMENT BORDERS
   *      - The next pipeline fragment starts with @param primary_import_proxy.
   *      - Determine the end by going up the PQP tree. Find an operator proxy that either completes or terminates the
   *        pipeline fragment.
   *      - Track pipeline predecessors, in case of ImportOperatorProxy inputs.
   */
  std::shared_ptr<AbstractOperatorProxy> current_pipeline_fragment = primary_import_proxy;
  VisitPqpUpwards(primary_import_proxy, [&](const auto& operator_proxy) {
    Assert(operator_proxy->OutputNodeCount() < 2, "Operator proxy should have more than 1 output.");

    if (operator_proxy->Type() == OperatorType::kExchange || operator_proxy->Type() == OperatorType::kExport) {
      // DataExchange or Export operations complete pipeline fragments. Therefore, the upwards visitation can be
      // cancelled.
      current_pipeline_fragment = operator_proxy;
      return PqpUpwardVisitation::kDoNotVisitOutputs;
    }

    if (operator_proxy->InputNodeCount() == 2) {
      // Check whether all inputs are resolved.
      for (const auto& input_proxy : operator_proxy->Inputs()) {
        if (input_proxy == current_pipeline_fragment) {
          // Continue because input is going to be resolved as part of the current pipeline fragment.
          continue;
        }

        if (input_proxy->Type() != OperatorType::kImport) {
          // current_pipeline_fragment terminates because input_proxy must be resolved first (by another pipeline).
          current_pipeline_fragment = nullptr;
          return PqpUpwardVisitation::kDoNotVisitOutputs;
        }

        // Secondary imports from join or union operations
        const auto secondary_import_proxy = std::static_pointer_cast<ImportOperatorProxy>(input_proxy);
        consumed_imports.push_back(secondary_import_proxy);
        // The import can be the result of another pipeline.
        auto pipeline_predecessor = FindPipelinePredecessor(secondary_import_proxy);
        if (pipeline_predecessor) {
          current_pipeline_predecessors.emplace_back(pipeline_predecessor);
        }
        secondary_import_proxies.emplace_back(secondary_import_proxy);
      }
    }

    return PqpUpwardVisitation::kVisitOutputs;
  });

  if (!current_pipeline_fragment) {
    // Early Out:
    //  It was not possible to complete the pipeline fragment. Therefore, there is no result to return.
    return nullptr;
  }

  // Determine pipeline fragment type
  bool last_pipeline = false;
  if (current_pipeline_fragment->OutputNodeCount() == 0) {
    Assert(current_pipeline_fragment == pqp_, "Expected root of PQP.");
    Assert(current_pipeline_fragment->Type() == OperatorType::kExport, "PQP root should have OperatorType::kExport.");
    last_pipeline = true;
  }

  /**
   * (3) GENERATE PIPELINE IMPORT DEFINITIONS
   */

  // Level of intra-operator parallelism
  size_t worker_count = std::min(current_pipeline_fragment->InputObjectsCount(), query_context_->MaxWorkerCount());
  std::vector<std::vector<PipelineFragmentDefinition>> current_pipeline_fragment_definitions =
      GetPipelineFragmentDefinitions(primary_import_proxy, worker_count);

  // Secondary imports from joins or union operations
  for (const auto& secondary_import_proxy : secondary_import_proxies) {
    // ToDo(anyone): Currently, Skyrise has no join or union implementation. Therefore, we have no rule for
    //               mapping input objects to one another.
    //               For now, we use an implementation that can be used for broadcast joins:
    // Each pipeline fragment should contain the given import_proxy with all object keys
    const PipelineFragmentDefinition import_definition(
        secondary_import_proxy->Identity(), secondary_import_proxy->BucketName(), secondary_import_proxy->ObjectKeys());
    for (auto& fragment_import_definitions : current_pipeline_fragment_definitions) {
      fragment_import_definitions.emplace_back(import_definition);
    }
  }

  /**
   * (4) GENERATE PIPELINE EXPORT KEYS
   */
  std::string current_pipeline_identity = query_context_->GeneratePipelineIdentity();

  auto current_pipeline_export_format = ExportFormat::kOrc;
  std::stringstream pipeline_export_key_prefix_stream;
  std::string pipeline_export_key_suffix;
  if (last_pipeline) {
    // Final result export
    pipeline_export_key_prefix_stream << query_context_->FinalResultsKeyPrefix();
    pipeline_export_key_prefix_stream << current_pipeline_identity;
    pipeline_export_key_suffix = query_context_->TargetFileExtension();
    current_pipeline_export_format = query_context_->TargetFormat();
  } else {
    // Intermediate result export
    pipeline_export_key_prefix_stream << query_context_->IntermediateResultsKeyPrefix();
    pipeline_export_key_prefix_stream << current_pipeline_identity;
    pipeline_export_key_suffix = ".orc";
  }

  // Generate an export key for each fragment instance
  std::vector<std::string> current_pipeline_export_keys =
      GetPipelineExportKeys(pipeline_export_key_prefix_stream.str(), pipeline_export_key_suffix,
                            current_pipeline_fragment_definitions.size());

  /**
   * (5) CUT OFF PIPELINE PLAN
   */
  if (!last_pipeline) {
    Assert(current_pipeline_fragment->Type() == OperatorType::kExchange,
           "Current pipeline fragment root should have OperatorType::kExchange");

    // Create ImportOperatorProxy above ExchangeOperatorProxy
    auto next_pipeline_import_column_ids = std::vector<ColumnId>(current_pipeline_fragment->OutputColumnsCount());
    std::iota(next_pipeline_import_column_ids.begin(), next_pipeline_import_column_ids.end(), ColumnId{0});
    auto next_pipeline_import_proxy = ImportOperatorProxy::Make(
        query_context_->TargetBucketName(), current_pipeline_export_keys, next_pipeline_import_column_ids);
    next_pipeline_import_proxy->PrefixIdentity(query_context_->QueryIdentity());
    // Set current pipeline's identity as a comment, so that it can be resolved as a predecessor by succeeding
    // pipelines.
    next_pipeline_import_proxy->SetComment(current_pipeline_identity);
    // Pass the output object count from the ExchangeOperatorProxy to the next pipeline
    next_pipeline_import_proxy->SetOutputObjectsCount(current_pipeline_fragment->OutputObjectsCount());
    PlanInsertNodeAbove<AbstractOperatorProxy>(current_pipeline_fragment, next_pipeline_import_proxy);

    // Cut off current_pipeline_fragment from PQP
    next_pipeline_import_proxy->SetLeftInput(nullptr);

    // Replace ExchangeOperatorProxy with ExportOperatorProxy placeholder
    auto export_proxy = ExportOperatorProxy::Dummy();
    export_proxy->PrefixIdentity(query_context_->QueryIdentity());
    ReplacePlanNode<AbstractOperatorProxy>(current_pipeline_fragment, export_proxy);
    current_pipeline_fragment = export_proxy;
    Assert(current_pipeline_fragment->OutputNodeCount() == 0, "Pipeline fragment should be cut off.");
  } else {
    // This pipeline is the last fragment of the PQP.
    pqp_ = nullptr;
  }

  /**
   * (6) CREATE PIPELINE
   */
  auto current_pipeline = std::make_shared<PqpPipeline>(current_pipeline_fragment);
  current_pipeline->SetIdentity(current_pipeline_identity);
  for (const auto& pipeline : current_pipeline_predecessors) {
    pipeline->SetAsPredecessorOf(current_pipeline);
  }
  // Define fragments
  for (size_t i = 0; i < current_pipeline_fragment_definitions.size(); ++i) {
    auto fragment_definition =
        PipelineFragmentDefinition(current_pipeline_fragment_definitions.at(i), query_context_->TargetBucketName(),
                                   current_pipeline_export_keys.at(i), current_pipeline_export_format);
    current_pipeline->DefineFragment(std::move(fragment_definition));
  }
  Assert(current_pipeline->FragmentCount() < query_context_->MaxWorkerCount(), "Pipeline fragment count is too high.");

  return current_pipeline;
}

std::shared_ptr<PqpPipeline> PqpPipelineSlicer::FindPipelinePredecessor(
    std::shared_ptr<ImportOperatorProxy> import_proxy) const {
  auto predecessor_pipeline_iter =
      std::find_if(pipelines_.cbegin(), pipelines_.cend(),
                   [&import_proxy](const auto& pipeline) { return (import_proxy->Comment() == pipeline->Identity()); });

  if (predecessor_pipeline_iter != pipelines_.cend()) {
    return *predecessor_pipeline_iter;
  }
  return nullptr;
}

}  // namespace skyrise
