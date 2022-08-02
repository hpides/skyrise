#include "pqp_pipeline_slicer.hpp"

#include <algorithm>
#include <iomanip>
#include <numeric>

#include "compiler/physical_query_plan/operator_proxy/exchange_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/plan_utils.hpp"
#include "pqp_utils.hpp"
#include "utils/assert.hpp"
#include "utils/vector.hpp"

namespace skyrise {

PqpPipelineSlicer::PqpPipelineSlicer(std::shared_ptr<AbstractOperatorProxy> pqp,
                                     std::shared_ptr<CompilationContext> query_context)
    : pqp_(std::move(pqp)), compilation_context_(std::move(query_context)) {
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
    std::vector<std::shared_ptr<ImportOperatorProxy>> consumed_import_proxies;
    // For each PQP leaf, cut off next pipeline, if possible.
    for (const auto& pqp_leaf : pqp_leaves) {
      Assert(pqp_leaf->Type() == OperatorType::kImport, "Leaf operator proxy should have OperatorType::kImport.");
      Assert(pqp_leaf->InputNodeCount() == 0, "Leaf operator proxy should have no inputs.");
      const auto import_proxy = std::static_pointer_cast<ImportOperatorProxy>(pqp_leaf);

      if (std::find(consumed_import_proxies.cbegin(), consumed_import_proxies.cend(), import_proxy) !=
          consumed_import_proxies.end()) {
        // Import is already part of another pipeline.
        continue;
      }

      std::shared_ptr<PqpPipeline> pipeline = TryCutOffNextPipeline(import_proxy, consumed_import_proxies);
      if (pipeline) {
        pipelines_.emplace_back(std::move(pipeline));
      }
    }
  }

  return pipelines_;
}

std::shared_ptr<PqpPipeline> PqpPipelineSlicer::TryCutOffNextPipeline(
    const std::shared_ptr<ImportOperatorProxy>& primary_import_proxy,
    std::vector<std::shared_ptr<ImportOperatorProxy>>& consumed_import_proxies) {
  std::vector<std::shared_ptr<ImportOperatorProxy>> current_pipeline_import_proxies;
  current_pipeline_import_proxies.push_back(primary_import_proxy);
  consumed_import_proxies.push_back(primary_import_proxy);

  /**
   * (1) CHECK FOR PIPELINE PREDECESSOR
   *      - The provided @param primary_import_proxy might be the result of a previous pipeline. In this case, track the
   *        according pipeline dependency.
   */
  std::vector<std::shared_ptr<PqpPipeline>> current_pipeline_predecessors;
  {
    auto pipeline_predecessor = FindPipelinePredecessor(primary_import_proxy);
    if (pipeline_predecessor) {
      current_pipeline_predecessors.emplace_back(std::move(pipeline_predecessor));
    }
  }

  /**
   * (2) DETERMINE PIPELINE PLAN BORDERS
   *      - The pipeline plan starts with @param primary_import_proxy.
   *      - Determine the end by going up the PQP. Find an operator proxy that either completes or terminates the
   *        pipeline plan.
   *      - Track pipeline predecessors, in case there are secondary import proxies in the pipeline plan.
   */
  std::shared_ptr<AbstractOperatorProxy> current_pipeline_plan = primary_import_proxy;
  VisitPqpUpwards(primary_import_proxy, [&](const auto& operator_proxy) {
    Assert(operator_proxy->OutputNodeCount() < 2, "Operator proxy should have more than 1 output.");

    if (operator_proxy->Type() == OperatorType::kExchange || operator_proxy->Type() == OperatorType::kExport) {
      // Since Exchange and Export operator proxies terminate pipeline plans, we cancel the upwards traversal.
      current_pipeline_plan = operator_proxy;
      return PqpUpwardVisitation::kDoNotVisitOutputs;
    }

    if (operator_proxy->InputNodeCount() == 2) {
      // Check whether all inputs are resolved.
      for (const auto& input_proxy : operator_proxy->Inputs()) {
        if (input_proxy == current_pipeline_plan) {
          // Continue because input is going to be resolved as part of the current pipeline plan.
          continue;
        }

        if (input_proxy->Type() != OperatorType::kImport) {
          // current_pipeline_plan terminates because input_proxy must be resolved first (by another pipeline).
          current_pipeline_plan = nullptr;
          return PqpUpwardVisitation::kDoNotVisitOutputs;
        }

        // Add input as a secondary import proxy of the current pipeline plan
        const auto secondary_import_proxy = std::static_pointer_cast<ImportOperatorProxy>(input_proxy);
        current_pipeline_import_proxies.push_back(secondary_import_proxy);
        consumed_import_proxies.push_back(secondary_import_proxy);
        // Check if the secondary import proxy results from another pipeline, and add a pipeline dependency accordingly.
        auto pipeline_predecessor = FindPipelinePredecessor(secondary_import_proxy);
        if (pipeline_predecessor) {
          current_pipeline_predecessors.emplace_back(pipeline_predecessor);
        }
      }
    }

    return PqpUpwardVisitation::kVisitOutputs;
  });

  // Abort, if no pipeline plan can be created from @param primary_import_proxy as of now.
  if (!current_pipeline_plan) {
    return nullptr;
  }

  /**
   * (3) CUT OFF PIPELINE PLAN AND GENERATE FRAGMENT DEFINITIONS
   */
  const size_t current_pipeline_id = compilation_context_->NextPipelineId();
  const std::string current_pipeline_identity =
      compilation_context_->QueryIdentity() + "_" + std::to_string(current_pipeline_id);
  std::vector<PipelineFragmentDefinition> current_pipeline_fragment_definitions;

  // Check if we have reached the PQP's final pipeline plan.
  if (current_pipeline_plan == pqp_) {
    Assert(pqp_->OutputNodeCount() == 0, "PQP root should not have any outputs.");
    pqp_ = nullptr;
    // Generate final pipeline's fragment definitions
    // TODO
  } else {
    // Apply the specified Exchange strategy
    Assert(current_pipeline_plan->Type() == OperatorType::kExchange, "Expected ExchangeOperatorProxy.");
    const auto exchange_proxy = std::static_pointer_cast<ExchangeOperatorProxy>(current_pipeline_plan);
    const auto exchange_result = exchange_proxy->Strategy()->ComputeExchangeResult(
        current_pipeline_id, compilation_context_, current_pipeline_import_proxies);
    current_pipeline_fragment_definitions = std::move(exchange_result.pipeline_fragment_definitions);

    // Adjust the pipeline plan to incorporate partitioning, if necessary.
    if (exchange_result.pipeline_partition_proxy) {
      InsertPlanNodeBelow<AbstractOperatorProxy>(current_pipeline_plan, PlanInputSide::kLeft,
                                                 *exchange_result.pipeline_partition_proxy);
    }

    // Create import proxy from exchange results
    auto import_column_ids = std::vector<ColumnId>(current_pipeline_plan->OutputColumnsCount());
    std::iota(import_column_ids.begin(), import_column_ids.end(), ColumnId{0});
    const auto import_proxy = ImportOperatorProxy::Make(std::move(exchange_result.target_objects), import_column_ids);
    import_proxy->SetOutputObjectsCount(exchange_result.target_worker_count);
    import_proxy->SetOutputPartitionsCount(exchange_result.target_partition_count);
    // TODO: Re-consider: Set current pipeline's identity as a comment,
    //                    so that this pipeline can be resolved as a predecessor later.
    import_proxy->SetComment(current_pipeline_identity);

    // Cut off the pipeline plan by substituting the exchange proxy with import and export proxies.
    InsertPlanNodeAbove<AbstractOperatorProxy>(current_pipeline_plan, import_proxy);
    import_proxy->SetLeftInput(nullptr);
    Assert(current_pipeline_plan->OutputNodeCount() == 0,
           "Pipeline plan should no longer have outputs since it was cut off.");
    auto export_proxy = ExportOperatorProxy::Dummy();
    export_proxy->PrefixIdentity(compilation_context_->QueryIdentity());
    ReplacePlanNode<AbstractOperatorProxy>(current_pipeline_plan, export_proxy);
    current_pipeline_plan = export_proxy;
  }

  /**
   * (3) GENERATE PIPELINE IMPORT DEFINITIONS
  // Level of intra-operator parallelism
  size_t worker_count = std::min(current_pipeline_plan->InputObjectsCount(), compilation_context_->MaxWorkerCount());
  std::vector<std::vector<PipelineFragmentDefinition>> current_pipeline_fragment_definitions =
      GetPipelineFragmentDefinitions(primary_import_proxy, worker_count);

  // Secondary imports from joins or union operations
  for (const auto& secondary_import_proxy : current_pipeline_import_proxies) {
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

  // (4) GENERATE PIPELINE EXPORT KEYS

  std::string current_pipeline_identity = compilation_context_->GeneratePipelineIdentity();

  auto current_pipeline_export_format = ExportFormat::kOrc;
  std::stringstream pipeline_export_key_prefix_stream;
  std::string pipeline_export_key_suffix;
  if (is_final_pipeline) {
    // Final result export
    pipeline_export_key_prefix_stream << compilation_context_->FinalResultsKeyPrefix();
    pipeline_export_key_prefix_stream << current_pipeline_identity;
    pipeline_export_key_suffix = compilation_context_->TargetFileExtension();
    current_pipeline_export_format = compilation_context_->TargetFormat();
  } else {
    // Intermediate result export
    pipeline_export_key_prefix_stream << compilation_context_->IntermediateResultsKeyPrefix();
    pipeline_export_key_prefix_stream << current_pipeline_identity;
    pipeline_export_key_suffix = ".orc";
  }

  // Generate an export key for each fragment instance
  std::vector<std::string> current_pipeline_export_keys =
      GetPipelineExportKeys(pipeline_export_key_prefix_stream.str(), pipeline_export_key_suffix,
                            current_pipeline_fragment_definitions.size());
  */

  /**
   * (6) CREATE PIPELINE
   */
  auto current_pipeline = std::make_shared<PqpPipeline>(current_pipeline_identity, current_pipeline_plan);
  for (const auto& pipeline : current_pipeline_predecessors) {
    pipeline->SetAsPredecessorOf(current_pipeline);
  }

  return current_pipeline;
}

std::shared_ptr<PqpPipeline> PqpPipelineSlicer::FindPipelinePredecessor(
    std::shared_ptr<ImportOperatorProxy> import_proxy) const {
  auto predecessor_pipeline_iter = std::find_if(
      pipelines_.cbegin(), pipelines_.cend(),
      [&import_proxy](const auto& pipeline) { return (import_proxy->Comment() == pipeline->Identity()); });

  if (predecessor_pipeline_iter != pipelines_.cend()) {
    return *predecessor_pipeline_iter;
  }
  return nullptr;
}

}  // namespace skyrise
