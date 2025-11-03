#include "pqp_pipeline_slicer.hpp"

#include <algorithm>
#include <iomanip>
#include <numeric>

#include "compiler/physical_query_plan/operator_proxy/exchange_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/partition_operator_proxy.hpp"
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

const std::vector<std::shared_ptr<PqpPipeline>>& PqpPipelineSlicer::SlicePqpIntoPipelines() {
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
   * (1) Track pipeline predecessor, if given.
   *      - We try to cut off a pipeline plan from a PQP with @param primary_import_proxy as the potential origin.
   *        import proxy. If the import proxy references previous pipeline results, we need to track the according
   * pipeline dependency.
   */
  std::vector<std::shared_ptr<PqpPipeline>> current_pipeline_predecessors;
  TryAddPipelineDependency(current_pipeline_predecessors, primary_import_proxy);

  /**
   * (2) Determine pipeline plan borders
   *      - The pipeline plan's origin is @param primary_import_proxy.
   *      - Traverse the pipeline plan upwards until finding an operator proxy that either completes or terminates the
   *        pipeline plan.
   *      - Track pipeline predecessors, in case there are secondary import proxies in the pipeline plan.
   *      - Abort when finding unresolved input plans requiring the creation of a predecessor pipeline first.
   */
  std::shared_ptr<AbstractOperatorProxy> current_pipeline_plan = primary_import_proxy;
  VisitPqpUpwards(primary_import_proxy, [&](const auto& operator_proxy) {
    Assert(operator_proxy->OutputNodeCount() <= 1, "Operator proxy is expected to have a single output proxy at most.");

    // Check for pipeline plan termination.
    if (operator_proxy->Type() == OperatorType::kExchange || operator_proxy->Type() == OperatorType::kExport) {
      // While an Export proxy defines the end of a PQP, an Exchange proxy defines the end of a pipeline plan.
      current_pipeline_plan = operator_proxy;
      return PqpUpwardVisitation::kDoNotVisitOutputs;
    }

    // Check for secondary predecessor pipeline plan
    if (operator_proxy->InputNodeCount() == 2) {
      for (const auto& input_proxy : operator_proxy->Inputs()) {
        if (input_proxy == current_pipeline_plan) {
          // Continue because input_proxy is resolved by the current pipeline plan.
          continue;
        }

        // Only Import proxies can reference pipeline plans. Abort upwards traversal for all other operator proxy
        // inputs, because they are part of predecessor pipelines not created yet.
        if (input_proxy->Type() != OperatorType::kImport) {
          current_pipeline_plan = nullptr;
          return PqpUpwardVisitation::kDoNotVisitOutputs;
        }

        // Add input as a secondary import proxy of the current pipeline plan
        const auto secondary_import_proxy = std::static_pointer_cast<ImportOperatorProxy>(input_proxy);
        current_pipeline_import_proxies.push_back(secondary_import_proxy);
        consumed_import_proxies.push_back(secondary_import_proxy);
        // If the import proxy defines previous pipeline results we need to track the according pipeline dependency.
        TryAddPipelineDependency(current_pipeline_predecessors, secondary_import_proxy);
      }
    }

    return PqpUpwardVisitation::kVisitOutputs;
  });

  // Abort routine if we cannot create a pipeline plan from @param primary_import_proxy as of now.
  if (!current_pipeline_plan) {
    return nullptr;
  }

  /**
   * (3) Cut off pipeline plan and substitute it with an Import proxy that references the according results.
   */
  const auto current_pipeline_id = compilation_context_->NextPipelineId();
  const auto current_pipeline_identity = compilation_context_->PipelineIdentity(current_pipeline_id);
  std::vector<PipelineFragmentDefinition> current_pipeline_fragment_definitions;

  // Check if PQP is finalized by current pipeline plan.
  if (pqp_ == current_pipeline_plan) {
    Assert(pqp_->OutputNodeCount() == 0, "PQP root should not have any outputs.");
    pqp_ = nullptr;
    // TODO Create StandardStrategy for final pipeline?
    // TODO Generate fragment definition
    auto current_pipeline_export_format = compilation_context_->GetExportFormat();
    //    std::stringstream pipeline_export_key_prefix_stream;
    //    std::string pipeline_export_key_suffix;
    //      // Final result export
    //      pipeline_export_key_prefix_stream << compilation_context_->FinalResultsKeyPrefix();
    //      pipeline_export_key_prefix_stream << current_pipeline_identity;
    //      pipeline_export_key_suffix = compilation_context_->TargetFileExtension();
    //      current_pipeline_export_format = compilation_context_->TargetFormat();
    //
    //    // Generate an export key for each fragment instance
    //    std::vector<std::string> current_pipeline_export_keys =
    //      GetPipelineExportKeys(pipeline_export_key_prefix_stream.str(), pipeline_export_key_suffix,
    //                            current_pipeline_fragment_definitions.size());

  } else {
    // 1. Resolve data exchange
    Assert(current_pipeline_plan->Type() == OperatorType::kExchange, "Expected ExchangeOperatorProxy.");
    const auto exchange_proxy = std::static_pointer_cast<ExchangeOperatorProxy>(current_pipeline_plan);
    const auto exchange_result = exchange_proxy->Strategy()->ComputeExchangeResult(
        current_pipeline_id, compilation_context_, current_pipeline_import_proxies);
    current_pipeline_fragment_definitions = std::move(exchange_result.pipeline_fragment_definitions);

    // 2. Adjust the pipeline plan to incorporate partitioning, if necessary.
    if (exchange_result.partitioning_function) {
      const auto partition_proxy = PartitionOperatorProxy::Make(exchange_result.partitioning_function);
      InsertPlanNodeBelow<AbstractOperatorProxy>(current_pipeline_plan, PlanInputSide::kLeft, partition_proxy);
    }

    // 3. Create Import proxy that substitutes the pipeline plan in the PQP.
    std::vector<ColumnId> import_column_ids;
    import_column_ids.reserve(current_pipeline_plan->OutputColumnsCount());
    std::iota(import_column_ids.begin(), import_column_ids.end(), ColumnId{0});
    const auto import_proxy_substitute =
        ImportOperatorProxy::Make(exchange_result.ObjectReferences(), import_column_ids);
    // An origin identifier must be provided, so that this pipeline can be resolved as a predecessor pipeline later.
    import_proxy_substitute->SetOrigin(current_pipeline_identity, exchange_result.PartitionCount(),
                                       exchange_result.next_pipeline_target_object_count);

    // 4. Cut off, and substitute the current pipeline plan in the PQP.
    InsertPlanNodeAbove<AbstractOperatorProxy>(current_pipeline_plan, import_proxy_substitute);
    import_proxy_substitute->SetLeftInput(nullptr);
    Assert(current_pipeline_plan->OutputNodeCount() == 0,
           "Pipeline plan got cut off, and should thus no longer have any outputs.");

    // 5. Finalize pipeline plan by replacing the Exchange proxy.
    auto export_proxy = ExportOperatorProxy::Dummy();
    ReplacePlanNode<AbstractOperatorProxy>(current_pipeline_plan, export_proxy);
    current_pipeline_plan = export_proxy;
  }

  /**
   * (4) Create PqpPipeline
   */
  auto current_pipeline = std::make_shared<PqpPipeline>(current_pipeline_identity, current_pipeline_plan);
  current_pipeline->SetFragmentDefinitions(std::move(current_pipeline_fragment_definitions));
  for (const auto& pipeline : current_pipeline_predecessors) {
    pipeline->SetAsPredecessorOf(current_pipeline);
  }

  return current_pipeline;
}

void PqpPipelineSlicer::TryAddPipelineDependency(
    std::vector<std::shared_ptr<PqpPipeline>>& current_pipeline_predecessors,
    std::shared_ptr<ImportOperatorProxy> import_proxy) const {
  if (pipelines_.empty() || import_proxy->OriginIdentifier().empty()) {
    // Zero pipelines or Import proxy without a set pipeline dependency.
    return;
  }

  for (const auto existing_pipeline : pipelines_) {
    if (import_proxy->OriginIdentifier() == existing_pipeline->Identity()) {
      // Track predecessor pipeline and return to caller.
      current_pipeline_predecessors.push_back(existing_pipeline);
      return;
    }
  }
}

}  // namespace skyrise
