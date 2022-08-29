#include "query_statement_compiler.hpp"

#include "logical_query_plan/lqp_translator.hpp"
#include "optimizer/lqp_optimizer.hpp"
#include "optimizer/pqp_optimizer.hpp"
#include "physical_query_plan/export_operator_proxy.hpp"
#include "physical_query_plan/pqp_pipeline_slicer.hpp"
#include "physical_query_plan/pqp_utils.hpp"
#include "sql/sql_translator.hpp"
#include "utils/timer.hpp"

namespace skyrise {

QueryStatementCompiler::QueryStatementCompiler(std::shared_ptr<hsql::SQLParserResult> parsed_sql_statement,
                                               std::shared_ptr<QueryContext> query_statement_context)
    : parsed_sql_statement_(std::move(parsed_sql_statement)),
      query_statement_context_(std::move(query_statement_context)),
      metrics_(std::make_shared<QueryStatementCompilerMetrics>()) {}

const std::string& QueryStatementCompiler::SqlStatementString() const {
  return query_statement_context_->QueryString();
}

const std::shared_ptr<AbstractLqpNode>& QueryStatementCompiler::GetLqp() {
  // Assert(!optimized_lqp_, "The LQP is already optimized. Call GetOptimizedLqp().");
  if (lqp_) {
    return lqp_;
  }

  Timer timer;
  {
    auto translation_result =
        SqlTranslator(query_statement_context_->Catalog()).translate_parser_result(*parsed_sql_statement_);
    DebugAssert(translation_result.lqp_nodes.size() == 1,
                "LQP translation returned no or more than one LQP root for a single statement.");
    lqp_ = translation_result.lqp_nodes.front();
    translation_info_ = std::move(translation_result.translation_info);
  }
  metrics_->sql_translation_duration = timer.Lap();

  return lqp_;
}

const std::shared_ptr<AbstractLqpNode>& QueryStatementCompiler::GetOptimizedLqp() {
  if (optimized_lqp_) {
    return optimized_lqp_;
  }

  auto lqp = GetLqp();
  // The LqpOptimizer works on the original unoptimized LQP nodes. After optimizing, the unoptimized version is also
  // optimized, which could lead to subtle bugs. Therefore, we release ownership as follows:
  lqp_.reset();

  Timer timer;
  auto lqp_optimizer_metrics = std::make_shared<OptimizerMetrics>();
  {
    auto lqp_optimizer = LqpOptimizer::CreateDefaultLqpOptimizer();
    optimized_lqp_ = lqp_optimizer->Optimize(std::move(lqp), lqp_optimizer_metrics);
  }
  metrics_->lqp_optimization_duration = timer.Lap();
  metrics_->lqp_optimizer_metrics = *lqp_optimizer_metrics;

  return optimized_lqp_;
}

const std::shared_ptr<AbstractOperatorProxy>& QueryStatementCompiler::GetPqp() {
  // Assert(!optimized_pqp_, "The PQP is already optimized. Call GetOptimizedPqp().");
  if (pqp_) {
    return pqp_;
  }

  auto optimized_lqp = GetOptimizedLqp();
  Timer timer;
  {
    // (1) Translate LQP
    const auto pqp_result = LqpTranslator(query_statement_context_).TranslateNode(optimized_lqp);
    // (2) Generate S3 path for the final result
    std::stringstream target_key_stream;
    target_key_stream << query_statement_context_->FinalResultsKeyPrefix();
    target_key_stream << query_statement_context_->QueryIdentity() << "_final_result";
    target_key_stream << query_statement_context_->TargetFileExtension();
    // (3) Define Export for final result
    const auto pqp_with_export =
        ExportOperatorProxy::Make(query_statement_context_->TargetBucketName(), target_key_stream.str(),
                                  query_statement_context_->TargetFormat(), pqp_result);
    pqp_ = std::static_pointer_cast<AbstractOperatorProxy>(pqp_with_export);
    // (4) Prefix all operator proxies in PQP
    PrefixOperatorProxyIdentities(pqp_, query_statement_context_->QueryIdentity());
  }
  metrics_->lqp_translation_duration = timer.Lap();

  return pqp_;
}

const std::shared_ptr<AbstractOperatorProxy>& QueryStatementCompiler::GetOptimizedPqp() {
  if (optimized_pqp_) {
    return optimized_pqp_;
  }

  auto pqp = GetPqp();
  // The PqpOptimizer works on the original unoptimized PQP nodes. After optimizing, the unoptimized version is also
  // optimized, which could lead to subtle bugs. Therefore, we release ownership as follows:
  pqp_.reset();

  Timer timer;
  auto pqp_optimizer_metrics = std::make_shared<OptimizerMetrics>();
  {
    auto pqp_optimizer = PqpOptimizer::CreateDefaultPqpOptimizer();
    optimized_pqp_ = pqp_optimizer->Optimize(std::move(pqp));
  }
  metrics_->pqp_optimization_duration = timer.Lap();
  metrics_->pqp_optimizer_metrics = *pqp_optimizer_metrics;

  return optimized_pqp_;
}

const std::vector<std::shared_ptr<PqpPipeline>>& QueryStatementCompiler::GetPqpPipelines() {
  if (!pqp_pipelines_.empty()) {
    return pqp_pipelines_;
  }

  auto pqp = GetOptimizedPqp();
  optimized_pqp_.reset();

  Timer timer;
  {
    auto pipeline_slicer = PqpPipelineSlicer(std::move(pqp), query_statement_context_);
    pqp_pipelines_ = pipeline_slicer.GetPipelines();
  }
  metrics_->pqp_slicing_duration = timer.Lap();

  return pqp_pipelines_;
}

const std::shared_ptr<QueryStatementCompilerMetrics>& QueryStatementCompiler::Metrics() const { return metrics_; }

}  // namespace skyrise
