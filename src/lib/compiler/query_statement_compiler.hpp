#pragma once

#include <chrono>
#include <memory>
#include <string>

#include <SQLParser.h>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "optimizer/abstract_optimizer.hpp"
#include "physical_query_plan/abstract_operator_proxy.hpp"
#include "physical_query_plan/pqp_pipeline.hpp"
#include "query_context.hpp"
#include "sql/sql_translator.hpp"

namespace skyrise {

struct QueryStatementCompilerMetrics {
  // SQL Translation
  std::chrono::nanoseconds sql_translation_duration;
  std::chrono::nanoseconds lqp_translation_duration;
  // LQP
  std::chrono::nanoseconds lqp_optimization_duration;
  OptimizerMetrics lqp_optimizer_metrics;
  // PQP
  std::chrono::nanoseconds pqp_optimization_duration;
  OptimizerMetrics pqp_optimizer_metrics;
  std::chrono::nanoseconds pqp_slicing_duration;
};

/**
 * Doc
 */
class QueryStatementCompiler : public Noncopyable {
 public:
  QueryStatementCompiler(std::shared_ptr<hsql::SQLParserResult> parsed_sql_statement,
                         std::shared_ptr<QueryContext> query_statement_context);

  /**
   * @returns the SQL string of this particular statement
   */
  [[nodiscard]] const std::string& SqlStatementString() const;

  /**
   * @returns an unoptimized logical query plan from the SQL statement
   */
  const std::shared_ptr<AbstractLqpNode>& GetLqp();
  const std::shared_ptr<AbstractLqpNode>& GetOptimizedLqp();

  /**
   * Uses the optimized logical query plan to create a physical query plan that can be executed on a single worker.
   */
  const std::shared_ptr<AbstractOperatorProxy>& GetPqp();

  /**
   * Uses the "vanilla" physical query plan and optimizes it for distributed execution with multiple independent
   * workers.
   */
  const std::shared_ptr<AbstractOperatorProxy>& GetOptimizedPqp();

  /**
   * Uses the optimized phyiscal query plan and slices it into PqpPipelines for distributed execution.
   */
  const std::vector<std::shared_ptr<PqpPipeline>>& GetPqpPipelines();

  const std::shared_ptr<QueryStatementCompilerMetrics>& Metrics() const;

 private:
  // SQL
  std::shared_ptr<hsql::SQLParserResult> parsed_sql_statement_;
  SqlTranslationInfo translation_info_;

  // Plans
  std::shared_ptr<AbstractLqpNode> lqp_;
  std::shared_ptr<AbstractLqpNode> optimized_lqp_;
  std::shared_ptr<AbstractOperatorProxy> pqp_;
  std::shared_ptr<AbstractOperatorProxy> optimized_pqp_;
  std::vector<std::shared_ptr<PqpPipeline>> pqp_pipelines_;

  std::shared_ptr<QueryContext> query_statement_context_;
  std::shared_ptr<QueryStatementCompilerMetrics> metrics_;
};

}  // namespace skyrise