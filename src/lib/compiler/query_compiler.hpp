#pragma once

#include <chrono>
#include <memory>
#include <string>

#include <SQLParser.h>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "metadata/abstract_catalog.hpp"
#include "physical_query_plan/abstract_operator_proxy.hpp"
#include "physical_query_plan/pqp_pipeline.hpp"
#include "query_statement_compiler.hpp"

namespace skyrise {

struct QueryCompilerMetrics {
  std::vector<std::shared_ptr<const QueryStatementCompilerMetrics>> statement_metrics;

  // This is different from the other measured times as we only get this for all statements at once.
  std::chrono::nanoseconds parse_time_nanos{0};
};
std::ostream& operator<<(std::ostream& stream, const QueryCompilerMetrics& metrics);

class QueryCompiler : public Noncopyable {
 public:
  QueryCompiler(std::string sql_query, std::shared_ptr<AbstractCatalog> catalog,
                std::string target_bucket_name = "mock_target_bucket");

  /**
   * @returns the original SQL string.
   */
  const std::string& SqlQueryString() const;

  /**
   * @returns the number of statements in the original SQL string.
   */
  size_t SqlStatementCount() const;

  /**
   * @returns the SQL string for each statement.
   */
  const std::vector<std::string>& SqlStatementStrings();

  /**
   * @returns the SQLParser results for each statement.
   */
  const std::vector<std::shared_ptr<hsql::SQLParserResult>>& ParsedSqlStatements() const;

  /**
   * @returns the unoptimized logical query plan roots for each statement.
   */
  std::vector<std::shared_ptr<AbstractLqpNode>> GetLqps();

  /**
   * @returns the optimized logical query plan roots for each statement.
   */
  std::vector<std::shared_ptr<AbstractLqpNode>> GetOptimizedLqps();

  /**
   * @returns unoptimized physical query plan roots for each statement.
   */
  std::vector<std::shared_ptr<AbstractOperatorProxy>> GetPqps();

  /**
   * @returns unoptimized physical query plan roots for each statement.
   */
  std::vector<std::shared_ptr<AbstractOperatorProxy>> GetOptimizedPqps();

  /**
   * @returns
   */
  const std::vector<std::shared_ptr<PqpPipeline>>& GetPqpPipelines();

  const QueryCompilerMetrics& Metrics() const;

 private:
  // Input data
  const std::string sql_query_;
  const std::shared_ptr<AbstractCatalog> catalog_;
  const std::string target_bucket_name_;

  // Utilities
  std::vector<std::shared_ptr<QueryStatementCompiler>> query_statement_compilers_;
  QueryCompilerMetrics metrics_ = {};

  // SQL artifacts
  std::vector<std::string> sql_statement_strings_;
  std::vector<std::shared_ptr<hsql::SQLParserResult>> parsed_sql_statements_;
  std::vector<std::reference_wrapper<const SqlTranslationInfo>> sql_translation_infos_;

  // Plans
  std::vector<std::shared_ptr<AbstractLqpNode>> optimized_lqps_;
  std::vector<std::shared_ptr<PqpPipeline>> pqp_pipelines_;
};

}  // namespace skyrise