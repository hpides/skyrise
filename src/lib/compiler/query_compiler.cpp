#include "query_compiler.hpp"

#include <boost/algorithm/string.hpp>

#include "query_context.hpp"
#include "sql/create_sql_parser_error_message.hpp"
#include "utils/assert.hpp"

namespace skyrise {

QueryCompiler::QueryCompiler(std::string sql_query, std::shared_ptr<AbstractCatalog> catalog,
                             std::string target_bucket_name)
    : sql_query_(std::move(sql_query)),
      catalog_(std::move(catalog)),
      target_bucket_name_(std::move(target_bucket_name)) {
  // (1) Get parse result from SQLParser & calculate runtime
  hsql::SQLParserResult parse_result;
  {
    const auto start = std::chrono::high_resolution_clock::now();
    hsql::SQLParser::parse(sql_query_, &parse_result);
    const auto done = std::chrono::high_resolution_clock::now();
    metrics_.parse_time_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(done - start);
  }

  AssertInput(parse_result.isValid(), CreateSqlParserErrorMessage(sql_query_, parse_result));
  DebugAssert(parse_result.size() > 0, "Cannot compile SQL query because it has zero statements.");

  // (2) Collect parse results
  for (auto* statement : parse_result.releaseStatements()) {
    // Create shared pointers from raw pointers
    parsed_sql_statements_.emplace_back(std::make_shared<hsql::SQLParserResult>(statement));
  }

  // (3) For each parsed SQL statement, create a QueryStatementCompiler with a QueryContext

  // Split the (multi-) statement SQL string into substrings for each statement. These substrings can be used to cache
  // query plans. The SQLParser only offers the length of the string, so it needs to be split manually.
  auto sql_string_offset = 0u;

  query_statement_compilers_.reserve(parsed_sql_statements_.size());
  for (const auto& parsed_sql_statement : parsed_sql_statements_) {
    parsed_sql_statement->setIsValid(true);

    // We will always have one at 0 because we set it ourselves
    const auto* statement = parsed_sql_statement->getStatement(0);

    // Check if statement alters the structure of the database in a way that following statements might depend upon.
    switch (statement->type()) {
      case hsql::StatementType::kStmtImport:
      case hsql::StatementType::kStmtCreate:
      case hsql::StatementType::kStmtDrop:
      case hsql::StatementType::kStmtAlter:
      case hsql::StatementType::kStmtRename: {
        Fail("Altering SQL statements are currently unsupported!");
      } break;
      default: { /* do nothing */
      }
    }

    const auto statement_string_length = statement->stringLength;
    const auto statement_string = boost::trim_copy(sql_query_.substr(sql_string_offset, statement_string_length));
    sql_string_offset += statement_string_length;
    sql_statement_strings_.emplace_back(statement_string);

    // Create QueryContext
    auto query_statement_context = std::make_shared<QueryContext>(statement_string, catalog_, target_bucket_name_);
    query_statement_context->SetTargetFormat(ExportFormat::kCsv);
    // Create StatementCompiler
    auto query_statement_compiler =
        std::make_shared<QueryStatementCompiler>(parsed_sql_statement, query_statement_context);
    metrics_.statement_metrics.emplace_back(query_statement_compiler->Metrics());
    query_statement_compilers_.emplace_back(std::move(query_statement_compiler));
  }
}

const std::string& QueryCompiler::SqlQueryString() const { return sql_query_; }

size_t QueryCompiler::SqlStatementCount() const { return sql_statement_strings_.size(); }

const std::vector<std::string>& QueryCompiler::SqlStatementStrings() { return sql_statement_strings_; }

const std::vector<std::shared_ptr<hsql::SQLParserResult>>& QueryCompiler::ParsedSqlStatements() const {
  return parsed_sql_statements_;
}

std::vector<std::shared_ptr<AbstractLqpNode>> QueryCompiler::GetLqps() {
  std::vector<std::shared_ptr<AbstractLqpNode>> lqps;
  lqps.reserve(SqlStatementCount());
  for (const auto& query_statement_compiler : query_statement_compilers_) {
    lqps.emplace_back(query_statement_compiler->GetLqp());
  }
  return lqps;
}

std::vector<std::shared_ptr<AbstractLqpNode>> QueryCompiler::GetOptimizedLqps() {
  std::vector<std::shared_ptr<AbstractLqpNode>> lqps;
  lqps.reserve(SqlStatementCount());
  for (const auto& query_statement_compiler : query_statement_compilers_) {
    lqps.emplace_back(query_statement_compiler->GetOptimizedLqp());
  }
  return lqps;
}

std::vector<std::shared_ptr<AbstractOperatorProxy>> QueryCompiler::GetPqps() {
  std::vector<std::shared_ptr<AbstractOperatorProxy>> pqps;
  pqps.reserve(SqlStatementCount());
  for (const auto& query_statement_compiler : query_statement_compilers_) {
    pqps.emplace_back(query_statement_compiler->GetPqp());
  }
  return pqps;
}

std::vector<std::shared_ptr<AbstractOperatorProxy>> QueryCompiler::GetOptimizedPqps() {
  std::vector<std::shared_ptr<AbstractOperatorProxy>> pqps;
  pqps.reserve(SqlStatementCount());
  for (const auto& query_statement_compiler : query_statement_compilers_) {
    pqps.emplace_back(query_statement_compiler->GetOptimizedPqp());
  }
  return pqps;
}

const std::vector<std::shared_ptr<PqpPipeline>>& QueryCompiler::GetPqpPipelines() {
  // for each PQP: call slicer->GetPipelines, SetPredecessorPipeline
  Assert(GetOptimizedPqps().size() == 1, "Currently, only one PQP is supported.");
  if (pqp_pipelines_.empty()) {
    pqp_pipelines_ = query_statement_compilers_.at(0)->GetPqpPipelines();
  }

  return pqp_pipelines_;
}

const QueryCompilerMetrics& QueryCompiler::Metrics() const { return metrics_; }

}  // namespace skyrise