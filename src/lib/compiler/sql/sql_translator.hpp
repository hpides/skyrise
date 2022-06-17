/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "SQLParser.h"
#include "compiler/logical_query_plan/abstract_lqp_node.hpp"
#include "compiler/logical_query_plan/lqp_wrapper.hpp"
#include "expression/abstract_expression.hpp"
#include "metadata/abstract_catalog.hpp"
#include "parameter_id_allocator.hpp"
#include "sql_identifier_resolver.hpp"
#include "sql_identifier_resolver_proxy.hpp"

namespace skyrise {

/**
 * Holds information about the query translation:
 *  - parameter_ids_of_value_placeholders:  the parameter ids of value placeholders
 */
struct SqlTranslationInfo {
  std::vector<ParameterID> parameter_ids_of_value_placeholders{};
};

/**
 * Return value of translate_parser_result().
 * lqp_node :          the actual LQP
 * translation_info :  meta info struct
 */
struct SqlTranslationResult {
  std::vector<std::shared_ptr<AbstractLqpNode>> lqp_nodes;
  SqlTranslationInfo translation_info;
};

/**
 * Produces an LQP (Logical Query Plan), as defined in src/logical_query_plan/, from an hsql::SQLParseResult.
 *
 * The elements of the vector returned by SqlTranslator::translate_parse_result(const hsql::SQLParserResult&)
 * point to the root/result nodes of the LQPs.
 *
 * An LQP can either be handed to the Optimizer, or it can be directly turned into Operators by the LqpTranslator.
 */
class SqlTranslator final {
 public:
  /**
   * Public constructor to create a SqlTranslator with an empty context.
   * @param catalog         Provides lookup information to resolve table and column names.
   * @param views           Contains a mapping of LQPs and associated SQL views, which already got evaluated.
   *                        TODO(anyone): Think about moving views into the QueryContext or Catalog.
   *                                      The QueryCompiler should make views, resulting from a statement, available to
   *                                      following statements.
   *
   * There is also a private constructor, down below, which is used for evaluating subqueries.
   */
  SqlTranslator(std::shared_ptr<AbstractCatalog> catalog,
                const std::unordered_map<std::string, std::shared_ptr<LqpWrapper>>& views =
                    std::unordered_map<std::string, std::shared_ptr<LqpWrapper>>());

  /**
   * Main entry point. Translate an AST produced by the SQLParser into LQPs, one for each SQL statement.
   */
  SqlTranslationResult translate_parser_result(const hsql::SQLParserResult& result);

  /**
   * Translate an Expression AST into a Skyrise expression. No columns can be referenced in expressions translated by
   * this call.
   */
  static std::shared_ptr<AbstractExpression> translate_hsql_expr(const hsql::Expr& hsql_expr);

 private:
  /**
   * An expression and its identifiers. This is partly redundant to the SqlIdentifierResolver, but allows expressions
   * for equal SQL expressions with different identifiers (e.g., SELECT COUNT(*) AS cnt1, COUNT(*) AS cnt2 FROM ...).
   */
  struct SelectListElement {
    explicit SelectListElement(const std::shared_ptr<AbstractExpression>& init_expression);
    SelectListElement(const std::shared_ptr<AbstractExpression>& init_expression,
                      const std::vector<SqlIdentifier>& init_identifiers);

    std::shared_ptr<AbstractExpression> expression;
    std::vector<SqlIdentifier> identifiers;
  };

  /**
   * Track state while translating the FROM clause. This makes sure only the actually available SQL identifiers can be
   * used, e.g. "SELECT * FROM t1, t2 JOIN t3 ON t1.a = t2.a" is illegal since t1 is invisible to the seconds entry.
   * Also ensures the correct columns go into Select wildcards, even in presence of NATURAL/SEMI joins that remove
   * columns from input tables
   */
  struct TableSourceState final {
    TableSourceState() = default;
    TableSourceState(const std::shared_ptr<AbstractLqpNode>& init_lqp,
                     const std::unordered_map<std::string, std::vector<SelectListElement>>& init_elements_by_table_name,
                     const std::vector<SelectListElement>& init_elements_in_order,
                     const std::shared_ptr<SqlIdentifierResolver>& init_sql_identifier_resolver);

    void Append(TableSourceState&& rhs);

    std::shared_ptr<AbstractLqpNode> lqp;

    // Collects the output of the FROM clause to expand wildcards (*; <t>.*) used in the SELECT list
    std::unordered_map<std::string, std::vector<SelectListElement>> elements_by_table_name;

    // To establish the correct order of columns in SELECT *
    std::vector<SelectListElement> elements_in_order;

    std::shared_ptr<SqlIdentifierResolver> sql_identifier_resolver;
  };

  /**
   * Represents the '*'/'<table>.*' wildcard in a Query. The SQLParser regards it as an Expression, but to Skyrise it is
   * not.
   */
  struct SQLWildcard final {
    std::optional<std::string> table_name;
  };

  /**
   * Internal constructor to create a SqlTranslator for subqueries
   * @param catalog                                 Provides lookup information to resolve table and column names.
   * @param external_sql_identifier_resolver_proxy  Set during recursive invocations to resolve external identifiers.
   *                                                in correlated subqueries.
   * @param parameter_id_counter                    Set during recursive invocations to allocate unique ParameterIDs
   *                                                for each encountered parameter.
   * @param views                                   Contains a mapping of LQPs and associated SQL views, which
   *                                                already got evaluated.
   * @param with_descriptions                       Contains a mapping of LQPs and associated WITH aliases, which
   *                                                already got evaluated.
   */
  SqlTranslator(std::shared_ptr<AbstractCatalog> catalog,
                const std::shared_ptr<SqlIdentifierResolverProxy>& external_sql_identifier_resolver_proxy,
                const std::shared_ptr<ParameterIDAllocator>& parameter_id_allocator,
                const std::unordered_map<std::string, std::shared_ptr<LqpWrapper>>& views,
                const std::unordered_map<std::string, std::shared_ptr<LqpWrapper>>& with_descriptions);

  std::shared_ptr<AbstractLqpNode> TranslateStatement(const hsql::SQLStatement& statement);
  std::shared_ptr<AbstractLqpNode> TranslateSelectStatement(const hsql::SelectStatement& select);

  void TranslateHsqlWithDescription(hsql::WithDescription& desc);
  TableSourceState TranslateTableRef(const hsql::TableRef& hsql_table_ref);
  TableSourceState TranslateTableOrigin(const hsql::TableRef& hsql_table_ref);
  std::shared_ptr<AbstractLqpNode> TranslateStoredTable(
      const std::string& name, const std::shared_ptr<SqlIdentifierResolver>& sql_identifier_resolver);
  TableSourceState TranslatePredicatedJoin(const hsql::JoinDefinition& join);
  TableSourceState TranslateNaturalJoin(const hsql::JoinDefinition& join);
  TableSourceState TranslateCrossProduct(const std::vector<hsql::TableRef*>& tables);

  std::vector<SelectListElement> TranslateSelectList(const std::vector<hsql::Expr*>& select_list);
  void TranslateSelectGroupByHaving(const hsql::SelectStatement& select,
                                    const std::vector<SelectListElement>& select_list_elements);

  void TranslateSetOperation(const hsql::SetOperation& set_operator);
  void TranslateOrderBy(const std::vector<hsql::OrderDescription*>& order_list);
  void TranslateLimit(const hsql::LimitDescription& limit);

  static std::shared_ptr<AbstractLqpNode> TranslateInsert(const hsql::InsertStatement& insert);
  static std::shared_ptr<AbstractLqpNode> TranslateDelete(const hsql::DeleteStatement& delete_statement);
  static std::shared_ptr<AbstractLqpNode> TranslateUpdate(const hsql::UpdateStatement& update);

  std::shared_ptr<AbstractLqpNode> TranslateCreate(const hsql::CreateStatement& create_statement);
  std::shared_ptr<AbstractLqpNode> TranslateCreateView(const hsql::CreateStatement& create_statement);
  static std::shared_ptr<AbstractLqpNode> TranslateCreateTable(const hsql::CreateStatement& create_statement);

  std::shared_ptr<AbstractLqpNode> TranslateDrop(const hsql::DropStatement& drop_statement);

  static std::shared_ptr<AbstractLqpNode> TranslatePrepare(const hsql::PrepareStatement& prepare_statement);
  static std::shared_ptr<AbstractLqpNode> TranslateExecute(const hsql::ExecuteStatement& execute_statement);

  std::shared_ptr<AbstractLqpNode> TranslateImport(const hsql::ImportStatement& import_statement);
  std::shared_ptr<AbstractLqpNode> TranslateExport(const hsql::ExportStatement& export_statement);

  std::shared_ptr<AbstractLqpNode> TranslatePredicateExpression(const std::shared_ptr<AbstractExpression>& expression,
                                                                std::shared_ptr<AbstractLqpNode> current_node) const;

  std::shared_ptr<AbstractLqpNode> TranslateShow(const hsql::ShowStatement& show_statement);

  std::shared_ptr<AbstractExpression> TranslateHsqlExpr(
      const hsql::Expr& expr, const std::shared_ptr<SqlIdentifierResolver>& sql_identifier_resolver);

  static std::shared_ptr<AbstractExpression> TranslateHsqlCase(
      const hsql::Expr& expr, const std::shared_ptr<SqlIdentifierResolver>& sql_identifier_resolver);

  static std::shared_ptr<AbstractExpression> InversePredicate(const AbstractExpression& expression);

  static std::shared_ptr<AbstractLqpNode> PruneExpressions(
      const std::shared_ptr<AbstractLqpNode>& node,
      const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  static std::shared_ptr<AbstractLqpNode> AddExpressionsIfUnavailable(
      const std::shared_ptr<AbstractLqpNode>& node,
      const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  static std::vector<std::shared_ptr<AbstractExpression>> UnwrapElements(
      const std::vector<SelectListElement>& select_list_elements);

 private:
  std::shared_ptr<AbstractCatalog> catalog_;
  std::shared_ptr<SqlIdentifierResolver> sql_identifier_resolver_;
  std::shared_ptr<SqlIdentifierResolverProxy> external_sql_identifier_resolver_proxy_;
  std::shared_ptr<ParameterIDAllocator> parameter_id_allocator_;
  std::unordered_map<std::string, std::shared_ptr<LqpWrapper>> views_;
  std::unordered_map<std::string, std::shared_ptr<LqpWrapper>> with_descriptions_;

  std::shared_ptr<AbstractLqpNode> current_lqp_;
  std::optional<TableSourceState> from_clause_result_;
  // "Inflated" because all wildcards will be inflated to the expressions they actually represent
  std::vector<SelectListElement> inflated_select_list_elements_;
};

}  // namespace skyrise
