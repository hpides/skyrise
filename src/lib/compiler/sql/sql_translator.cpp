/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "sql_translator.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <SQLParser.h>
#include <magic_enum.hpp>

#include "compiler/logical_query_plan/abstract_lqp_node.hpp"
#include "compiler/logical_query_plan/aggregate_node.hpp"
#include "compiler/logical_query_plan/alias_node.hpp"
#include "compiler/logical_query_plan/create_view_node.hpp"
#include "compiler/logical_query_plan/drop_view_node.hpp"
#include "compiler/logical_query_plan/dummy_table_node.hpp"
#include "compiler/logical_query_plan/export_node.hpp"
#include "compiler/logical_query_plan/import_node.hpp"
#include "compiler/logical_query_plan/join_node.hpp"
#include "compiler/logical_query_plan/limit_node.hpp"
#include "compiler/logical_query_plan/lqp_expression_utils.hpp"
#include "compiler/logical_query_plan/lqp_utils.hpp"
#include "compiler/logical_query_plan/predicate_node.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/logical_query_plan/sort_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "compiler/logical_query_plan/union_node.hpp"
#include "constant_mappings.hpp"
#include "create_sql_parser_error_message.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "import_export/file_type.hpp"
#include "types.hpp"

using namespace std::string_literals;            // NOLINT(google-build-using-namespace)
using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace {

using namespace skyrise;  // NOLINT(google-build-using-namespace)

const std::unordered_map<hsql::OperatorType, ArithmeticOperator> hsql_arithmetic_operators = {
    {hsql::kOpPlus, ArithmeticOperator::kAddition},           {hsql::kOpMinus, ArithmeticOperator::kSubtraction},
    {hsql::kOpAsterisk, ArithmeticOperator::kMultiplication}, {hsql::kOpSlash, ArithmeticOperator::kDivision},
    {hsql::kOpPercentage, ArithmeticOperator::kModulo},
};

const std::unordered_map<hsql::OperatorType, LogicalOperator> hsql_logical_operators = {
    {hsql::kOpAnd, LogicalOperator::kAnd}, {hsql::kOpOr, LogicalOperator::kOr}};

const std::unordered_map<hsql::OperatorType, PredicateCondition> hsql_predicate_condition = {
    {hsql::kOpBetween, PredicateCondition::kBetweenInclusive},
    {hsql::kOpEquals, PredicateCondition::kEquals},
    {hsql::kOpNotEquals, PredicateCondition::kNotEquals},
    {hsql::kOpLess, PredicateCondition::kLessThan},
    {hsql::kOpLessEq, PredicateCondition::kLessThanEquals},
    {hsql::kOpGreater, PredicateCondition::kGreaterThan},
    {hsql::kOpGreaterEq, PredicateCondition::kGreaterThanEquals},
    {hsql::kOpLike, PredicateCondition::kLike},
    {hsql::kOpNotLike, PredicateCondition::kNotLike},
    {hsql::kOpIsNull, PredicateCondition::kIsNull}};

const std::unordered_map<hsql::DatetimeField, DatetimeComponent> hsql_datetime_field = {
    {hsql::kDatetimeYear, DatetimeComponent::kYear},     {hsql::kDatetimeMonth, DatetimeComponent::kMonth},
    {hsql::kDatetimeDay, DatetimeComponent::kDay},       {hsql::kDatetimeHour, DatetimeComponent::kHour},
    {hsql::kDatetimeMinute, DatetimeComponent::kMinute}, {hsql::kDatetimeSecond, DatetimeComponent::kSecond},
};

const std::unordered_map<hsql::OrderType, SortMode> order_type_to_sort_mode = {
    {hsql::kOrderAsc, SortMode::kAscending},
    {hsql::kOrderDesc, SortMode::kDescending},
};

JoinMode translate_join_mode(const hsql::JoinType join_type) {
  static const std::unordered_map<const hsql::JoinType, const JoinMode> join_type_to_mode = {
      {hsql::kJoinInner, JoinMode::kInner},    {hsql::kJoinFull, JoinMode::kFullOuter},
      {hsql::kJoinLeft, JoinMode::kLeftOuter}, {hsql::kJoinRight, JoinMode::kRightOuter},
      {hsql::kJoinCross, JoinMode::kCross},
  };

  auto it = join_type_to_mode.find(join_type);
  Assert(it != join_type_to_mode.end(), "Unknown join type.");
  return it->second;
}

/**
 * Is the expression a predicate that our Join Operators can process directly?
 * That is, is it of the form <column> <predicate_condition> <column>?
 */
bool is_trivial_join_predicate(const AbstractExpression& expression, const AbstractLqpNode& left_input,
                               const AbstractLqpNode& right_input) {
  if (expression.type_ != ExpressionType::kPredicate) return false;

  const auto* binary_predicate_expression = dynamic_cast<const BinaryPredicateExpression*>(&expression);
  if (!binary_predicate_expression) return false;

  const auto left_in_left = left_input.FindColumnId(*binary_predicate_expression->LeftOperand());
  const auto right_in_right = right_input.FindColumnId(*binary_predicate_expression->RightOperand());
  const auto right_in_left = left_input.FindColumnId(*binary_predicate_expression->RightOperand());
  const auto left_in_right = right_input.FindColumnId(*binary_predicate_expression->LeftOperand());

  return (left_in_left && right_in_right) || (right_in_left && left_in_right);
}

}  // namespace

namespace skyrise {

SqlTranslator::SqlTranslator(std::shared_ptr<AbstractCatalog> catalog,
                             const std::unordered_map<std::string, std::shared_ptr<LqpWrapper>>& views)
    : SqlTranslator(std::move(catalog), nullptr, std::make_shared<ParameterIDAllocator>(), views,
                    std::unordered_map<std::string, std::shared_ptr<LqpWrapper>>{}) {}

SqlTranslator::SqlTranslator(std::shared_ptr<AbstractCatalog> catalog,
                             const std::shared_ptr<SqlIdentifierResolverProxy>& external_sql_identifier_resolver_proxy,
                             const std::shared_ptr<ParameterIDAllocator>& parameter_id_allocator,
                             const std::unordered_map<std::string, std::shared_ptr<LqpWrapper>>& views,
                             const std::unordered_map<std::string, std::shared_ptr<LqpWrapper>>& with_descriptions)
    : catalog_(std::move(catalog)),
      external_sql_identifier_resolver_proxy_(external_sql_identifier_resolver_proxy),
      parameter_id_allocator_(parameter_id_allocator),
      views_(views),
      with_descriptions_(with_descriptions) {}

SqlTranslationResult SqlTranslator::translate_parser_result(const hsql::SQLParserResult& result) {
  std::vector<std::shared_ptr<AbstractLqpNode>> result_nodes;
  const std::vector<hsql::SQLStatement*>& statements = result.getStatements();

  for (const hsql::SQLStatement* stmt : statements) {
    auto result_node = TranslateStatement(*stmt);
    result_nodes.push_back(result_node);
  }

  const auto& parameter_ids_of_value_placeholders = parameter_id_allocator_->value_placeholders();
  auto parameter_ids = std::vector<ParameterID>();
  parameter_ids.reserve(parameter_ids_of_value_placeholders.size());

  for (const auto& [value_placeholder_id, parameter_id] : parameter_ids_of_value_placeholders) {
    parameter_ids[value_placeholder_id] = parameter_id;
  }

  return {result_nodes, {parameter_ids}};
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateStatement(const hsql::SQLStatement& statement) {
  switch (statement.type()) {
    case hsql::kStmtSelect:
      return TranslateSelectStatement(static_cast<const hsql::SelectStatement&>(statement));
    case hsql::kStmtInsert:
      return TranslateInsert(static_cast<const hsql::InsertStatement&>(statement));
    case hsql::kStmtDelete:
      return TranslateDelete(static_cast<const hsql::DeleteStatement&>(statement));
    case hsql::kStmtUpdate:
      return TranslateUpdate(static_cast<const hsql::UpdateStatement&>(statement));
    case hsql::kStmtShow:
      return TranslateShow(static_cast<const hsql::ShowStatement&>(statement));
    case hsql::kStmtCreate:
      return TranslateCreate(static_cast<const hsql::CreateStatement&>(statement));
    case hsql::kStmtDrop:
      return TranslateDrop(static_cast<const hsql::DropStatement&>(statement));
    case hsql::kStmtPrepare:
      return TranslatePrepare(static_cast<const hsql::PrepareStatement&>(statement));
    case hsql::kStmtExecute:
      return TranslateExecute(static_cast<const hsql::ExecuteStatement&>(statement));
    case hsql::kStmtImport:
      return TranslateImport(static_cast<const hsql::ImportStatement&>(statement));
    case hsql::kStmtExport:
      return TranslateExport(static_cast<const hsql::ExportStatement&>(statement));
    case hsql::kStmtTransaction:
      // The transaction statements are handled directly in the SQLPipelineStatement,
      //  but the translation is still called, so we return a dummy node here.
      return DummyTableNode::Make();
    case hsql::kStmtAlter:
    case hsql::kStmtError:
    case hsql::kStmtRename:
      FailInput("Statement type not supported");
  }
  Fail("Invalid enum value");
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateSelectStatement(const hsql::SelectStatement& select) {
  // SQL Orders of Operations
  // 1. WITH clause
  // 2. FROM clause (incl. JOINs and sub-SELECTs that are part of this)
  // 3. SELECT list (to retrieve aliases)
  // 4. WHERE clause
  // 5. GROUP BY clause
  // 6. HAVING clause
  // 7. SELECT clause (incl. DISTINCT)
  // 8. ORDER BY clause
  // 9. LIMIT clause
  // 10. UNION/INTERSECT/EXCEPT clause
  // 11. UNION/INTERSECT/EXCEPT ORDER BY clause
  // 12. UNION/INTERSECT/EXCEPT LIMIT clause

  AssertInput(select.selectList, "SELECT list needs to exist");
  AssertInput(!select.selectList->empty(), "SELECT list needs to have entries");

  // Translate WITH clause
  if (select.withDescriptions) {
    for (const auto& with_description : *select.withDescriptions) {
      TranslateHsqlWithDescription(*with_description);
    }
  }

  // Translate FROM
  if (select.fromTable) {
    from_clause_result_ = TranslateTableRef(*select.fromTable);
    current_lqp_ = from_clause_result_->lqp;
    sql_identifier_resolver_ = from_clause_result_->sql_identifier_resolver;
  } else {
    current_lqp_ = std::make_shared<DummyTableNode>();
    sql_identifier_resolver_ = std::make_shared<SqlIdentifierResolver>();
  }

  // Translate SELECT list (to retrieve aliases)
  const auto select_list_elements = TranslateSelectList(*select.selectList);

  // Translate WHERE
  if (select.whereClause) {
    const auto where_expression = TranslateHsqlExpr(*select.whereClause, sql_identifier_resolver_);
    current_lqp_ = TranslatePredicateExpression(where_expression, current_lqp_);
  }

  // Translate SELECT, HAVING, GROUP BY in one go, as they are interdependent
  TranslateSelectGroupByHaving(select, select_list_elements);

  // Translate ORDER BY and LIMIT
  if (select.order) TranslateOrderBy(*select.order);
  if (select.limit) TranslateLimit(*select.limit);

  /**
   * Name, select and arrange the Columns as specified in the SELECT clause
   */
  // Only add a ProjectionNode if necessary
  const auto& inflated_select_list_expressions = UnwrapElements(inflated_select_list_elements_);
  if (!ExpressionsEqual(current_lqp_->OutputExpressions(), inflated_select_list_expressions)) {
    current_lqp_ = ProjectionNode::Make(inflated_select_list_expressions, current_lqp_);
  }

  // Check whether we need to create an AliasNode - this is the case whenever an Expression was assigned a column_name
  // that is not its generated name.
  auto need_alias_node = std::any_of(
      inflated_select_list_elements_.begin(), inflated_select_list_elements_.end(), [](const auto& element) {
        return std::any_of(element.identifiers.begin(), element.identifiers.end(), [&](const auto& identifier) {
          return identifier.column_name != element.expression->AsColumnName();
        });
      });

  if (need_alias_node) {
    std::vector<std::string> aliases;
    for (const auto& element : inflated_select_list_elements_) {
      if (!element.identifiers.empty()) {
        aliases.emplace_back(element.identifiers.back().column_name);
      } else {
        aliases.emplace_back(element.expression->AsColumnName());
      }
    }

    current_lqp_ = AliasNode::Make(UnwrapElements(inflated_select_list_elements_), aliases, current_lqp_);
  }

  if (select.setOperations) {
    for (const auto* const set_operator : *select.setOperations) {
      TranslateSetOperation(*set_operator);

      // In addition to local ORDER BY and LIMIT clauses, the result of the set operation(s) may have final clauses too.
      if (set_operator->resultOrder) TranslateOrderBy(*set_operator->resultOrder);
      if (set_operator->resultLimit) TranslateLimit(*set_operator->resultLimit);
    }
  }

  return current_lqp_;
}

void SqlTranslator::TranslateHsqlWithDescription(hsql::WithDescription& desc) {
  SqlTranslator with_translator{catalog_, nullptr, parameter_id_allocator_, views_, with_descriptions_};
  const auto lqp = with_translator.TranslateSelectStatement(*desc.select);

  // Save mappings: ColumnId -> ColumnName
  std::unordered_map<ColumnId, std::string> column_names;
  const auto output_expressions = lqp->OutputExpressions();
  for (auto column_id = ColumnId{0}; column_id < output_expressions.size(); ++column_id) {
    for (const auto& identifier : with_translator.inflated_select_list_elements_[column_id].identifiers) {
      column_names.insert_or_assign(column_id, identifier.column_name);
    }
  }

  // Store resolved WithDescription / temporary view
  const auto lqp_view = std::make_shared<LqpWrapper>(lqp, column_names);
  // A WITH description masks a preceding WITH description if their aliases are identical
  AssertInput(with_descriptions_.count(desc.alias) == 0, "Invalid redeclaration of WITH alias.");
  with_descriptions_.emplace(desc.alias, lqp_view);
}

SqlTranslator::TableSourceState SqlTranslator::TranslateTableRef(const hsql::TableRef& hsql_table_ref) {
  switch (hsql_table_ref.type) {
    case hsql::kTableName:
    case hsql::kTableSelect:
      return TranslateTableOrigin(hsql_table_ref);

    case hsql::kTableJoin:
      if (hsql_table_ref.join->type == hsql::kJoinNatural) {
        return TranslateNaturalJoin(*hsql_table_ref.join);
      } else {
        return TranslatePredicatedJoin(*hsql_table_ref.join);
      }

    case hsql::kTableCrossProduct:
      return TranslateCrossProduct(*hsql_table_ref.list);
  }
  Fail("Invalid enum value");
}

std::shared_ptr<AbstractExpression> SqlTranslator::translate_hsql_expr(const hsql::Expr& hsql_expr) {
  // Create an empty SqlIdentifier context - thus the expression cannot refer to any external columns
  // We do not pass catalog_ because it is not required to translate the Expression AST.
  return SqlTranslator{nullptr}.TranslateHsqlExpr(hsql_expr, std::make_shared<SqlIdentifierResolver>());
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateInsert(const hsql::InsertStatement& /* insert */) {
  Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateDelete(const hsql::DeleteStatement& /* delete_statement */) {
  Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateUpdate(const hsql::UpdateStatement& /* update */) {
  Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
}

SqlTranslator::TableSourceState SqlTranslator::TranslateTableOrigin(const hsql::TableRef& hsql_table_ref) {
  Assert(catalog_, "A catalog is required to resolve table and column names, but has not been set.");
  auto lqp = std::shared_ptr<AbstractLqpNode>{};

  // Each element in the FROM list needs to have a unique table name (i.e. Subqueries are required to have an ALIAS)
  auto table_name = std::string{};
  auto sql_identifier_resolver = std::make_shared<SqlIdentifierResolver>();
  std::vector<SelectListElement> select_list_elements;

  switch (hsql_table_ref.type) {
    case hsql::kTableName: {
      // WITH descriptions or subqueries are treated as though they were inline views or tables
      // They mask existing tables or views with the same name.
      const auto with_descriptions_iter = with_descriptions_.find(hsql_table_ref.name);
      if (with_descriptions_iter != with_descriptions_.end()) {
        const auto lqp_view = with_descriptions_iter->second->DeepCopy();
        lqp = lqp_view->lqp_;

        // Add all named columns to the IdentifierContext
        const auto output_expressions = lqp_view->lqp_->OutputExpressions();
        for (auto column_id = ColumnId{0}; column_id < output_expressions.size(); ++column_id) {
          const auto expression = output_expressions[column_id];

          const auto column_name_iter = lqp_view->column_names_.find(column_id);
          if (column_name_iter != lqp_view->column_names_.end()) {
            sql_identifier_resolver->AddColumnName(expression, column_name_iter->second);
          }
          sql_identifier_resolver->SetTableName(expression, hsql_table_ref.name);
        }

      } else if (catalog_->TableExists(hsql_table_ref.name)) {
        lqp = TranslateStoredTable(hsql_table_ref.name, sql_identifier_resolver);

      } else if (views_.find(hsql_table_ref.name) != views_.end()) {
        const auto view = views_.find(hsql_table_ref.name)->second;
        lqp = view->lqp_;

        /**
         * Add all named columns from the view to the IdentifierContext
         */
        const auto output_expressions = view->lqp_->OutputExpressions();
        for (auto column_id = ColumnId{0}; column_id < output_expressions.size(); ++column_id) {
          const auto expression = output_expressions[column_id];

          const auto column_name_iter = view->column_names_.find(column_id);
          if (column_name_iter != view->column_names_.end()) {
            sql_identifier_resolver->AddColumnName(expression, column_name_iter->second);
          }
          sql_identifier_resolver->SetTableName(expression, hsql_table_ref.name);
        }
      } else {
        FailInput(std::string("Did not find a table or view with name ") + hsql_table_ref.name);
      }
      table_name = hsql_table_ref.alias ? hsql_table_ref.alias->name : hsql_table_ref.name;

      for (const auto& expression : lqp->OutputExpressions()) {
        const auto identifiers = sql_identifier_resolver->GetExpressionIdentifiers(expression);
        select_list_elements.emplace_back(SelectListElement{expression, identifiers});
      }
    } break;

    case hsql::kTableSelect: {
      AssertInput(hsql_table_ref.alias && hsql_table_ref.alias->name, "Every nested SELECT must have its own alias");
      table_name = hsql_table_ref.alias->name;

      SqlTranslator subquery_translator{catalog_, external_sql_identifier_resolver_proxy_, parameter_id_allocator_,
                                        views_, with_descriptions_};
      lqp = subquery_translator.TranslateSelectStatement(*hsql_table_ref.select);

      std::vector<std::vector<SqlIdentifier>> identifiers;
      for (const auto& element : subquery_translator.inflated_select_list_elements_) {
        identifiers.emplace_back(element.identifiers);
      }

      const auto output_expressions = lqp->OutputExpressions();
      Assert(identifiers.size() == output_expressions.size(),
             "There have to be as many identifier lists as output expressions");
      for (auto select_list_element_idx = size_t{0}; select_list_element_idx < output_expressions.size();
           ++select_list_element_idx) {
        const auto subquery_expression = output_expressions[select_list_element_idx];

        // Make sure each column from the Subquery has a name
        if (identifiers.empty()) {
          sql_identifier_resolver->AddColumnName(subquery_expression, subquery_expression->AsColumnName());
        }
        for (const auto& identifier : identifiers[select_list_element_idx]) {
          sql_identifier_resolver->AddColumnName(subquery_expression, identifier.column_name);
        }

        select_list_elements.emplace_back(SelectListElement{subquery_expression, identifiers[select_list_element_idx]});
      }

      table_name = hsql_table_ref.alias->name;
    } break;

    case hsql::kTableJoin:
    case hsql::kTableCrossProduct:
      // These should not make it this far.
      Fail("Unexpected table reference type");
  }

  // Rename columns as in "SELECT * FROM t AS x (y,z)"
  if (hsql_table_ref.alias && hsql_table_ref.alias->columns) {
    const auto& output_expressions = lqp->OutputExpressions();

    AssertInput(hsql_table_ref.alias->columns->size() == output_expressions.size(),
                "Must specify a name for exactly each column");
    Assert(hsql_table_ref.alias->columns->size() == select_list_elements.size(),
           "There have to be as many aliases as output expressions");

    std::set<std::shared_ptr<AbstractExpression>> renamed_expressions;
    for (auto column_id = ColumnId{0}; column_id < hsql_table_ref.alias->columns->size(); ++column_id) {
      const auto& expression = output_expressions[column_id];

      if (renamed_expressions.find(expression) == renamed_expressions.end()) {
        // The original column names should not be accessible anymore because the table schema is renamed.
        sql_identifier_resolver->ResetColumnNames(expression);
        renamed_expressions.insert(expression);
      }

      const auto& column_name = (*hsql_table_ref.alias->columns)[column_id];
      sql_identifier_resolver->AddColumnName(expression, column_name);
      select_list_elements[column_id].identifiers.clear();
      select_list_elements[column_id].identifiers.emplace_back(column_name);
    }
  }

  for (const auto& expression : lqp->OutputExpressions()) {
    sql_identifier_resolver->SetTableName(expression, table_name);
  }

  return {lqp,
          {{
              {table_name, select_list_elements},
          }},
          {select_list_elements},
          sql_identifier_resolver};
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateStoredTable(
    const std::string& name, const std::shared_ptr<SqlIdentifierResolver>& sql_identifier_resolver) {
  AssertInput(catalog_->TableExists(name), std::string{"Did not find a table with name "} + name);

  const auto stored_table_node = StoredTableNode::Make(name, catalog_);

  // Publish the columns of the table in the SqlIdentifierResolver
  const auto table_schema = catalog_->GetTableSchema(name);

  Assert(table_schema != nullptr, "Table schema is missing.");
  Assert(table_schema->TableColumnCount() > 0, "Table schema has no columns.");
  for (auto column_id = ColumnId{0}; column_id < table_schema->TableColumnCount(); ++column_id) {
    const auto& column_definition = table_schema->GetTableColumnDefinition(column_id);
    const auto column_expression = std::make_shared<LqpColumnExpression>(stored_table_node, column_id);
    sql_identifier_resolver->AddColumnName(column_expression, column_definition.name);
    sql_identifier_resolver->SetTableName(column_expression, name);
  }

  return stored_table_node;
}

SqlTranslator::TableSourceState SqlTranslator::TranslatePredicatedJoin(const hsql::JoinDefinition& join) {
  const auto join_mode = translate_join_mode(join.type);

  auto left_state = TranslateTableRef(*join.left);
  auto right_state = TranslateTableRef(*join.right);

  auto left_input_lqp = left_state.lqp;
  auto right_input_lqp = right_state.lqp;

  // left_state becomes the result state
  auto result_state = std::move(left_state);
  result_state.Append(std::move(right_state));

  /**
   * Hyrise doesn't have support for complex join predicates in OUTER JOINs
   * The current implementation expects a single join condition in a set of conjunctive
   * clauses. The remaining clauses are expected to be relevant for only one of
   * the join partners and are therefore converted into predicates inserted in between the
   * source relations and the actual join node.
   * See TPC-H 13 for an example query.
   */
  const auto raw_join_predicate = TranslateHsqlExpr(*join.condition, result_state.sql_identifier_resolver);
  const auto raw_join_predicate_cnf = FlattenLogicalExpressions(raw_join_predicate, LogicalOperator::kAnd);

  auto left_local_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto right_local_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto join_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};

  for (const auto& predicate : raw_join_predicate_cnf) {
    if (ExpressionEvaluableOnLqp(predicate, *left_input_lqp)) {
      left_local_predicates.emplace_back(predicate);
    } else if (ExpressionEvaluableOnLqp(predicate, *right_input_lqp)) {
      right_local_predicates.emplace_back(predicate);
    } else {
      // Accept any kind of predicate here and let the LqpTranslator fail on those that it doesn't support
      join_predicates.emplace_back(predicate);
    }
  }

  AssertInput(join_mode != JoinMode::kFullOuter || (left_local_predicates.empty() && right_local_predicates.empty()),
              "Local predicates not supported for full outer joins. See Hyrise #1436.");
  AssertInput(join_mode != JoinMode::kLeftOuter || left_local_predicates.empty(),
              "Local predicates not supported on left side of left outer join. See Hyrise #1436.");
  AssertInput(join_mode != JoinMode::kRightOuter || right_local_predicates.empty(),
              "Local predicates not supported on right side of right outer join. See Hyrise #1436.");

  /**
   * Add local predicates - ignore local predicates on the preserving side of OUTER JOINs
   */
  if (join_mode != JoinMode::kLeftOuter && join_mode != JoinMode::kFullOuter) {
    for (const auto& left_local_predicate : left_local_predicates) {
      left_input_lqp = TranslatePredicateExpression(left_local_predicate, left_input_lqp);
    }
  }
  if (join_mode != JoinMode::kRightOuter && join_mode != JoinMode::kFullOuter) {
    for (const auto& right_local_predicate : right_local_predicates) {
      right_input_lqp = TranslatePredicateExpression(right_local_predicate, right_input_lqp);
    }
  }

  /**
   * Add the join predicates
   */
  auto lqp = std::shared_ptr<AbstractLqpNode>{};

  if (join_mode != JoinMode::kInner && join_predicates.size() > 1) {
    lqp = JoinNode::Make(join_mode, join_predicates, left_input_lqp, right_input_lqp);
  } else {
    const auto join_predicate_iter =
        std::find_if(join_predicates.begin(), join_predicates.end(), [&](const auto& join_predicate) {
          return is_trivial_join_predicate(*join_predicate, *left_input_lqp, *right_input_lqp);
        });

    // Inner Joins with predicates like `5 + t0.a = 6+ t1.b` can be supported via Cross join + Scan. For all other join
    // modes such predicates are not supported.
    AssertInput(join_mode == JoinMode::kInner || join_predicate_iter != join_predicates.end(),
                "Non column-to-column comparison in join predicate only supported for inner joins");

    if (join_predicate_iter == join_predicates.end()) {
      lqp = JoinNode::Make(JoinMode::kCross, left_input_lqp, right_input_lqp);
    } else {
      lqp = JoinNode::Make(join_mode, *join_predicate_iter, left_input_lqp, right_input_lqp);
      join_predicates.erase(join_predicate_iter);
    }

    // Add secondary join predicates as normal PredicateNodes
    for (const auto& join_predicate : join_predicates) {
      lqp = TranslatePredicateExpression(join_predicate, lqp);
    }
  }

  result_state.lqp = lqp;
  return result_state;
}

SqlTranslator::TableSourceState SqlTranslator::TranslateNaturalJoin(const hsql::JoinDefinition& join) {
  Assert(join.type == hsql::kJoinNatural, "join must be a natural join");

  auto left_state = TranslateTableRef(*join.left);
  auto right_state = TranslateTableRef(*join.right);

  const auto left_sql_identifier_resolver = left_state.sql_identifier_resolver;
  const auto right_sql_identifier_resolver = right_state.sql_identifier_resolver;
  const auto left_input_lqp = left_state.lqp;
  const auto right_input_lqp = right_state.lqp;

  auto join_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto result_state = std::move(left_state);

  // a) Find matching columns and create JoinPredicates from them
  // b) Add columns from right input to the output when they have no match in the left input
  for (const auto& right_element : right_state.elements_in_order) {
    const auto& right_expression = right_element.expression;
    const auto& right_identifiers = right_element.identifiers;

    if (!right_identifiers.empty()) {
      // Ignore previous names if there is an alias
      const auto right_identifier = right_identifiers.back();

      const auto left_expression =
          left_sql_identifier_resolver->ResolveIdentifierRelaxed({right_identifier.column_name});

      if (left_expression) {
        // Two columns match, let's join on them.
        join_predicates.emplace_back(std::make_shared<BinaryPredicateExpression>(PredicateCondition::kEquals,
                                                                                 left_expression, right_expression));
        continue;
      }

      // No matching column in the left input found, add the column from the right input to the output
      result_state.elements_in_order.emplace_back(right_element);
      result_state.sql_identifier_resolver->AddColumnName(right_expression, right_identifier.column_name);
      if (right_identifier.table_name) {
        result_state.elements_by_table_name[*right_identifier.table_name].emplace_back(right_element);
        result_state.sql_identifier_resolver->SetTableName(right_expression, *right_identifier.table_name);
      }
    }
  }

  auto lqp = std::shared_ptr<AbstractLqpNode>();

  if (join_predicates.empty()) {
    // No matching columns? Then the NATURAL JOIN becomes a Cross Join
    lqp = JoinNode::Make(JoinMode::kCross, left_input_lqp, right_input_lqp);
  } else {
    // Turn one of the Join Predicates into an actual join
    lqp = JoinNode::Make(JoinMode::kInner, join_predicates.front(), left_input_lqp, right_input_lqp);
  }

  // Add remaining join predicates as normal predicates
  for (auto join_predicate_idx = size_t{1}; join_predicate_idx < join_predicates.size(); ++join_predicate_idx) {
    lqp = PredicateNode::Make(join_predicates[join_predicate_idx], lqp);
  }

  if (!join_predicates.empty()) {
    // Projection Node to remove duplicate columns
    lqp = ProjectionNode::Make(UnwrapElements(result_state.elements_in_order), lqp);
  }

  // Create output TableSourceState
  result_state.lqp = lqp;

  return result_state;
}

SqlTranslator::TableSourceState SqlTranslator::TranslateCrossProduct(const std::vector<hsql::TableRef*>& tables) {
  Assert(!tables.empty(), "Cannot translate cross product without tables");

  auto result_table_source_state = TranslateTableRef(*tables.front());

  for (auto table_idx = size_t{1}; table_idx < tables.size(); ++table_idx) {
    auto table_source_state = TranslateTableRef(*tables[table_idx]);
    result_table_source_state.lqp =
        JoinNode::Make(JoinMode::kCross, result_table_source_state.lqp, table_source_state.lqp);
    result_table_source_state.Append(std::move(table_source_state));
  }

  return result_table_source_state;
}

std::vector<SqlTranslator::SelectListElement> SqlTranslator::TranslateSelectList(
    const std::vector<hsql::Expr*>& select_list) {
  // Build the select_list_elements
  // Each expression of a select_list_element is either an Expression or nullptr if the element is a Wildcard
  // Create an SqlIdentifierResolver that knows the aliases
  std::vector<SelectListElement> select_list_elements;
  auto post_select_sql_identifier_resolver = std::make_shared<SqlIdentifierResolver>(*sql_identifier_resolver_);
  for (const auto& hsql_select_expr : select_list) {
    if (hsql_select_expr->type == hsql::kExprStar) {
      select_list_elements.emplace_back(SelectListElement{nullptr});
    } else {
      auto expression = TranslateHsqlExpr(*hsql_select_expr, sql_identifier_resolver_);
      select_list_elements.emplace_back(SelectListElement{expression});
      if (hsql_select_expr->name && hsql_select_expr->type != hsql::kExprFunctionRef &&
          hsql_select_expr->type != hsql::kExprExtract) {
        select_list_elements.back().identifiers.emplace_back(hsql_select_expr->name);
      }

      if (hsql_select_expr->alias) {
        auto identifier = SqlIdentifier{hsql_select_expr->alias};
        if (hsql_select_expr->table) {
          identifier.table_name = hsql_select_expr->table;
        }
        post_select_sql_identifier_resolver->AddColumnName(expression, hsql_select_expr->alias);
        select_list_elements.back().identifiers.emplace_back(identifier);
      }
    }
  }
  sql_identifier_resolver_ = post_select_sql_identifier_resolver;
  return select_list_elements;
}

void SqlTranslator::TranslateSelectGroupByHaving(const hsql::SelectStatement& select,
                                                 const std::vector<SelectListElement>& select_list_elements) {
  auto pre_aggregate_expression_set = ExpressionUnorderedSet{};
  auto pre_aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto aggregate_expression_set = ExpressionUnorderedSet{};
  auto aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};

  // Visitor that identifies still uncomputed AggregateExpressions and their arguments.
  const auto find_uncomputed_aggregates_and_arguments = [&](auto& sub_expression) {
    /**
     * If the AggregateExpression has already been computed in a previous node (consider "x" in
     * "SELECT x FROM (SELECT MIN(a) as x FROM t) AS y)", it doesn't count as a new Aggregate and is therefore not
     * considered an "Aggregate" in the current SELECT list. Handling this as a special case seems hacky to me as well,
     * but it's the best solution I can come up with right now.
     */
    if (current_lqp_->FindColumnId(*sub_expression)) return ExpressionVisitation::kDoNotVisitArguments;

    if (sub_expression->type_ != ExpressionType::kAggregate) return ExpressionVisitation::kVisitArguments;

    auto aggregate_expression = std::static_pointer_cast<AggregateExpression>(sub_expression);
    if (aggregate_expression_set.emplace(aggregate_expression).second) {
      aggregate_expressions.emplace_back(aggregate_expression);
      for (const auto& argument : aggregate_expression->arguments_) {
        if (pre_aggregate_expression_set.emplace(argument).second) {
          // Handle COUNT(*)
          const auto* const column_expression = dynamic_cast<const LqpColumnExpression*>(&*argument);
          if (!column_expression || column_expression->original_column_id_ != kInvalidColumnId) {
            pre_aggregate_expressions.emplace_back(argument);
          }
        }
      }
    }

    return ExpressionVisitation::kDoNotVisitArguments;
  };

  // Identify all Aggregates and their arguments needed for SELECT
  for (const auto& element : select_list_elements) {
    if (element.expression) {
      VisitExpression(element.expression, find_uncomputed_aggregates_and_arguments);
    }
  }

  // Identify all GROUP BY expressions
  auto group_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  if (select.groupBy && select.groupBy->columns) {
    group_by_expressions.reserve(select.groupBy->columns->size());
    for (const auto* group_by_hsql_expr : *select.groupBy->columns) {
      const auto group_by_expression = TranslateHsqlExpr(*group_by_hsql_expr, sql_identifier_resolver_);
      group_by_expressions.emplace_back(group_by_expression);
      if (pre_aggregate_expression_set.emplace(group_by_expression).second) {
        pre_aggregate_expressions.emplace_back(group_by_expression);
      }
    }
  }

  // Gather all aggregates and arguments from HAVING
  auto having_expression = std::shared_ptr<AbstractExpression>{};
  if (select.groupBy && select.groupBy->having) {
    having_expression = TranslateHsqlExpr(*select.groupBy->having, sql_identifier_resolver_);
    VisitExpression(having_expression, find_uncomputed_aggregates_and_arguments);
  }

  const auto is_aggregate = !aggregate_expressions.empty() || !group_by_expressions.empty();

  const auto pre_aggregate_lqp = current_lqp_;

  // Build Aggregate
  if (is_aggregate) {
    // If needed, add a Projection to evaluate all Expression required for GROUP BY/Aggregates
    if (!pre_aggregate_expressions.empty()) {
      const auto any_expression_not_yet_available =
          std::any_of(pre_aggregate_expressions.begin(), pre_aggregate_expressions.end(),
                      [&](const auto& expression) { return !current_lqp_->FindColumnId(*expression); });

      if (any_expression_not_yet_available) {
        current_lqp_ = ProjectionNode::Make(pre_aggregate_expressions, current_lqp_);
      }
    }
    current_lqp_ = AggregateNode::Make(group_by_expressions, aggregate_expressions, current_lqp_);
  }

  // Build Having
  if (having_expression) {
    AssertInput(ExpressionEvaluableOnLqp(having_expression, *current_lqp_),
                "HAVING references columns not accessible after Aggregation");
    current_lqp_ = TranslatePredicateExpression(having_expression, current_lqp_);
  }

  for (auto select_list_idx = size_t{0}; select_list_idx < select.selectList->size(); ++select_list_idx) {
    const auto* hsql_expr = (*select.selectList)[select_list_idx];

    if (hsql_expr->type == hsql::kExprStar) {
      AssertInput(from_clause_result_, "Can't SELECT with wildcards since there are no FROM tables specified");

      if (is_aggregate) {
        // SELECT * is only valid if every input column is named in the GROUP BY clause
        for (const auto& pre_aggregate_expression : pre_aggregate_lqp->OutputExpressions()) {
          if (hsql_expr->table) {
            // Dealing with SELECT t.* here
            auto identifiers = sql_identifier_resolver_->GetExpressionIdentifiers(pre_aggregate_expression);
            if (std::any_of(identifiers.begin(), identifiers.end(),
                            [&](const auto& identifier) { return identifier.table_name != hsql_expr->table; })) {
              // The pre_aggregate_expression may or may not be part of the GROUP BY clause, but since it comes from a
              // different table, it is not included in the `SELECT t.*`.
              continue;
            }
          }

          AssertInput(std::find_if(group_by_expressions.begin(), group_by_expressions.end(),
                                   [&](const auto& group_by_expression) {
                                     return *pre_aggregate_expression == *group_by_expression;
                                   }) != group_by_expressions.end(),
                      std::string("Expression ") + pre_aggregate_expression->AsColumnName() +
                          " was added to SELECT list when resolving *, but it is not part of the GROUP BY clause");
        }
      }

      if (hsql_expr->table) {
        if (is_aggregate) {
          // Select all GROUP BY columns with the specified table name
          for (const auto& group_by_expression : group_by_expressions) {
            const auto identifiers = sql_identifier_resolver_->GetExpressionIdentifiers(group_by_expression);
            for (const auto& identifier : identifiers) {
              if (identifier.table_name == hsql_expr->table) {
                inflated_select_list_elements_.emplace_back(SelectListElement{group_by_expression});
              }
            }
          }
        } else {
          // Select all columns from the FROM element with the specified name
          const auto from_element_iter = from_clause_result_->elements_by_table_name.find(hsql_expr->table);
          AssertInput(from_element_iter != from_clause_result_->elements_by_table_name.end(),
                      std::string("No such element in FROM with table name '") + hsql_expr->table + "'");

          for (const auto& element : from_element_iter->second) {
            inflated_select_list_elements_.emplace_back(element);
          }
        }
      } else {
        if (is_aggregate) {
          // Select all GROUP BY columns
          for (const auto& expression : group_by_expressions) {
            inflated_select_list_elements_.emplace_back(SelectListElement{expression});
          }
        } else {
          // Select all columns from the FROM elements
          inflated_select_list_elements_.insert(inflated_select_list_elements_.end(),
                                                from_clause_result_->elements_in_order.begin(),
                                                from_clause_result_->elements_in_order.end());
        }
      }
    } else {
      inflated_select_list_elements_.emplace_back(select_list_elements[select_list_idx]);
    }
  }

  // For SELECT DISTINCT, we add an aggregate node that groups by all output columns, but doesn't use any aggregate
  // functions, e.g.: `SELECT DISTINCT a, b ...` becomes  `SELECT a, b ... GROUP BY a, b`.
  //
  // This might create unnecessary aggregate nodes when we already have an aggregation that creates unique results:
  // `SELECT DISTINCT a, MIN(b) FROM t GROUP BY a` would have one aggregate that groups by a and calculates MIN(b), and
  // one that groups by both a and MIN(b) without calculating anything. Fixing this should be done by an optimizer rule
  // that checks for each GROUP BY whether it guarantees the results to be unique or not. Doable, but no priority.
  if (select.selectDistinct) {
    current_lqp_ = AggregateNode::Make(UnwrapElements(inflated_select_list_elements_),
                                       std::vector<std::shared_ptr<AbstractExpression>>{}, current_lqp_);
  }
}

void SqlTranslator::TranslateSetOperation(const hsql::SetOperation& set_operator) {
  const auto& left_input_lqp = current_lqp_;
  const auto left_output_expressions = left_input_lqp->OutputExpressions();

  // The right-hand side of the set operation has to be translated independently and must not access SQL identifiers
  // from the left-hand side. To ensure this, we create a new SqlTranslator with its own SqlIdentifierResolver.
  SqlTranslator nested_set_translator{catalog_, external_sql_identifier_resolver_proxy_, parameter_id_allocator_,
                                      views_, with_descriptions_};
  const auto right_input_lqp = nested_set_translator.TranslateSelectStatement(*set_operator.nestedSelectStatement);
  const auto right_output_expressions = right_input_lqp->OutputExpressions();

  AssertInput(left_output_expressions.size() == right_output_expressions.size(),
              "Mismatching number of input columns for set operation");

  // Check to see if both input LQPs use the same data type for each column
  for (auto expression_idx = size_t{0}; expression_idx < left_output_expressions.size(); ++expression_idx) {
    const auto& left_expression = left_output_expressions[expression_idx];
    const auto& right_expression = right_output_expressions[expression_idx];

    AssertInput(left_expression->GetDataType() == right_expression->GetDataType(),
                "Mismatching input data types for left and right side of set operation");
  }

  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores): Now unused because the implementation, down below, was removed.
  auto lqp = std::shared_ptr<AbstractLqpNode>();

  // Create corresponding node depending on the SetType
  switch (set_operator.setType) {
    case hsql::kSetExcept:
      Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
      break;
    case hsql::kSetIntersect:
      Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
      break;
    case hsql::kSetUnion:
      Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
      break;
  }

  current_lqp_ = lqp;
}

void SqlTranslator::TranslateOrderBy(const std::vector<hsql::OrderDescription*>& order_list) {
  if (order_list.empty()) return;

  // So we can later reset the available Expressions to the Expressions of this LQP
  const auto input_lqp = current_lqp_;

  std::vector<std::shared_ptr<AbstractExpression>> expressions(order_list.size());
  std::vector<SortMode> sort_modes(order_list.size());
  for (auto expression_idx = size_t{0}; expression_idx < order_list.size(); ++expression_idx) {
    const auto& order_description = order_list[expression_idx];
    expressions[expression_idx] = TranslateHsqlExpr(*order_description->expr, sql_identifier_resolver_);
    sort_modes[expression_idx] = order_type_to_sort_mode.at(order_description->type);
  }

  current_lqp_ = AddExpressionsIfUnavailable(current_lqp_, expressions);

  current_lqp_ = SortNode::Make(expressions, sort_modes, current_lqp_);

  // If any Expressions were added to perform the sorting, remove them again
  const auto input_output_expressions = input_lqp->OutputExpressions();
  if (input_output_expressions.size() != current_lqp_->OutputExpressions().size()) {
    current_lqp_ = ProjectionNode::Make(input_output_expressions, current_lqp_);
  }
}

void SqlTranslator::TranslateLimit(const hsql::LimitDescription& limit) {
  AssertInput(!limit.offset, "OFFSET not supported");
  const auto num_rows_expression = TranslateHsqlExpr(*limit.limit, sql_identifier_resolver_);
  current_lqp_ = LimitNode::Make(num_rows_expression, current_lqp_);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateShow(const hsql::ShowStatement& /* show_statement */) {
  Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateCreate(const hsql::CreateStatement& create_statement) {
  switch (create_statement.type) {
    case hsql::CreateType::kCreateView:
      return TranslateCreateView(create_statement);
    case hsql::CreateType::kCreateTable:
      return TranslateCreateTable(create_statement);
    case hsql::CreateType::kCreateTableFromTbl:
      FailInput("CREATE TABLE FROM is not yet supported");
  }
  Fail("Invalid enum value");
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateCreateView(const hsql::CreateStatement& create_statement) {
  auto lqp = TranslateSelectStatement(static_cast<const hsql::SelectStatement&>(*create_statement.select));
  const auto output_expressions = lqp->OutputExpressions();

  std::unordered_map<ColumnId, std::string> column_names;

  if (create_statement.viewColumns) {
    // The CREATE VIEW statement has renamed the columns: CREATE VIEW myview (foo, bar) AS SELECT ...
    AssertInput(create_statement.viewColumns->size() == output_expressions.size(),
                "Number of Columns in CREATE VIEW does not match SELECT statement");

    for (auto column_id = ColumnId{0}; column_id < create_statement.viewColumns->size(); ++column_id) {
      column_names.insert_or_assign(column_id, (*create_statement.viewColumns)[column_id]);
    }
  } else {
    for (auto column_id = ColumnId{0}; column_id < output_expressions.size(); ++column_id) {
      for (const auto& identifier : inflated_select_list_elements_[column_id].identifiers) {
        column_names.insert_or_assign(column_id, identifier.column_name);
      }
    }
  }

  return CreateViewNode::Make(create_statement.tableName, std::make_shared<LqpWrapper>(lqp, column_names),
                              create_statement.ifNotExists);
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateCreateTable(
    const hsql::CreateStatement& /* create_statement */) {
  Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateDrop(const hsql::DropStatement& drop_statement) {
  switch (drop_statement.type) {
    case hsql::DropType::kDropView:
      return DropViewNode::Make(drop_statement.name, drop_statement.ifExists);
    case hsql::DropType::kDropTable:
    case hsql::DropType::kDropSchema:
    case hsql::DropType::kDropIndex:
    case hsql::DropType::kDropPreparedStatement:
      FailInput("This DROP type is not implemented yet");
  }
  Fail("Invalid enum value");
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslatePrepare(
    const hsql::PrepareStatement& /* prepare_statement */) {
  Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateExecute(
    const hsql::ExecuteStatement& /* execute_statement */) {
  Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateImport(const hsql::ImportStatement& import_statement) {
  return ImportNode::Make(import_statement.tableName, import_statement.filePath,
                          import_type_to_file_type(import_statement.type));
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslateExport(const hsql::ExportStatement& export_statement) {
  // Get stored table as input
  auto sql_identifier_resolver = std::make_shared<SqlIdentifierResolver>();
  auto lqp = TranslateStoredTable(export_statement.tableName, sql_identifier_resolver);

  return ExportNode::Make(export_statement.tableName, export_statement.filePath,
                          import_type_to_file_type(export_statement.type), lqp);
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::TranslatePredicateExpression(
    const std::shared_ptr<AbstractExpression>& expression, std::shared_ptr<AbstractLqpNode> current_node) const {
  /**
   * Translate AbstractPredicateExpression
   */
  switch (expression->type_) {
    case ExpressionType::kPredicate: {
      const auto predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(expression);
      return PredicateNode::Make(expression, current_node);
    }

    case ExpressionType::kLogical: {
      const auto logical_expression = std::static_pointer_cast<LogicalExpression>(expression);

      switch (logical_expression->logical_operator_) {
        case LogicalOperator::kAnd: {
          current_node = TranslatePredicateExpression(logical_expression->RightOperand(), current_node);
          return TranslatePredicateExpression(logical_expression->LeftOperand(), current_node);
        }
        case LogicalOperator::kOr:
          return PredicateNode::Make(expression, current_node);
      }
    } break;

    case ExpressionType::kExists:
      return PredicateNode::Make(expression, current_node);

    default:
      FailInput("Cannot use this ExpressionType as predicate");
  }

  Fail("Invalid enum value");
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::PruneExpressions(
    const std::shared_ptr<AbstractLqpNode>& node, const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  if (ExpressionsEqual(node->OutputExpressions(), expressions)) return node;
  return ProjectionNode::Make(expressions, node);
}

std::shared_ptr<AbstractLqpNode> SqlTranslator::AddExpressionsIfUnavailable(
    const std::shared_ptr<AbstractLqpNode>& node, const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  std::vector<std::shared_ptr<AbstractExpression>> projection_expressions;

  for (const auto& expression : expressions) {
    // The required expression is already available or doesn't need to be computed (e.g. when it is a literal)
    if (!expression->RequiresComputation() || node->FindColumnId(*expression)) continue;
    projection_expressions.emplace_back(expression);
  }

  // If all requested expressions are available, no need to create a projection
  if (projection_expressions.empty()) return node;

  const auto output_expressions = node->OutputExpressions();
  projection_expressions.insert(projection_expressions.end(), output_expressions.cbegin(), output_expressions.cend());

  return ProjectionNode::Make(projection_expressions, node);
}

std::shared_ptr<AbstractExpression> SqlTranslator::TranslateHsqlExpr(
    const hsql::Expr& expr, const std::shared_ptr<SqlIdentifierResolver>& sql_identifier_resolver) {
  auto name = expr.name ? std::string(expr.name) : "";

  const auto left = expr.expr ? TranslateHsqlExpr(*expr.expr, sql_identifier_resolver) : nullptr;
  const auto right = expr.expr2 ? TranslateHsqlExpr(*expr.expr2, sql_identifier_resolver) : nullptr;

  switch (expr.type) {
    case hsql::kExprColumnRef: {
      const auto table_name = expr.table ? std::optional<std::string>(std::string(expr.table)) : std::nullopt;
      const auto identifier = SqlIdentifier{name, table_name};

      auto expression = sql_identifier_resolver->ResolveIdentifierRelaxed(identifier);
      if (!expression && external_sql_identifier_resolver_proxy_) {
        // Try to resolve the identifier in the outer queries
        expression = external_sql_identifier_resolver_proxy_->ResolveIdentifierRelaxed(identifier);
      }
      AssertInput(expression, "Couldn't resolve identifier '" + identifier.AsString() + "' or it is ambiguous");

      return expression;
    }

    case hsql::kExprLiteralFloat:
      return std::make_shared<ValueExpression>(expr.fval);

    case hsql::kExprLiteralString:
      AssertInput(expr.name, "No value given for string literal");
      return std::make_shared<ValueExpression>(std::string{name});

    case hsql::kExprLiteralInt:
      if (static_cast<int32_t>(expr.ival) == expr.ival) {
        return std::make_shared<ValueExpression>(static_cast<int32_t>(expr.ival));
      } else {
        return std::make_shared<ValueExpression>(expr.ival);
      }

    case hsql::kExprLiteralNull:
      return std::make_shared<ValueExpression>(NullValue{});

    case hsql::kExprParameter: {
      Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
    }

    case hsql::kExprExtract: {
      Assert(expr.datetimeField != hsql::kDatetimeNone, "No DatetimeField specified in EXTRACT. Bug in sqlparser?");

      auto datetime_component = hsql_datetime_field.at(expr.datetimeField);
      return std::make_shared<ExtractExpression>(datetime_component, left);
    }

    case hsql::kExprFunctionRef: {
      // convert to upper-case to find mapping
      std::transform(name.begin(), name.end(), name.begin(), [](const auto c) { return std::toupper(c); });

      // Some SQL functions have aliases, which we map to one unique identifier here.
      static const std::unordered_map<std::string, std::string> function_aliases{{{"SUBSTRING"}, {"SUBSTR"}}};
      const auto found_alias = function_aliases.find(name);
      if (found_alias != function_aliases.end()) {
        name = found_alias->second;
      }

      Assert(expr.exprList, "FunctionRef has no exprList. Bug in sqlparser?");

      /**
       * Aggregate function
       */
      const auto aggregate_iter = kAggregateFunctionToString.right.find(name);
      if (aggregate_iter != kAggregateFunctionToString.right.end()) {
        auto aggregate_function = aggregate_iter->second;

        if (aggregate_function == AggregateFunction::kCount && expr.distinct) {
          aggregate_function = AggregateFunction::kCountDistinct;
        }

        AssertInput(expr.exprList && expr.exprList->size() == 1,
                    "Expected exactly one argument for this AggregateFunction");

        auto aggregate_expression = std::shared_ptr<AggregateExpression>{};

        switch (aggregate_function) {
          case AggregateFunction::kMin:
          case AggregateFunction::kMax:
          case AggregateFunction::kSum:
          case AggregateFunction::kAvg:
          case AggregateFunction::kStandardDeviationSample: {
            aggregate_expression = std::make_shared<AggregateExpression>(
                aggregate_function, TranslateHsqlExpr(*expr.exprList->front(), sql_identifier_resolver));
          } break;
          case AggregateFunction::kAny:
            Fail("ANY() is an internal aggregation function.");
          case AggregateFunction::kCount:
          case AggregateFunction::kCountDistinct: {
            if (expr.exprList->front()->type == hsql::kExprStar) {
              AssertInput(!expr.exprList->front()->name, "Illegal <t>.* in COUNT()");

              // Find any leaf node below COUNT(*)
              std::shared_ptr<AbstractLqpNode> leaf_node = nullptr;
              VisitLqp(current_lqp_, [&](const auto& node) {
                if (!node->LeftInput() && !node->RightInput()) {
                  leaf_node = node;
                  return LqpVisitation::kDoNotVisitInputs;
                }
                return LqpVisitation::kVisitInputs;
              });
              Assert(leaf_node, "No leaf node found below COUNT(*)");

              const auto column_expression = std::make_shared<LqpColumnExpression>(leaf_node, kInvalidColumnId);

              aggregate_expression = std::make_shared<AggregateExpression>(aggregate_function, column_expression);
            } else {
              aggregate_expression = std::make_shared<AggregateExpression>(
                  aggregate_function, TranslateHsqlExpr(*expr.exprList->front(), sql_identifier_resolver));
            }
          } break;
        }

        // Check that the aggregate can be calculated on the given expression
        const auto aggregate_data_type = aggregate_expression->GetDataType();
        AssertInput(aggregate_data_type != DataType::kNull,
                    std::string{"Invalid aggregate "} + aggregate_expression->AsColumnName() + " for input data type " +
                        std::string{magic_enum::enum_name(aggregate_expression->Argument()->GetDataType())});

        // Check for ambiguous expressions that occur both at the current node and in its input tables. Example:
        //   SELECT COUNT(a) FROM (SELECT a, COUNT(a) FROM t GROUP BY a) t2
        // Our current expression system cannot handle this case and would consider the two COUNT(a) to be identical,
        // see Hyrise #1902 for details. This check here might have false positives, feel free to improve the check or
        // tackle the underlying issue if this ever becomes an issue.
        auto table_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
        DebugAssert(from_clause_result_, "_from_clause_result should be set by now");
        table_expressions.reserve(from_clause_result_->elements_in_order.size());
        for (const auto& select_list_element : from_clause_result_->elements_in_order) {
          table_expressions.emplace_back(select_list_element.expression);
        }

        AssertInput(std::none_of(table_expressions.cbegin(), table_expressions.cend(),
                                 [&aggregate_expression](const auto input_expression) {
                                   return *input_expression == *aggregate_expression;
                                 }),
                    "Hyrise cannot handle repeated aggregate expressions, see #1902 for details.");

        return aggregate_expression;
      }

      /**
       * "Normal" function
       * TODO(anyone) Implement FunctionExpression to support SUBSTR, CONCAT
       */
      Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
    }

    case hsql::kExprOperator: {
      // Translate ArithmeticExpression
      const auto arithmetic_operators_iter = hsql_arithmetic_operators.find(expr.opType);
      if (arithmetic_operators_iter != hsql_arithmetic_operators.end()) {
        Assert(left && right, "Unexpected SQLParserResult. Didn't receive two arguments for binary expression.");
        return std::make_shared<ArithmeticExpression>(arithmetic_operators_iter->second, left, right);
      }

      // Translate PredicateExpression
      const auto predicate_condition_iter = hsql_predicate_condition.find(expr.opType);
      if (predicate_condition_iter != hsql_predicate_condition.end()) {
        const auto predicate_condition = predicate_condition_iter->second;

        if (IsBinaryPredicateCondition(predicate_condition)) {
          Assert(left && right, "Unexpected SQLParserResult. Didn't receive two arguments for binary_expression");
          return std::make_shared<BinaryPredicateExpression>(predicate_condition, left, right);
        } else if (predicate_condition == PredicateCondition::kBetweenInclusive) {
          Assert(expr.exprList && expr.exprList->size() == 2, "Expected two arguments for BETWEEN");
          return std::make_shared<BetweenExpression>(PredicateCondition::kBetweenInclusive, left,
                                                     TranslateHsqlExpr(*(*expr.exprList)[0], sql_identifier_resolver),
                                                     TranslateHsqlExpr(*(*expr.exprList)[1], sql_identifier_resolver));
        }
      }

      // Translate other expression types that can be expected at this point
      switch (expr.opType) {
        case hsql::kOpUnaryMinus:
          return std::make_shared<UnaryMinusExpression>(left);
        case hsql::kOpCase:
          return TranslateHsqlCase(expr, sql_identifier_resolver);
        case hsql::kOpOr:
          return std::make_shared<LogicalExpression>(LogicalOperator::kOr, left, right);
        case hsql::kOpAnd:
          return std::make_shared<LogicalExpression>(LogicalOperator::kAnd, left, right);
        case hsql::kOpIn: {
          if (expr.select) {
            // `a IN (SELECT ...)`
            Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
          } else {
            // `a IN (x, y, z)`
            Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
          }
        }

        case hsql::kOpIsNull:
          return IsNull_(left);

        case hsql::kOpNot:
          return InversePredicate(*left);

        case hsql::kOpExists:
          Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");

        default:
          Fail("Unexpected expression type");  // There are 19 of these, so we make an exception here and use default
      }
    }

    case hsql::kExprSelect:
      Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");

    case hsql::kExprArray:
      FailInput("Can't translate a standalone array, arrays only valid in IN expressions");

    case hsql::kExprStar:
      Fail("Star expression should have been handled earlier");

    case hsql::kExprArrayIndex:
      FailInput("Array indexes are not yet supported");

    case hsql::kExprHint:
      FailInput("Hints are not yet supported");

    case hsql::kExprCast:
      FailInput("Explicit casts are not yet supported");
  }
  Fail("Invalid enum value");
}

std::shared_ptr<AbstractExpression> SqlTranslator::TranslateHsqlCase(
    const hsql::Expr& /* expr */, const std::shared_ptr<SqlIdentifierResolver>& /* sql_identifier_resolver */) {
  Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
}

std::shared_ptr<AbstractExpression> SqlTranslator::InversePredicate(const AbstractExpression& expression) {
  /**
   * Inverse a boolean expression
   */
  switch (expression.type_) {
    case ExpressionType::kPredicate: {
      if (const auto* binary_predicate_expression = dynamic_cast<const BinaryPredicateExpression*>(&expression);
          binary_predicate_expression) {
        // If the argument is a predicate, just inverse it (e.g. NOT (a > b) becomes b <= a)
        return std::make_shared<BinaryPredicateExpression>(
            InversePredicateCondition(binary_predicate_expression->predicate_condition_),
            binary_predicate_expression->LeftOperand(), binary_predicate_expression->RightOperand());
      } else if (const auto* const is_null_expression = dynamic_cast<const IsNullExpression*>(&expression);
                 is_null_expression) {
        // NOT (IS NULL ...) -> IS NOT NULL ...
        return std::make_shared<IsNullExpression>(InversePredicateCondition(is_null_expression->predicate_condition_),
                                                  is_null_expression->Operand());
      } else if (const auto* const between_expression = dynamic_cast<const BetweenExpression*>(&expression);
                 between_expression) {
        // a BETWEEN b AND c -> a < b OR a > c
        return Or_(LessThan_(between_expression->Value(), between_expression->LowerBound()),
                   GreaterThan_(between_expression->Value(), between_expression->UpperBound()));
      } else {
        const auto* in_expression = dynamic_cast<const InExpression*>(&expression);
        Assert(in_expression, "Expected InExpression");
        return std::make_shared<InExpression>(InversePredicateCondition(in_expression->predicate_condition_),
                                              in_expression->Value(), in_expression->Set());
      }
    } break;

    case ExpressionType::kLogical: {
      const auto* logical_expression = static_cast<const LogicalExpression*>(&expression);

      switch (logical_expression->logical_operator_) {
        case LogicalOperator::kAnd:
          return Or_(InversePredicate(*logical_expression->LeftOperand()),
                     InversePredicate(*logical_expression->RightOperand()));
        case LogicalOperator::kOr:
          return And_(InversePredicate(*logical_expression->LeftOperand()),
                      InversePredicate(*logical_expression->RightOperand()));
      }
    } break;
    case ExpressionType::kExists: {
      Fail("Missing SQL translation functionality. For code examples, see Hyrise codebase.");
    } break;
    default:
      Fail("Can't invert non-boolean expression");
  }

  Fail("Invalid enum value");
}

std::vector<std::shared_ptr<AbstractExpression>> SqlTranslator::UnwrapElements(
    const std::vector<SelectListElement>& select_list_elements) {
  std::vector<std::shared_ptr<AbstractExpression>> expressions;
  expressions.reserve(select_list_elements.size());
  for (const auto& element : select_list_elements) {
    expressions.emplace_back(element.expression);
  }
  return expressions;
}

SqlTranslator::SelectListElement::SelectListElement(const std::shared_ptr<AbstractExpression>& init_expression)
    : expression(init_expression) {}

SqlTranslator::SelectListElement::SelectListElement(const std::shared_ptr<AbstractExpression>& init_expression,
                                                    const std::vector<SqlIdentifier>& init_identifiers)
    : expression(init_expression), identifiers(init_identifiers) {}

SqlTranslator::TableSourceState::TableSourceState(
    const std::shared_ptr<AbstractLqpNode>& init_lqp,
    const std::unordered_map<std::string, std::vector<SelectListElement>>& init_elements_by_table_name,
    const std::vector<SelectListElement>& init_elements_in_order,
    const std::shared_ptr<SqlIdentifierResolver>& init_sql_identifier_resolver)
    : lqp(init_lqp),
      elements_by_table_name(init_elements_by_table_name),
      elements_in_order(init_elements_in_order),
      sql_identifier_resolver(init_sql_identifier_resolver) {}

void SqlTranslator::TableSourceState::Append(TableSourceState&& rhs) {
  for (auto& table_name_and_elements : rhs.elements_by_table_name) {
    const auto unique = elements_by_table_name.count(table_name_and_elements.first) == 0;
    AssertInput(unique, "Table Name '"s + table_name_and_elements.first + "' in FROM clause is not unique");
  }

  // This should be ::merge, but that is not yet supported by clang.
  // elements_by_table_name.merge(std::move(rhs.elements_by_table_name));
  for (auto& kv : rhs.elements_by_table_name) {
    elements_by_table_name.try_emplace(kv.first, std::move(kv.second));
  }

  elements_in_order.insert(elements_in_order.end(), rhs.elements_in_order.begin(), rhs.elements_in_order.end());
  sql_identifier_resolver->Append(std::move(*rhs.sql_identifier_resolver));
}

}  // namespace skyrise
