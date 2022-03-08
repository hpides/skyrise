/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "lqp_translator.hpp"

#include <memory>
#include <string>
#include <vector>

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
#include "compiler/logical_query_plan/mock_node.hpp"
#include "compiler/logical_query_plan/predicate_node.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/logical_query_plan/sort_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "compiler/logical_query_plan/union_node.hpp"
#include "compiler/physical_query_plan/aggregate_operator_proxy.hpp"
#include "compiler/physical_query_plan/alias_operator_proxy.hpp"
#include "compiler/physical_query_plan/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/join_operator_proxy.hpp"
#include "compiler/physical_query_plan/limit_operator_proxy.hpp"
#include "compiler/physical_query_plan/projection_operator_proxy.hpp"
#include "compiler/physical_query_plan/sort_operator_proxy.hpp"
#include "compiler/physical_query_plan/union_operator_proxy.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "lqp_expression_utils.hpp"

using namespace std::string_literals;  // NOLINT(google-build-using-namespace)

namespace skyrise {

LqpTranslator::LqpTranslator(std::shared_ptr<QueryContext> query_context) : query_context_(std::move(query_context)) {}

std::shared_ptr<AbstractOperatorProxy> LqpTranslator::TranslateNode(
    const std::shared_ptr<AbstractLqpNode>& node) const {
  /**
   * Translate a node (i.e. call `TranslateByNodeType`) only if it hasn't been translated before, otherwise just
   * retrieve it from the cache.
   *
   * Without this caching, translating this kind of LQP
   *
   *    _____union____
   *   /              \
   *  predicate_a     predicate_b
   *  \                /
   *   \__predicate_c_/
   *          |
   *     table_int_float2
   *
   * would result in multiple operators created from predicate_c and thus in performance drops.
   *
   * Deduplication:
   * _operator_by_lqp_node compares entries by value (i.e., AbstractOperatorProxy::operator==), not by identity
   * (shared_ptr::operator==). As a result, two separate, but equal LQP nodes will be translated into a single PQP
   * node. This prevents us from executing the same operation twice.
   *   Excursus: You would be right to wonder why this is not done on the LQP by some type of optimizer rule. That would
   *   indeed be the cleaner way to do it. The problem is that self-joins are only representable in the LQP if we use
   *   two independent StoredTableNodes. If we deduplicate these StoredTableNodes, the LqpColumnExpressions of the two
   *   instances would also become indistinguishable. That breaks things left and right.
   */

  // Look in the cache
  const auto operator_iter = operator_proxy_by_lqp_node_.find(node);
  if (operator_iter != operator_proxy_by_lqp_node_.end()) {
    return operator_iter->second;
  }

  // Create OperatorProxy
  auto operator_proxy = TranslateByNodeType(node->Type(), node);
  operator_proxy_by_lqp_node_.emplace(node, operator_proxy);

  return operator_proxy;
}

std::shared_ptr<AbstractOperatorProxy> LqpTranslator::TranslateByNodeType(
    LqpNodeType type, const std::shared_ptr<AbstractLqpNode>& node) const {
  switch (type) {
    // clang-format off
    case LqpNodeType::kAggregate:          return _translate_aggregate_node(node);
    case LqpNodeType::kAlias:              return _translate_alias_node(node);
    case LqpNodeType::kDummyTable:         return _translate_dummy_table_node(node);
    case LqpNodeType::kJoin:               return _translate_join_node(node);
    case LqpNodeType::kLimit:              return TranslateLimitNode(node);
    case LqpNodeType::kPredicate:          return _translate_predicate_node(node);
    case LqpNodeType::kProjection:         return _translate_projection_node(node);
    case LqpNodeType::kSort:               return _translate_sort_node(node);
    case LqpNodeType::kStoredTable:        return TranslateStoredTableNode(node);
    case LqpNodeType::kUnion:              return _translate_union_node(node);

    // Maintenance operators
    case LqpNodeType::kCreateView:         return TranslateCreateViewNode(node);
    case LqpNodeType::kDropView:           return TranslateDropViewNode(node);
    case LqpNodeType::kExport:             return TranslateExportNode(node);
    case LqpNodeType::kImport:             return TranslateImportNode(node);
    default:
      Fail("Unknown node type encountered.");
      // clang-format on
  }
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperatorProxy> LqpTranslator::TranslateStoredTableNode(
    const std::shared_ptr<AbstractLqpNode>& node) const {
  const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(node);

  // Define Import
  const auto& bucket_name = query_context_->Catalog()->TableBucketName(stored_table_node->table_name_);
  // Determine objects to import
  const auto& table_partitions = query_context_->Catalog()->GetTablePartitions(stored_table_node->table_name_);
  std::vector<std::string> object_keys;
  object_keys.reserve(table_partitions.size());
  std::transform(table_partitions.begin(), table_partitions.end(), std::back_inserter(object_keys),
                 [](const auto& table_partition) { return table_partition.ObjectKey(); });
  Assert(!object_keys.empty(), "Cannot create an ImportOperatorProxy without object key(s).");

  // Determine column(s) to import
  std::vector<ColumnId> column_ids;
  column_ids.reserve(stored_table_node->OutputExpressions().size());
  for (const auto& output_expression : stored_table_node->OutputExpressions()) {
    const auto lqp_column_expression = std::static_pointer_cast<LqpColumnExpression>(output_expression);
    column_ids.push_back(lqp_column_expression->original_column_id_);
  }
  auto import_proxy = ImportOperatorProxy::Make(bucket_name, object_keys, column_ids);
  // Set Comment: table_name n/m column(s)
  size_t table_column_count = column_ids.size() + stored_table_node->pruned_column_ids().size();
  std::stringstream comment_stream;
  comment_stream << stored_table_node->table_name_ << " ";
  comment_stream << column_ids.size() << "/" << table_column_count << " columns";
  import_proxy->SetComment(comment_stream.str());

  return import_proxy;
}

std::shared_ptr<AbstractOperatorProxy> LqpTranslator::_translate_predicate_node(
    const std::shared_ptr<AbstractLqpNode>& node) const {
  const auto input_node = node->LeftInput();
  const auto input_operator_proxy = TranslateNode(input_node);
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
  auto predicate = _translate_expression(predicate_node->predicate(), input_node);

  return FilterOperatorProxy::Make(predicate, input_operator_proxy);
}

std::shared_ptr<AbstractOperatorProxy> LqpTranslator::_translate_alias_node(
    const std::shared_ptr<skyrise::AbstractLqpNode>& node) const {
  const auto alias_node = std::dynamic_pointer_cast<AliasNode>(node);
  const auto input_node = alias_node->LeftInput();
  const auto input_operator_proxy = TranslateNode(input_node);

  auto column_ids = std::vector<ColumnId>();
  column_ids.reserve(alias_node->OutputExpressions().size());

  for (const auto& expression : alias_node->OutputExpressions()) {
    column_ids.emplace_back(input_node->GetColumnId(*expression));
  }

  return AliasOperatorProxy::Make(column_ids, alias_node->aliases, input_operator_proxy);
}

std::shared_ptr<AbstractOperatorProxy> LqpTranslator::_translate_projection_node(
    const std::shared_ptr<AbstractLqpNode>& node) const {
  const auto input_node = node->LeftInput();
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node);
  const auto input_operator_proxy = TranslateNode(input_node);

  return ProjectionOperatorProxy::Make(_translate_expressions(projection_node->node_expressions_, input_node),
                                       input_operator_proxy);
}

std::shared_ptr<AbstractOperatorProxy> LqpTranslator::_translate_sort_node(
    const std::shared_ptr<AbstractLqpNode>& node) const {
  auto input_operator_proxy = TranslateNode(node->LeftInput());

  const auto sort_node = std::dynamic_pointer_cast<SortNode>(node);
  const auto& pqp_expressions = _translate_expressions(sort_node->node_expressions_, node->LeftInput());

  std::vector<SortColumnDefinition> sort_column_definitions;
  sort_column_definitions.reserve(pqp_expressions.size());

  auto pqp_expression_iter = pqp_expressions.begin();
  auto sort_mode_iter = sort_node->sort_modes.begin();
  for (; pqp_expression_iter != pqp_expressions.end(); ++pqp_expression_iter, ++sort_mode_iter) {
    const auto& pqp_expression = *pqp_expression_iter;
    const auto pqp_column_expression = std::dynamic_pointer_cast<PqpColumnExpression>(pqp_expression);
    Assert(pqp_column_expression,
           "Sort Expression '"s + pqp_expression->AsColumnName() + "' must be available as column, LQP is invalid.");

    sort_column_definitions.emplace_back(SortColumnDefinition{pqp_column_expression->column_id_, *sort_mode_iter});
  }

  return SortOperatorProxy::Make(sort_column_definitions, input_operator_proxy);
}

std::shared_ptr<AbstractOperatorProxy> LqpTranslator::_translate_join_node(
    const std::shared_ptr<AbstractLqpNode>& node) const {
  const auto left_input_operator_proxy = TranslateNode(node->LeftInput());
  const auto right_input_operator_proxy = TranslateNode(node->RightInput());
  auto join_node = std::dynamic_pointer_cast<JoinNode>(node);
  auto join_mode = join_node->join_mode;

  // Cross Joins
  if (join_mode == JoinMode::kCross) {
    std::vector<std::shared_ptr<AbstractExpression>> secondary_join_predicates;
    return JoinOperatorProxy::Make(join_mode, nullptr, secondary_join_predicates, left_input_operator_proxy,
                                   right_input_operator_proxy);
  }
  Assert(!join_node->join_predicates().empty(), "Need predicate for non Cross Join");

  const auto& primary_join_predicate = join_node->join_predicates().front();
  std::vector<std::shared_ptr<AbstractExpression>> secondary_join_predicates(join_node->join_predicates().cbegin() + 1,
                                                                             join_node->join_predicates().cend());
  return JoinOperatorProxy::Make(join_mode, primary_join_predicate, secondary_join_predicates,
                                 left_input_operator_proxy, right_input_operator_proxy);
}

std::shared_ptr<AbstractOperatorProxy> LqpTranslator::_translate_aggregate_node(
    const std::shared_ptr<AbstractLqpNode>& node) const {
  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(node);
  const auto input_operator_proxy = TranslateNode(node->LeftInput());

  std::vector<std::shared_ptr<AbstractExpression>> pqp_aggregate_expressions;
  pqp_aggregate_expressions.reserve(aggregate_node->node_expressions_.size() -
                                    aggregate_node->aggregate_expressions_begin_idx);
  for (size_t i = aggregate_node->aggregate_expressions_begin_idx; i < aggregate_node->node_expressions_.size(); ++i) {
    const auto& lqp_expression = aggregate_node->node_expressions_[i];
    Assert(lqp_expression->type_ == ExpressionType::kAggregate,
           "Expression '" + lqp_expression->AsColumnName() +
               "' used as AggregateExpression is not an AggregateExpression");

    const auto pqp_expression = _translate_expression(lqp_expression, node->LeftInput());
    pqp_aggregate_expressions.emplace_back(pqp_expression);
  }

  // Create GroupByColumnIds from the GroupBy expressions. For now, we expect all GroupBy expressions to be already
  // present, i.e., we do not calculate them on the fly.
  std::vector<ColumnId> group_by_column_ids;
  group_by_column_ids.reserve(aggregate_node->node_expressions_.size() -
                              aggregate_node->aggregate_expressions_begin_idx);

  for (size_t i = 0; i < aggregate_node->aggregate_expressions_begin_idx; ++i) {
    const auto& expression = aggregate_node->node_expressions_[i];
    const auto column_id = node->LeftInput()->FindColumnId(*expression);
    Assert(column_id, "GroupBy expression '"s + expression->AsColumnName() + "' not available as a column.");
    group_by_column_ids.emplace_back(*column_id);
  }

  return AggregateOperatorProxy::Make(group_by_column_ids, pqp_aggregate_expressions, input_operator_proxy);
}

std::shared_ptr<AbstractOperatorProxy> LqpTranslator::TranslateLimitNode(
    const std::shared_ptr<AbstractLqpNode>& node) const {
  const auto input_operator_proxy = TranslateNode(node->LeftInput());
  auto limit_node = std::dynamic_pointer_cast<LimitNode>(node);
  auto row_count_expression = _translate_expression(limit_node->num_rows_expression(), node->LeftInput());

  return LimitOperatorProxy::Make(row_count_expression, input_operator_proxy);
}

std::shared_ptr<AbstractOperatorProxy> LqpTranslator::_translate_union_node(
    const std::shared_ptr<AbstractLqpNode>& node) const {
  const auto left_input_operator_proxy = TranslateNode(node->LeftInput());
  const auto right_input_operator_proxy = TranslateNode(node->RightInput());
  const auto union_node = std::dynamic_pointer_cast<UnionNode>(node);

  return UnionOperatorProxy::Make(union_node->set_operation_mode, left_input_operator_proxy,
                                  right_input_operator_proxy);
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperatorProxy> LqpTranslator::TranslateCreateViewNode(
    const std::shared_ptr<AbstractLqpNode>& /*node*/) const {
  // const auto create_view_node = std::dynamic_pointer_cast<CreateViewNode>(node);
  Fail("Missing LQP translation functionality. For code examples, see Hyrise codebase.");
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperatorProxy> LqpTranslator::TranslateDropViewNode(
    const std::shared_ptr<AbstractLqpNode>& /*node*/) const {
  // const auto drop_view_node = std::dynamic_pointer_cast<DropViewNode>(node);
  Fail("Missing LQP translation functionality. For code examples, see Hyrise codebase.");
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperatorProxy> LqpTranslator::TranslateImportNode(
    const std::shared_ptr<AbstractLqpNode>& /*node*/) const {
  // const auto import_node = std::dynamic_pointer_cast<ImportNode>(node);
  Fail("Missing LQP translation functionality. For code examples, see Hyrise codebase.");
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperatorProxy> LqpTranslator::TranslateExportNode(
    const std::shared_ptr<AbstractLqpNode>& /*node*/) const {
  // const auto export_node = std::dynamic_pointer_cast<ExportNode>(node);
  Fail("Missing LQP translation functionality. For code examples, see Hyrise codebase.");
}

// NOLINTNEXTLINE - while this particular method could be made static, others cannot.
std::shared_ptr<AbstractOperatorProxy> LqpTranslator::_translate_dummy_table_node(
    const std::shared_ptr<AbstractLqpNode>& /*node*/) const {
  // const auto dummy_table_node = std::dynamic_pointer_cast<DummyTableNode>(node);
  Fail("Missing LQP translation functionality. For code examples, see Hyrise codebase.");
  // return std::make_shared<TableWrapper>(Projection::dummy_table());
}

std::shared_ptr<AbstractExpression> LqpTranslator::_translate_expression(
    const std::shared_ptr<AbstractExpression>& lqp_expression, const std::shared_ptr<AbstractLqpNode>& node) {
  auto pqp_expression = lqp_expression->DeepCopy();

  /**
   * Resolve expressions to PqpColumnExpressions to reference columns from an input operator.
   * After this, LqpColumnExpressions remain in the pqp_expression and it is a valid PQP expression.
   */
  VisitExpression(pqp_expression, [&](auto& expression) {
    // Try to resolve the Expression to a column from the input node
    const auto column_id = node->FindColumnId(*expression);
    if (column_id) {
      const auto referenced_expression = node->OutputExpressions()[*column_id];
      expression = std::make_shared<PqpColumnExpression>(
          *column_id, referenced_expression->GetDataType(),
          node->IsColumnNullable(node->GetColumnId(*referenced_expression)), referenced_expression->AsColumnName());
      return ExpressionVisitation::kDoNotVisitArguments;
    }

    // Resolve COUNT(*)
    if (IsCountStarAggregateExpression(expression)) {
      const auto star = std::make_shared<PqpColumnExpression>(kInvalidColumnId, DataType::kLong, false, "*");
      expression = std::make_shared<AggregateExpression>(AggregateFunction::kCount, star);
      return ExpressionVisitation::kDoNotVisitArguments;
    }

    // If we support subqueries, resolve LqpSubqueryExpression here. For code examples, see Hyrise codebase.

    AssertInput(expression->type_ != ExpressionType::kLqpColumn,
                "Failed to resolve Column '"s + expression->AsColumnName() + "'. LQP is invalid.");

    return ExpressionVisitation::kVisitArguments;
  });

  return pqp_expression;
}

std::vector<std::shared_ptr<AbstractExpression>> LqpTranslator::_translate_expressions(
    const std::vector<std::shared_ptr<AbstractExpression>>& lqp_expressions,
    const std::shared_ptr<AbstractLqpNode>& node) {
  auto pqp_expressions = std::vector<std::shared_ptr<AbstractExpression>>(lqp_expressions.size());

  for (auto expression_idx = size_t{0}; expression_idx < pqp_expressions.size(); ++expression_idx) {
    pqp_expressions[expression_idx] = _translate_expression(lqp_expressions[expression_idx], node);
  }

  return pqp_expressions;
}

}  // namespace skyrise
