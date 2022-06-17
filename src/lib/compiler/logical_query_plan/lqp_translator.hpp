/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "compiler/logical_query_plan/abstract_lqp_node.hpp"
#include "compiler/physical_query_plan/abstract_operator_proxy.hpp"
#include "compiler/query_context.hpp"

namespace skyrise {

class AbstractOperatorProxy;
class AbstractExpression;

/**
 * Translates an LQP (Logical Query Plan), represented by its root node, into a physical query plan consisting of
 * operator proxies.
 */
class LqpTranslator {
 public:
  LqpTranslator(std::shared_ptr<QueryContext> query_context);

  std::shared_ptr<AbstractOperatorProxy> TranslateNode(const std::shared_ptr<AbstractLqpNode>& node) const;

 protected:
  // TODO(julianmenzler): The following functions can be static.. However, if we pass a catalog reference into the
  //                      LqpTranslator, we need to keep them non-static
  std::shared_ptr<AbstractOperatorProxy> TranslateByNodeType(LqpNodeType type,
                                                             const std::shared_ptr<AbstractLqpNode>& node) const;
  /**
   * LQP node translation
   */
  std::shared_ptr<AbstractOperatorProxy> TranslateStoredTableNode(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> _translate_predicate_node(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> _translate_alias_node(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> _translate_projection_node(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> _translate_sort_node(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> _translate_join_node(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> _translate_aggregate_node(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> TranslateLimitNode(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> _translate_dummy_table_node(
      const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> _translate_union_node(const std::shared_ptr<AbstractLqpNode>& node) const;

  /**
   * Maintenance operators
   */
  std::shared_ptr<AbstractOperatorProxy> TranslateCreateViewNode(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> TranslateDropViewNode(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> TranslateImportNode(const std::shared_ptr<AbstractLqpNode>& node) const;
  std::shared_ptr<AbstractOperatorProxy> TranslateExportNode(const std::shared_ptr<AbstractLqpNode>& node) const;

  /**
   * Translate LQP- to PQPExpressions
   */
  static std::shared_ptr<AbstractExpression> _translate_expression(
      const std::shared_ptr<AbstractExpression>& lqp_expression, const std::shared_ptr<AbstractLqpNode>& node);
  static std::vector<std::shared_ptr<AbstractExpression>> _translate_expressions(
      const std::vector<std::shared_ptr<AbstractExpression>>& lqp_expressions,
      const std::shared_ptr<AbstractLqpNode>& node);

 private:
  const std::shared_ptr<QueryContext> query_context_;
  /**
   * Cache operator subtrees by LQP node to avoid redundantly executing
   *   - identical operators (operators below a diamond shape)
   *   - equal but not identical operators
   */
  mutable LqpNodeUnorderedMap<std::shared_ptr<AbstractOperatorProxy>> operator_proxy_by_lqp_node_;
};

}  // namespace skyrise
