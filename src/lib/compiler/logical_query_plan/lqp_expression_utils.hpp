/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"

namespace skyrise {

/**
 * @return true, if the given @param expression is an AggregateExpression of type COUNT(*).
 */
bool IsCountStarAggregateExpression(const std::shared_ptr<AbstractExpression> expression);

/**
 * Utility to compare vectors of Expressions from different LQPs
 */
bool ExpressionsEqualToExpressionsInDifferentLqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions_left,
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions_right, const LqpNodeMapping& node_mapping);

/**
 * Utility to compare two Expressions from different LQPs
 */
bool ExpressionEqualToExpressionInDifferentLqp(const AbstractExpression& expression_left,
                                               const AbstractExpression& expression_right,
                                               const LqpNodeMapping& node_mapping);

/**
 * Utility to AbstractExpression::DeepCopy() a vector of expressions while adjusting LqpColumnExpressions according to
 * the node_mapping
 */
std::vector<std::shared_ptr<AbstractExpression>> ExpressionsCopyAndAdaptToDifferentLqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions, const LqpNodeMapping& node_mapping);

/**
 * Utility to AbstractExpression::DeepCopy() a single expression while adjusting LqpColumnExpressions according to the
 * node_mapping
 */
std::shared_ptr<AbstractExpression> ExpressionCopyAndAdaptToDifferentLqp(const AbstractExpression& expression,
                                                                         const LqpNodeMapping& node_mapping);

/**
 * Makes all LqpColumnExpressions point to their equivalent in a copied LQP
 */
void ExpressionAdaptToDifferentLqp(std::shared_ptr<AbstractExpression>& expression, const LqpNodeMapping& node_mapping);
std::shared_ptr<LqpColumnExpression> ExpressionAdaptToDifferentLqp(const LqpColumnExpression& lqp_column_expression,
                                                                   const LqpNodeMapping& node_mapping);

/**
 * @return Checks whether the expression can be evaluated on top of a specified LQP (i.e., all required
 *         LqpColumnExpressions are available from this LQP). This does not mean that all expressions are already
 *         readily available as a column. It might be necessary to add a Projection or an Aggregate.
 *         To check if an expression is available in a form ready to be used by a scan/join,
 *         use `Operator*Predicate::from_expression(...)`.
 */
bool ExpressionEvaluableOnLqp(const std::shared_ptr<AbstractExpression>& expression, const AbstractLqpNode& lqp);

/**
 * @returns whether @param expression, executed on the output of plan @param lqp, would produce a nullable result.
 * @note    Determining whether an Expression (isolated from a plan to execute it on) is nullable is intentionally
 *          not supported: This is because "expression X is nullable" does NOT always equal "a column containing
 *          expression X is nullable": An expression `a + 5` (with `a` being a non-nullable column),
 *          e.g., is actually nullable if `a` comes from the null-supplying side of an outer join.
 */
bool ExpressionIsNullableOnLqp(const std::shared_ptr<AbstractExpression>& expression, const AbstractLqpNode& lqp);

}  // namespace skyrise
