/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "lqp_column_expression.hpp"

#include <boost/container_hash/hash.hpp>

#include "compiler/logical_query_plan/mock_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

LqpColumnExpression::LqpColumnExpression(const std::shared_ptr<const AbstractLqpNode>& init_original_node,
                                         const ColumnId init_original_column_id)
    : AbstractExpression(ExpressionType::kLqpColumn, {}),
      original_node_(init_original_node),
      original_column_id_(init_original_column_id) {}

std::string LqpColumnExpression::Description(const DescriptionMode mode) const {
  // Even if the LQP is invalid, we still want to be able to print it as good as possible
  const auto original_node_locked = original_node_.lock();
  if (!original_node_locked) return "<Expired Column>";

  std::stringstream output;
  if (mode == AbstractExpression::DescriptionMode::kDetailed) {
    output << original_node_locked << ".";
  }

  if (original_column_id_ == kInvalidColumnId) {
    // Case: AggregateExpression COUNT(*)
    output << "*";
    return output.str();
  }

  switch (original_node_locked->Type()) {
    case LqpNodeType::kStoredTable: {
      const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(original_node_locked);
      const auto table_schema = stored_table_node->catalog_->GetTableSchema(stored_table_node->table_name_);
      output << table_schema->ColumnName(original_column_id_);
      return output.str();
    }

    case LqpNodeType::kMock: {
      const auto mock_node = std::static_pointer_cast<const MockNode>(original_node_locked);
      Assert(original_column_id_ < mock_node->column_definitions().size(), "ColumnId out of range");
      output << mock_node->column_definitions()[original_column_id_].second;
      return output.str();
    }

    default: {
      Fail("Node type can not be referenced in LqpColumnExpression");
    }
  }
}

DataType LqpColumnExpression::GetDataType() const {
  const auto original_node_locked = original_node_.lock();
  Assert(original_node_locked, "Trying to retrieve data_type of expired LqpColumnExpression, LQP is invalid");

  if (original_column_id_ == kInvalidColumnId) {
    // Handle COUNT(*). Note: This is the input data type.
    return DataType::kLong;
  }

  switch (original_node_locked->Type()) {
    case LqpNodeType::kStoredTable: {
      const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(original_node_locked);
      const auto table_schema = stored_table_node->catalog_->GetTableSchema(stored_table_node->table_name_);
      return table_schema->ColumnDataType(original_column_id_);
    }

    case LqpNodeType::kMock: {
      const auto mock_node = std::static_pointer_cast<const MockNode>(original_node_locked);
      Assert(original_column_id_ < mock_node->column_definitions().size(), "ColumnId out of range");
      return mock_node->column_definitions()[original_column_id_].first;
    }

    default: {
      Fail("Node type can not be referenced in LqpColumnExpressions");
    }
  }
}

bool LqpColumnExpression::RequiresComputation() const { return false; }

bool LqpColumnExpression::ShallowEquals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const LqpColumnExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& lqp_column_expression = static_cast<const LqpColumnExpression&>(expression);
  return original_column_id_ == lqp_column_expression.original_column_id_ &&
         original_node_.lock() == lqp_column_expression.original_node_.lock();
}

size_t LqpColumnExpression::ShallowHash() const {
  // It is important not to combine the address of the original_node with the hash code as it was done before. (Hyrise
  // #1795) If this address is combined with the return hash code, equal LQP nodes that are not identical and that have
  // LqpColumnExpressions or child nodes with LqpColumnExpressions would have different hash codes.
  auto hash = boost::hash_value(original_node_.lock()->hash());
  boost::hash_combine(hash, static_cast<size_t>(original_column_id_));
  return hash;
}

std::shared_ptr<AbstractExpression> LqpColumnExpression::DeepCopy() const {
  return std::make_shared<LqpColumnExpression>(original_node_.lock(), original_column_id_);
}

std::shared_ptr<LqpColumnExpression> LqpColumn_(const std::shared_ptr<const AbstractLqpNode>& original_node,
                                                const ColumnId original_column_id) {
  return std::make_shared<LqpColumnExpression>(original_node, original_column_id);
}

std::shared_ptr<AggregateExpression> CountStarLqp_(const std::shared_ptr<AbstractLqpNode>& lqp_node) {
  const auto column_expression = std::make_shared<LqpColumnExpression>(lqp_node, kInvalidColumnId);
  return std::make_shared<AggregateExpression>(AggregateFunction::kCount, column_expression);
}

}  // namespace skyrise
