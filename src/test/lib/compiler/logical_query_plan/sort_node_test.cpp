/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 * TODO(julianmenzler): Enable after we found a solution for load_table("..")
 */
#include "compiler/logical_query_plan/sort_node.hpp"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/lqp_utils.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"
#include "metadata/mock_catalog.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class SortNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_catalog_ = std::make_shared<MockCatalog>();
    mock_catalog_->AddTableSchemaFromFileHeader("table_a", "resources/test_data/tbl/int_float_double_string.tbl");

    stored_table_node_ = StoredTableNode::Make("table_a", mock_catalog_);

    a_i_ = stored_table_node_->GetColumn("i");
    a_f_ = stored_table_node_->GetColumn("f");
    a_d_ = stored_table_node_->GetColumn("d");

    sort_node_ =
        SortNode::Make(ExpressionVector_(a_i_), std::vector<SortMode>{SortMode::kAscending}, stored_table_node_);
  }

  std::shared_ptr<MockCatalog> mock_catalog_;
  std::shared_ptr<StoredTableNode> stored_table_node_;
  std::shared_ptr<SortNode> sort_node_;
  std::shared_ptr<LqpColumnExpression> a_i_, a_f_, a_d_;
};

TEST_F(SortNodeTest, Description) {
  EXPECT_EQ(sort_node_->Description(), "[Sort] i (Ascending)");

  // clang-format off
  const auto sort_b =
  SortNode::Make(ExpressionVector_(a_i_), std::vector<SortMode>{SortMode::kDescending},
    stored_table_node_);

  const auto sort_c =
  SortNode::Make(ExpressionVector_(a_d_, a_f_, a_i_), std::vector<SortMode>{SortMode::kDescending, SortMode::kAscending, SortMode::kDescending},
    stored_table_node_);
  // clang-format on

  EXPECT_EQ(sort_b->Description(), "[Sort] i (Descending)");
  EXPECT_EQ(sort_c->Description(), "[Sort] d (Descending), f (Ascending), i (Descending)");
}

TEST_F(SortNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*sort_node_, *sort_node_);

  const auto sort_a =
      SortNode::Make(ExpressionVector_(a_i_), std::vector<SortMode>{SortMode::kDescending}, stored_table_node_);
  const auto sort_b =
      SortNode::Make(ExpressionVector_(a_d_, a_f_, a_i_),
                     std::vector<SortMode>{SortMode::kDescending, SortMode::kAscending, SortMode::kDescending});
  const auto sort_c =
      SortNode::Make(ExpressionVector_(a_i_), std::vector<SortMode>{SortMode::kAscending}, stored_table_node_);

  EXPECT_NE(*sort_node_, *sort_a);
  EXPECT_NE(*sort_node_, *sort_b);
  EXPECT_EQ(*sort_node_, *sort_c);

  EXPECT_NE(sort_node_->Hash(), sort_a->Hash());
  EXPECT_NE(sort_node_->Hash(), sort_b->Hash());
  EXPECT_EQ(sort_node_->Hash(), sort_c->Hash());
}

TEST_F(SortNodeTest, Copy) {
  EXPECT_EQ(*sort_node_->DeepCopy(), *sort_node_);

  // clang-format off
  const auto sort_b =
  SortNode::Make(ExpressionVector_(a_d_, a_f_, a_i_), std::vector<SortMode>{SortMode::kDescending, SortMode::kAscending, SortMode::kDescending},
    stored_table_node_);
  // clang-format off

EXPECT_EQ(*sort_b->DeepCopy(), *sort_b);
}

TEST_F(SortNodeTest, NodeExpressions) {
  ASSERT_EQ(sort_node_->node_expressions_.size(), 1u);
  EXPECT_EQ(*sort_node_->node_expressions_.at(0), *a_i_);
}

}  // namespace skyrise
