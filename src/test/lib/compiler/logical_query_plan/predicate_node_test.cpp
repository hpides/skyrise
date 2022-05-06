/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 * TODO(julianmenzler): Enable after we found a solution for load_table("..")
 */
#include "compiler/logical_query_plan/predicate_node.hpp"

#include <memory>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/lqp_utils.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "expression/expression_functional.hpp"

// using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class PredicateNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_catalog_ = std::make_shared<MockCatalog>();
    mock_catalog_->AddTableSchemaFromFileHeader("table_a", "resources/test_data/tbl/int_float_double_string.tbl");

    stored_table_node_ = StoredTableNode::Make("table_a");
    i_ = LqpColumn_(stored_table_node_, ColumnId{0});
    f_ = LqpColumn_(stored_table_node_, ColumnId{1});

    predicate_node_ = PredicateNode::Make(Equals_(i_, 5), stored_table_node_);
  }

  std::shared_ptr<MockCatalog> mock_catalog_;
  std::shared_ptr<StoredTableNode> stored_table_node_;
  std::shared_ptr<LqpColumnExpression> i_, f_;
  std::shared_ptr<PredicateNode> predicate_node_;
};

TEST_F(PredicateNodeTest, Description) { EXPECT_EQ(predicate_node_->Description(), "[Predicate] i = 5"); }

TEST_F(PredicateNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*predicate_node_, *predicate_node_);
  const auto equal_table_node = StoredTableNode::Make("table_a");
  const auto equal_i = equal_table_node->GetColumn("i");

  const auto other_predicate_node_a = PredicateNode::Make(Equals_(i_, 5), stored_table_node_);
  const auto other_predicate_node_b = PredicateNode::Make(Equals_(f_, 5), stored_table_node_);
  const auto other_predicate_node_c = PredicateNode::Make(NotEquals_(i_, 5), stored_table_node_);
  const auto other_predicate_node_d = PredicateNode::Make(Equals_(i_, 6), stored_table_node_);
  const auto other_predicate_node_e = PredicateNode::Make(Equals_(equal_i, 5), equal_table_node);

  EXPECT_EQ(*other_predicate_node_a, *predicate_node_);
  EXPECT_NE(*other_predicate_node_b, *predicate_node_);
  EXPECT_NE(*other_predicate_node_c, *predicate_node_);
  EXPECT_NE(*other_predicate_node_d, *predicate_node_);
  EXPECT_EQ(*other_predicate_node_e, *predicate_node_);

  EXPECT_EQ(other_predicate_node_a->Hash(), predicate_node_->Hash());
  EXPECT_NE(other_predicate_node_b->Hash(), predicate_node_->Hash());
  EXPECT_NE(other_predicate_node_c->Hash(), predicate_node_->Hash());
  EXPECT_NE(other_predicate_node_d->Hash(), predicate_node_->Hash());
  EXPECT_EQ(other_predicate_node_e->Hash(), predicate_node_->Hash());
}

TEST_F(PredicateNodeTest, Copy) { EXPECT_EQ(*predicate_node_->DeepCopy(), *predicate_node_); }

TEST_F(PredicateNodeTest, NodeExpressions) {
  ASSERT_EQ(predicate_node_->node_expressions_.size(), 1u);
  EXPECT_EQ(*predicate_node_->node_expressions_.at(0), *Equals_(i_, 5));
}

}  // namespace skyrise
