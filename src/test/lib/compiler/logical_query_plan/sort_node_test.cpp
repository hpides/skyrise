/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 * TODO(julianmenzler): Enable after we found a solution for load_table("..")
 */
#include "compiler/logical_query_plan/sort_node.hpp"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

//#include "expression/expression_functional.hpp"
//#include "compiler/logical_query_plan/lqp_utils.hpp"
//#include "compiler/logical_query_plan/stored_table_node.hpp"

// using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class SortNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    //    Hyrise::get().storage_manager.add_table("table_a",
    //                                            load_table("resources/test_data/tbl/int_float_double_string.tbl", 2));
    //
    //    table_node_ = StoredTableNode::Make("table_a");
    //
    //    a_i_ = table_node_->get_column("i");
    //    a_f_ = table_node_->get_column("f");
    //    a_d_ = table_node_->get_column("d");
    //
    //    sort_node_ = SortNode::Make(ExpressionVector_(a_i_), std::vector<SortMode>{SortMode::kAscending},
    //    table_node_);
  }
  //
  //  std::shared_ptr<StoredTableNode> table_node_;
  //  std::shared_ptr<SortNode> sort_node_;
  //  std::shared_ptr<LqpColumnExpression> a_i_, a_f_, a_d_;
};
//
// TEST_F(SortNodeTest, Descriptions) {
//  EXPECT_EQ(sort_node_->Description(), "[Sort] i (Ascending)");
//
//  auto sort_b = SortNode::Make(ExpressionVector_(a_i_), std::vector<SortMode>{SortMode::kDescending}, table_node_);
//  EXPECT_EQ(sort_b->Description(), "[Sort] i (Descending)");
//
//  auto sort_c = SortNode::Make(ExpressionVector_(a_d_, a_f_, a_i_),
//                               std::vector<SortMode>{SortMode::kDescending, SortMode::kAscending,
//                               SortMode::kDescending});
//  sort_c->SetLeftInput(table_node_);
//  EXPECT_EQ(sort_c->Description(), "[Sort] d (Descending), f (Ascending), i (Descending)");
//}
//
// TEST_F(SortNodeTest, HashingAndEqualityCheck) {
//  EXPECT_EQ(*sort_node_, *sort_node_);
//
//  const auto sort_a = SortNode::Make(ExpressionVector_(a_i_), std::vector<SortMode>{SortMode::kDescending},
//  table_node_); const auto sort_b =
//      SortNode::Make(ExpressionVector_(a_d_, a_f_, a_i_),
//                     std::vector<SortMode>{SortMode::kDescending, SortMode::kAscending, SortMode::kDescending});
//  const auto sort_c = SortNode::Make(ExpressionVector_(a_i_), std::vector<SortMode>{SortMode::kAscending},
//  table_node_);
//
//  EXPECT_NE(*sort_node_, *sort_a);
//  EXPECT_NE(*sort_node_, *sort_b);
//  EXPECT_EQ(*sort_node_, *sort_c);
//
//  EXPECT_NE(sort_node_->hash(), sort_a->hash());
//  EXPECT_NE(sort_node_->hash(), sort_b->hash());
//  EXPECT_EQ(sort_node_->hash(), sort_c->hash());
//}
//
// TEST_F(SortNodeTest, Copy) {
//  EXPECT_EQ(*sort_node_->DeepCopy(), *sort_node_);
//
//  const auto sort_b = SortNode::Make(
//      ExpressionVector_(a_d_, a_f_, a_i_),
//      std::vector<SortMode>{SortMode::kDescending, SortMode::kAscending, SortMode::kDescending}, table_node_);
//  EXPECT_EQ(*sort_b->DeepCopy(), *sort_b);
//}
//
// TEST_F(SortNodeTest, NodeExpressions) {
//  ASSERT_EQ(sort_node_->node_expressions_.size(), 1u);
//  EXPECT_EQ(*sort_node_->node_expressions_.at(0), *a_i_);
//}

}  // namespace skyrise
