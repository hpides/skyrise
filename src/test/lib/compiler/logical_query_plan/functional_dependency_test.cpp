/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/functional_dependency.hpp"

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/mock_node.hpp"

namespace skyrise {

class FunctionalDependencyTest : public ::testing::Test {
 public:
  void SetUp() override {
    mock_node_a_ =
        MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kInt, "b"}, {DataType::kInt, "c"}},
                       "mock_node_a");
    a_ = mock_node_a_->GetColumn("a");
    b_ = mock_node_a_->GetColumn("b");
    c_ = mock_node_a_->GetColumn("c");

    mock_node_b_ =
        MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "x"}, {DataType::kInt, "y"}}, "mock_node_b");
    x_ = mock_node_b_->GetColumn("x");
    y_ = mock_node_b_->GetColumn("y");
  }

 protected:
  std::shared_ptr<MockNode> mock_node_a_, mock_node_b_;
  std::shared_ptr<AbstractExpression> a_, b_, c_, x_, y_;
};

TEST_F(FunctionalDependencyTest, Equals) {
  const auto fd_a = FunctionalDependency({a_}, {b_, c_});
  const auto fd_a_b = FunctionalDependency({a_, b_}, {c_});

  // Equal
  EXPECT_EQ(fd_a, FunctionalDependency({a_}, {b_, c_}));
  EXPECT_EQ(fd_a, FunctionalDependency({a_}, {c_, b_}));
  EXPECT_EQ(fd_a_b, FunctionalDependency({a_, b_}, {c_}));
  EXPECT_EQ(fd_a_b, FunctionalDependency({b_, a_}, {c_}));
  // Not Equal
  EXPECT_NE(fd_a, FunctionalDependency({a_}, {c_}));
  EXPECT_NE(fd_a, FunctionalDependency({a_, x_}, {b_, c_}));
  EXPECT_NE(fd_a_b, FunctionalDependency({a_, b_}, {c_, x_}));
  EXPECT_NE(fd_a_b, FunctionalDependency({a_}, {c_}));
}

TEST_F(FunctionalDependencyTest, Hash) {
  const auto fd_a = FunctionalDependency({a_}, {b_, c_});
  const auto fd_a_b = FunctionalDependency({a_, b_}, {c_});

  // Equal Hash
  EXPECT_EQ(fd_a.Hash(), FunctionalDependency({a_}, {b_, c_}).Hash());
  EXPECT_EQ(fd_a.Hash(), FunctionalDependency({a_}, {b_}).Hash());
  EXPECT_EQ(fd_a.Hash(), FunctionalDependency({a_}, {x_, y_}).Hash());
  EXPECT_EQ(fd_a_b.Hash(), FunctionalDependency({a_, b_}, {c_}).Hash());
  EXPECT_EQ(fd_a_b.Hash(), FunctionalDependency({b_, a_}, {c_}).Hash());
  EXPECT_EQ(fd_a_b.Hash(), FunctionalDependency({a_, b_}, {c_, x_}).Hash());
  EXPECT_EQ(fd_a_b.Hash(), FunctionalDependency({a_, b_}, {x_}).Hash());
  // Non-Equal Hash
  EXPECT_NE(fd_a.Hash(), FunctionalDependency({a_, x_}, {b_, c_}).Hash());
  EXPECT_NE(fd_a.Hash(), FunctionalDependency({x_}, {b_, c_}).Hash());
  EXPECT_NE(fd_a_b.Hash(), FunctionalDependency({a_}, {c_}).Hash());
  EXPECT_NE(fd_a_b.Hash(), FunctionalDependency({a_, b_, x_}, {c_}).Hash());
}

TEST_F(FunctionalDependencyTest, InflateFDs) {
  const auto fd_a = FunctionalDependency({a_}, {b_, c_});
  const auto fd_a_1 = FunctionalDependency({a_}, {b_});
  const auto fd_a_2 = FunctionalDependency({a_}, {c_});
  const auto fd_a_b = FunctionalDependency({a_, b_}, {c_});
  const auto fd_x = FunctionalDependency({x_}, {y_});

  const auto& inflated_fds = InflateFunctionalDependencies({fd_a, fd_a_b, fd_x, fd_x});
  EXPECT_EQ(inflated_fds.size(), 4);
  // TODO(anyone): C++20: Replace with .contains
  EXPECT_FALSE(inflated_fds.find(fd_a) != inflated_fds.end());
  EXPECT_TRUE(inflated_fds.find(fd_a_1) != inflated_fds.end());
  EXPECT_TRUE(inflated_fds.find(fd_a_2) != inflated_fds.end());
  EXPECT_TRUE(inflated_fds.find(fd_a_b) != inflated_fds.end());
  EXPECT_TRUE(inflated_fds.find(fd_x) != inflated_fds.end());
}

TEST_F(FunctionalDependencyTest, DeflateFDs) {
  const auto fd_a = FunctionalDependency({a_}, {b_, c_});
  const auto fd_a_1 = FunctionalDependency({a_}, {b_});
  const auto fd_a_2 = FunctionalDependency({a_}, {c_});
  const auto fd_b_c = FunctionalDependency({b_, c_}, {a_});

  const auto& deflated_fds = DeflateFunctionalDependencies({fd_a_1, fd_a_2, fd_a_2, fd_b_c});
  EXPECT_EQ(deflated_fds.size(), 2);
  const auto deflated_fds_set = std::unordered_set<FunctionalDependency>(deflated_fds.cbegin(), deflated_fds.cend());
  // TODO(anyone): C++20: Replace with .contains
  EXPECT_TRUE(deflated_fds_set.find(fd_a) != deflated_fds_set.end());
  EXPECT_TRUE(deflated_fds_set.find(fd_b_c) != deflated_fds_set.end());
}

TEST_F(FunctionalDependencyTest, UnionFDsEmpty) {
  const auto fd_a = FunctionalDependency({a_}, {b_, c_});

  EXPECT_TRUE(UnionFunctionalDependencies({}, {}).empty());
  EXPECT_EQ(UnionFunctionalDependencies({fd_a}, {}), std::vector<FunctionalDependency>{fd_a});
  EXPECT_EQ(UnionFunctionalDependencies({}, {fd_a}), std::vector<FunctionalDependency>{fd_a});
}

TEST_F(FunctionalDependencyTest, UnionFDs) {
  const auto fd_a = FunctionalDependency({a_}, {b_, c_});
  const auto fd_a_1 = FunctionalDependency({a_}, {b_});
  const auto fd_a_2 = FunctionalDependency({a_}, {c_});
  const auto fd_a_b = FunctionalDependency({a_, b_}, {c_});
  const auto fd_b = FunctionalDependency({b_}, {c_});

  const auto& fds_unified = UnionFunctionalDependencies({fd_a_1, fd_a_b, fd_b}, {fd_a_2});
  const auto& fds_unified_set = std::unordered_set<FunctionalDependency>(fds_unified.begin(), fds_unified.end());

  EXPECT_EQ(fds_unified_set.size(), 3);
  // TODO(anyone): C++20: Replace with .contains
  EXPECT_TRUE(fds_unified_set.find(fd_a) != fds_unified_set.end());
  EXPECT_TRUE(fds_unified_set.find(fd_b) != fds_unified_set.end());
  EXPECT_TRUE(fds_unified_set.find(fd_a_b) != fds_unified_set.end());
}

TEST_F(FunctionalDependencyTest, UnionFDsRemoveDuplicates) {
  const auto fd_a = FunctionalDependency({a_}, {b_, c_});
  const auto fd_b = FunctionalDependency({b_}, {c_});

  const auto& fds_unified = UnionFunctionalDependencies({fd_a, fd_b}, {fd_b});

  EXPECT_EQ(fds_unified.size(), 2);
  const auto fds_unified_set = std::unordered_set<FunctionalDependency>(fds_unified.cbegin(), fds_unified.cend());
  // TODO(anyone): C++20: Replace with .contains
  EXPECT_TRUE(fds_unified_set.find(fd_a) != fds_unified_set.end());
  EXPECT_TRUE(fds_unified_set.find(fd_b) != fds_unified_set.end());
}

TEST_F(FunctionalDependencyTest, IntersectFDsEmpty) {
  const auto fd_x = FunctionalDependency({x_}, {y_});

  EXPECT_TRUE(IntersectFunctionalDependencies({}, {}).empty());
  EXPECT_TRUE(IntersectFunctionalDependencies({fd_x}, {}).empty());
  EXPECT_TRUE(IntersectFunctionalDependencies({}, {fd_x}).empty());
}

TEST_F(FunctionalDependencyTest, IntersectFDs) {
  const auto fd_a = FunctionalDependency({a_}, {b_, c_});
  const auto fd_a_1 = FunctionalDependency({a_}, {b_});
  const auto fd_a_2 = FunctionalDependency({a_}, {c_});
  const auto fd_a_b = FunctionalDependency({a_, b_}, {c_});
  const auto fd_x = FunctionalDependency({x_}, {y_});

  const auto& intersected_fds = IntersectFunctionalDependencies({fd_a, fd_a_b, fd_x}, {fd_a_b, fd_a_2});
  EXPECT_EQ(intersected_fds.size(), 2);
  const auto intersected_fds_set =
      std::unordered_set<FunctionalDependency>(intersected_fds.begin(), intersected_fds.end());
  // TODO(anyone): C++20: Replace with .contains
  EXPECT_TRUE(intersected_fds_set.find(fd_a_b) != intersected_fds_set.end());
  EXPECT_TRUE(intersected_fds_set.find(fd_a_2) != intersected_fds_set.end());
}

}  // namespace skyrise
