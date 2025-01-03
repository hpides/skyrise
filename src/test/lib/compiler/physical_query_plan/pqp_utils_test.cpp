#include "compiler/physical_query_plan/pqp_utils.hpp"

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/join_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/union_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class PqpUtilsTest : public ::testing::Test {
 public:
  void SetUp() override {
    const std::vector<ColumnId> dummy_column_ids = {ColumnId{2}, ColumnId{4}};
    const std::vector<ObjectReference> object_references = {ObjectReference("dummy_bucket", "key1"),
                                                            ObjectReference("dummy_bucket", "key2")};
    import_proxy_a_ = ImportOperatorProxy::Make(object_references, dummy_column_ids);
    import_proxy_b_ = ImportOperatorProxy::Make(object_references, dummy_column_ids);

    a_a_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a_a");
    a_b_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "a_b");
    b_x_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "b_x");
    b_y_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b_y");
  }

 protected:
  std::shared_ptr<ImportOperatorProxy> import_proxy_a_;
  std::shared_ptr<ImportOperatorProxy> import_proxy_b_;

  std::shared_ptr<PqpColumnExpression> a_a_;
  std::shared_ptr<PqpColumnExpression> a_b_;
  std::shared_ptr<PqpColumnExpression> b_x_;
  std::shared_ptr<PqpColumnExpression> b_y_;
};

TEST_F(PqpUtilsTest, VisitPqp) {
  // clang-format off
  const std::vector<std::shared_ptr<AbstractOperatorProxy>> expected_proxies = {
    FilterOperatorProxy::Make(GreaterThan_(a_a_, 4)),
    UnionOperatorProxy::Make(SetOperationMode::kAll),
    FilterOperatorProxy::Make(LessThan_(a_a_, 4)),
    FilterOperatorProxy::Make(Equals_(a_a_, 4)),
    import_proxy_a_
  };
  // clang-format on

  expected_proxies[0]->SetLeftInput(expected_proxies[1]);
  expected_proxies[1]->SetLeftInput(expected_proxies[2]);
  expected_proxies[1]->SetRightInput(expected_proxies[3]);
  expected_proxies[2]->SetLeftInput(import_proxy_a_);
  expected_proxies[3]->SetLeftInput(import_proxy_a_);

  std::cout << *expected_proxies[0];

  {
    // Visit AbstractOperatorProxy
    std::vector<std::shared_ptr<AbstractOperatorProxy>> actual_proxies;
    VisitPqp(expected_proxies[0], [&](const auto& proxy) {
      actual_proxies.emplace_back(proxy);
      return PqpVisitation::kVisitInputs;
    });

    EXPECT_EQ(actual_proxies, expected_proxies);
  }

  {
    // Visit FilterOperatorProxy
    std::vector<std::shared_ptr<AbstractOperatorProxy>> actual_proxies;
    VisitPqp(std::static_pointer_cast<FilterOperatorProxy>(expected_proxies[0]), [&](const auto& proxy) {
      actual_proxies.emplace_back(proxy);
      return PqpVisitation::kVisitInputs;
    });

    EXPECT_EQ(actual_proxies, expected_proxies);
  }
}

TEST_F(PqpUtilsTest, VisitPqpUpwards) {
  // clang-format off
  const std::vector<std::shared_ptr<AbstractOperatorProxy>> expected_proxies = {
    import_proxy_a_,
    FilterOperatorProxy::Make(GreaterThan_(a_a_, 4)),
    FilterOperatorProxy::Make(LessThan_(a_a_, 4)),
    UnionOperatorProxy::Make(SetOperationMode::kAll),
    FilterOperatorProxy::Make(Equals_(a_a_, 4))
  };
  // clang-format on

  expected_proxies[4]->SetLeftInput(expected_proxies[3]);
  expected_proxies[3]->SetLeftInput(expected_proxies[1]);
  expected_proxies[3]->SetRightInput(expected_proxies[2]);
  expected_proxies[1]->SetLeftInput(import_proxy_a_);
  expected_proxies[2]->SetLeftInput(import_proxy_a_);

  std::cout << *expected_proxies[4];

  {
    std::vector<std::shared_ptr<AbstractOperatorProxy>> actual_proxies;
    VisitPqpUpwards(import_proxy_a_, [&](const auto& proxy) {
      actual_proxies.emplace_back(proxy);
      return PqpUpwardVisitation::kVisitOutputs;
    });

    EXPECT_EQ(actual_proxies, expected_proxies);
  }
}

TEST_F(PqpUtilsTest, PqpFindLeaves) {
  // Based on PqpFindNodesByType test
  std::vector<std::shared_ptr<JoinOperatorPredicate>> secondary_predicates;
  // clang-format off
  auto pqp =
  JoinOperatorProxy::Make(JoinMode::kInner, JoinOperatorPredicate_(Equals_(a_a_, b_x_)), secondary_predicates,
    UnionOperatorProxy::Make(SetOperationMode::kAll,
      FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
        import_proxy_a_),
      FilterOperatorProxy::Make(LessThan_(a_b_, 123),
        import_proxy_a_)),
    import_proxy_b_);
  // clang-format on

  const auto leaf_proxies = PqpFindLeaves(pqp);
  // Since the LqpTranslator deduplicates the PQP, we expect two leaf proxies only.
  ASSERT_EQ(leaf_proxies.size(), 2);
  EXPECT_EQ(leaf_proxies.at(0), import_proxy_b_);
  EXPECT_EQ(leaf_proxies.at(1), import_proxy_a_);
}

TEST_F(PqpUtilsTest, PrefixOperatorProxyIdentities) {
  std::vector<std::shared_ptr<JoinOperatorPredicate>> secondary_predicates;
  // clang-format off
  auto pqp =
  JoinOperatorProxy::Make(JoinMode::kInner, JoinOperatorPredicate_(Equals_(a_a_, b_x_)), secondary_predicates,
    UnionOperatorProxy::Make(SetOperationMode::kAll,
      FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
        import_proxy_a_),
      FilterOperatorProxy::Make(LessThan_(a_b_, 123),
        import_proxy_a_)),
    import_proxy_b_);
  // clang-format on

  PrefixOperatorProxyIdentities(pqp, "my-prefix");

  // Collect all identities
  std::vector<std::string> identities;
  VisitPqp(pqp, [&](const auto& proxy) {
    identities.emplace_back(proxy->Identity());
    return PqpVisitation::kVisitInputs;
  });

  for (const auto& identity : identities) {
    // Check whether identity starts with prefix
    EXPECT_EQ(identity.find("my-prefix"), 0);
  }
}

}  // namespace skyrise
