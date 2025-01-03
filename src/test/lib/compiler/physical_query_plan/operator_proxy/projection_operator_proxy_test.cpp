#include "compiler/physical_query_plan/operator_proxy/projection_operator_proxy.hpp"

#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "types.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class ProjectionOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    a_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a");
    b_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b");

    expressions_ = ExpressionVector_(a_, b_, Add_(a_, b_));
  }

 protected:
  std::shared_ptr<PqpColumnExpression> a_, b_;
  std::vector<std::shared_ptr<AbstractExpression>> expressions_;
};

TEST_F(ProjectionOperatorProxyTest, BaseProperties) {
  const auto projection_proxy = ProjectionOperatorProxy::Make(expressions_);
  EXPECT_EQ(projection_proxy->Type(), OperatorType::kProjection);
  EXPECT_EQ(projection_proxy->Expressions(), expressions_);
  EXPECT_FALSE(projection_proxy->IsPipelineBreaker());
  EXPECT_EQ(projection_proxy->OutputColumnsCount(), expressions_.size());
}

TEST_F(ProjectionOperatorProxyTest, Description) {
  const auto projection_proxy = ProjectionOperatorProxy::Make(expressions_);
  EXPECT_EQ(projection_proxy->Description(DescriptionMode::kSingleLine), "[Projection]");
  EXPECT_EQ(projection_proxy->Description(DescriptionMode::kMultiLine), "[Projection]");
}

TEST_F(ProjectionOperatorProxyTest, SerializeAndDeserialize) {
  const auto projection_proxy = ProjectionOperatorProxy::Make(expressions_);
  // (1) Serialize
  const auto proxy_json = projection_proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_proxy = ProjectionOperatorProxy::FromJson(proxy_json);
  const auto deserialized_projection_proxy = std::dynamic_pointer_cast<ProjectionOperatorProxy>(deserialized_proxy);
  EXPECT_TRUE(ExpressionsEqual(deserialized_projection_proxy->Expressions(), expressions_));

  // (3) Serialize again
  EXPECT_EQ(proxy_json, deserialized_proxy->ToJson());
}

TEST_F(ProjectionOperatorProxyTest, DeepCopy) {
  // clang-format off
  const auto projection_proxy =
  ProjectionOperatorProxy::Make(expressions_,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  const auto projection_proxy_copy = std::dynamic_pointer_cast<ProjectionOperatorProxy>(projection_proxy->DeepCopy());
  EXPECT_NE(projection_proxy_copy->Expressions(), expressions_);
  EXPECT_TRUE(ExpressionsEqual(projection_proxy_copy->Expressions(), expressions_));
  EXPECT_EQ(projection_proxy_copy->InputNodeCount(), 1);
  // Without input
  projection_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(projection_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(ProjectionOperatorProxyTest, CreateOperatorInstance) {
  // clang-format off
  const auto projection_proxy =
  ProjectionOperatorProxy::Make(expressions_, 
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  EXPECT_TRUE(projection_proxy->GetOrCreateOperatorInstance());
}

}  // namespace skyrise
