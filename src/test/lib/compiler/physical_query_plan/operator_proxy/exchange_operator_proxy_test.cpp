#include "compiler/physical_query_plan/operator_proxy/exchange_operator_proxy.hpp"

#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

TEST(ExchangeOperatorProxyTest, BaseProperties) {
  const auto exchange_proxy = ExchangeOperatorProxy::Make();
  EXPECT_EQ(exchange_proxy->Type(), OperatorType::kExchange);
  EXPECT_EQ(exchange_proxy->GetExchangeMode(), ExchangeMode::kFullMerge);
  EXPECT_FALSE(exchange_proxy->IsPipelineBreaker());
}

TEST(ExchangeOperatorProxyTest, DescriptionFullMerge) {
  const auto exchange_proxy = ExchangeOperatorProxy::Make();
  EXPECT_EQ(exchange_proxy->Description(DescriptionMode::kSingleLine), "[DataExchange] Full Merge");
  EXPECT_EQ(exchange_proxy->Description(DescriptionMode::kMultiLine), "[DataExchange]\nFull Merge");
}

TEST(ExchangeOperatorProxyTest, DescriptionPartialMerge) {
  const auto exchange_proxy = ExchangeOperatorProxy::Make();
  exchange_proxy->SetToPartialMerge(50);
  EXPECT_EQ(exchange_proxy->Description(DescriptionMode::kSingleLine), "[DataExchange] Partial Merge, 50 objects");
  EXPECT_EQ(exchange_proxy->Description(DescriptionMode::kMultiLine), "[DataExchange]\nPartial Merge\n50 objects");
}

TEST(ExchangeOperatorProxyTest, SetExchangeMode) {
  const auto exchange_proxy = ExchangeOperatorProxy::Make();
  exchange_proxy->SetToPartialMerge(100);
  EXPECT_EQ(exchange_proxy->GetExchangeMode(), ExchangeMode::kPartialMerge);
  exchange_proxy->SetToFullMerge();
  EXPECT_EQ(exchange_proxy->GetExchangeMode(), ExchangeMode::kFullMerge);
  EXPECT_THROW(exchange_proxy->SetToFullyMeshedExchange(), std::logic_error);
}

TEST(ExchangeOperatorProxyTest, OutputObjectsCountFullMerge) {
  const auto exchange_proxy = ExchangeOperatorProxy::Make();
  EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 1);
}

TEST(ExchangeOperatorProxyTest, OutputObjectsCountPartialMerge) {
  const auto exchange_proxy = ExchangeOperatorProxy::Make();
  exchange_proxy->SetToPartialMerge(50);
  EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 50);
}

TEST(ExchangeOperatorProxyTest, DeepCopy) {
  // clang-format off
  const auto exchange_proxy =
  ExchangeOperatorProxy::Make(
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  exchange_proxy->SetToPartialMerge(50);
  const auto exchange_proxy_copy = std::dynamic_pointer_cast<ExchangeOperatorProxy>(exchange_proxy->DeepCopy());
  EXPECT_EQ(exchange_proxy_copy->GetExchangeMode(), exchange_proxy->GetExchangeMode());
  EXPECT_EQ(exchange_proxy_copy->OutputObjectsCount(), 50);
  EXPECT_EQ(exchange_proxy_copy->InputNodeCount(), 1);
  // Without input
  exchange_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(exchange_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST(ExchangeOperatorProxyTest, DisabledFunctionality) {
  // clang-format off
  const auto exchange_proxy =
  ExchangeOperatorProxy::Make(
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  EXPECT_THROW(exchange_proxy->ToJson(), std::logic_error);
  EXPECT_THROW(exchange_proxy->GetOrCreateOperatorInstance(), std::logic_error);
}

}  // namespace skyrise
