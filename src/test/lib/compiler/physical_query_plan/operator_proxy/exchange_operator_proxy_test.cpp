#include "compiler/physical_query_plan/operator_proxy/exchange_operator_proxy.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

namespace {

const std::vector<ObjectReference> kObjectReferences = {ObjectReference{"dummy_bucket", "key1.orc", "etag1"},
                                                        ObjectReference{"dummy_bucket", "key2.orc", "etag1"},
                                                        ObjectReference{"dummy_bucket", "key3.orc", "etag3"}};
const std::vector<ColumnId> kColumnIds = {ColumnId{0}, ColumnId{1}, ColumnId{3}};

}  // namespace

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
  // Partial Merge
  exchange_proxy->SetToPartialMerge(100);
  EXPECT_EQ(exchange_proxy->GetExchangeMode(), ExchangeMode::kPartialMerge);
  // Full Merge
  exchange_proxy->SetToFullMerge();
  EXPECT_EQ(exchange_proxy->GetExchangeMode(), ExchangeMode::kFullMerge);
  // Fully Meshed Exchange
  const std::shared_ptr<const AbstractPartitioningFunction> partitioning_function =
      std::make_shared<const HashPartitioningFunction>(std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}, 50);
  exchange_proxy->SetToFullyMeshedExchange(partitioning_function);
  EXPECT_EQ(exchange_proxy->GetExchangeMode(), ExchangeMode::kFullyMeshedExchange);
}

TEST(ExchangeOperatorProxyTest, OutputObjectsCount) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  import_proxy->SetOutputObjectsCount(100);
  {
    // Full Merge
    const auto exchange_proxy = ExchangeOperatorProxy::Make();
    EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 1);
  }
  {
    // Partial Merge
    const auto exchange_proxy = ExchangeOperatorProxy::Make();
    exchange_proxy->SetToPartialMerge(50);
    EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 50);
  }
  {
    // Fully Meshed Exchange
    const auto exchange_proxy = ExchangeOperatorProxy::Make(import_proxy);
    const std::shared_ptr<const AbstractPartitioningFunction> partitioning_function =
        std::make_shared<const HashPartitioningFunction>(std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}, 50);
    exchange_proxy->SetToFullyMeshedExchange(partitioning_function);
    EXPECT_EQ(exchange_proxy->OutputObjectsCount(), import_proxy->OutputObjectsCount());
    exchange_proxy->SetToFullyMeshedExchange(partitioning_function, 5);
    EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 5);
  }
}

TEST(ExchangeOperatorProxyTest, OutputPartitionsCount) {
  {
    // Full Merge
    const auto exchange_proxy = ExchangeOperatorProxy::Make();
    EXPECT_EQ(exchange_proxy->OutputPartitonsCount(), 1);
  }
  {
    // Partial Merge
    const auto exchange_proxy = ExchangeOperatorProxy::Make();
    exchange_proxy->SetToPartialMerge(50);
    EXPECT_EQ(exchange_proxy->OutputPartitonsCount(), 1);
  }
  {
    // Fully Meshed Exchange
    const auto exchange_proxy = ExchangeOperatorProxy::Make(import_proxy);
    const std::shared_ptr<const AbstractPartitioningFunction> partitioning_function =
        std::make_shared<const HashPartitioningFunction>(std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}, 50);
    exchange_proxy->SetToFullyMeshedExchange(partitioning_function);
    EXPECT_EQ(exchange_proxy->OutputPartitonsCount(), 50);
  }
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
