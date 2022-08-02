#include "compiler/physical_query_plan/operator_proxy/exchange_operator_proxy.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

namespace {

//const std::vector<ObjectReference> kObjectReferences = {ObjectReference{"dummy_bucket", "key1.orc", "etag1"},
//                                                        ObjectReference{"dummy_bucket", "key2.orc", "etag1"},
//                                                        ObjectReference{"dummy_bucket", "key3.orc", "etag3"}};
//const std::vector<ColumnId> kColumnIds = {ColumnId{0}, ColumnId{1}, ColumnId{3}};

}  // namespace

TEST(ExchangeOperatorProxyTest, BaseProperties) {
  EXPECT_EQ(exchange_proxy->Type(), OperatorType::kExchange);
  const auto exchange_proxy = ExchangeOperatorProxy::Make(std::make_shared<const CombineObjectsExchangeStrategy>(1));
  EXPECT_EQ(exchange_proxy->Type(), ExchangeStrategyType::kCombineObjects);
  EXPECT_FALSE(exchange_proxy->IsPipelineBreaker());

  // TODO Switch to Shuffle strategy
//  const std::shared_ptr<const AbstractPartitioningFunction> partitioning_function =
//      std::make_shared<const HashPartitioningFunction>(std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}, 50);
//  exchange_proxy->SetStrategy();
//  EXPECT_EQ(exchange_proxy->Type(), ExchangeStrategyType::kShuffle);
}

TEST(ExchangeOperatorProxyTest, Description) {
  const auto exchange_proxy = ExchangeOperatorProxy::Make(std::make_shared<const CombineObjectsExchangeStrategy>(50));
  EXPECT_EQ(exchange_proxy->Description(DescriptionMode::kSingleLine), "[Exchange] Combine Objects Target: 50 object(s), 1 partition(s)");
  EXPECT_EQ(exchange_proxy->Description(DescriptionMode::kMultiLine), "[Exchange]\nCombine Objects\nTarget: 50 object(s),\n1 partition(s)");

  // TODO Shuffle
}

//TEST(ExchangeOperatorProxyTest, OutputObjectsCount) {
//  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
//  import_proxy->SetOutputObjectsCount(100);
//  {
//    // Full Merge
//    const auto exchange_proxy = ExchangeOperatorProxy::Make();
//    EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 1);
//  }
//  {
//    // Partial Merge
//    const auto exchange_proxy = ExchangeOperatorProxy::Make();
//    exchange_proxy->SetToPartialMerge(50);
//    EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 50);
//  }
//  {
//    // Fully Meshed Exchange
//    const auto exchange_proxy = ExchangeOperatorProxy::Make(import_proxy);
//    const std::shared_ptr<const AbstractPartitioningFunction> partitioning_function =
//        std::make_shared<const HashPartitioningFunction>(std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}, 50);
//    exchange_proxy->SetToFullyMeshedExchange(partitioning_function);
//    EXPECT_EQ(exchange_proxy->OutputObjectsCount(), import_proxy->OutputObjectsCount());
//    exchange_proxy->SetToFullyMeshedExchange(partitioning_function, 5);
//    EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 5);
//  }
//}
//
//TEST(ExchangeOperatorProxyTest, OutputPartitionsCount) {
//  {
//    // Full Merge
//    const auto exchange_proxy = ExchangeOperatorProxy::Make();
//    EXPECT_EQ(exchange_proxy->OutputPartitionsCount(), 1);
//  }
//  {
//    // Partial Merge
//    const auto exchange_proxy = ExchangeOperatorProxy::Make();
//    exchange_proxy->SetToPartialMerge(50);
//    EXPECT_EQ(exchange_proxy->OutputPartitionsCount(), 1);
//  }
//  {
//    // Fully Meshed Exchange
//    const auto exchange_proxy = ExchangeOperatorProxy::Make();
//    std::vector<ColumnId> partitioning_column_ids(ColumnId{0}, ColumnId{1});
//    const std::shared_ptr<const AbstractPartitioningFunction> partitioning_function =
//        std::make_shared<const HashPartitioningFunction>(partitioning_column_ids, 50);
//    exchange_proxy->SetToFullyMeshedExchange(partitioning_function);
//    EXPECT_EQ(exchange_proxy->OutputPartitionsCount(), 50);
//  }
//}
//
//TEST(ExchangeOperatorProxyTest, DeepCopy) {
//  // clang-format off
//  const auto exchange_proxy =
//  ExchangeOperatorProxy::Make(
//    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}}));
//
//  // clang-format on
//  exchange_proxy->SetToPartialMerge(50);
//  const auto exchange_proxy_copy = std::dynamic_pointer_cast<ExchangeOperatorProxy>(exchange_proxy->DeepCopy());
//  EXPECT_EQ(exchange_proxy_copy->Type(), exchange_proxy->Type());
//  EXPECT_EQ(exchange_proxy_copy->OutputObjectsCount(), 50);
//  EXPECT_EQ(exchange_proxy_copy->InputNodeCount(), 1);
//  // Without input
//  exchange_proxy->SetLeftInput(nullptr);
//  EXPECT_EQ(exchange_proxy->DeepCopy()->InputNodeCount(), 0);
//}

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
