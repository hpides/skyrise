#include "compiler/physical_query_plan/data_exchange_operator_proxy.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class DataExchangeOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {}
};

TEST_F(DataExchangeOperatorProxyTest, BaseProperties) {
  auto data_exchange_proxy = DataExchangeOperatorProxy::Make();
  EXPECT_EQ(data_exchange_proxy->Type(), OperatorType::kDataExchange);
  EXPECT_EQ(data_exchange_proxy->GetDataExchangeMode(), DataExchangeMode::kFullMerge);
  EXPECT_FALSE(data_exchange_proxy->IsPipelineBreaker());
}

TEST_F(DataExchangeOperatorProxyTest, DescriptionFullMerge) {
  auto data_exchange_proxy = DataExchangeOperatorProxy::Make();
  EXPECT_EQ(data_exchange_proxy->Description(DescriptionMode::kSingleLine), "[DataExchange] Full Merge");
  EXPECT_EQ(data_exchange_proxy->Description(DescriptionMode::kMultiLine), "[DataExchange]\nFull Merge");
}

TEST_F(DataExchangeOperatorProxyTest, DescriptionPartialMerge) {
  auto data_exchange_proxy = DataExchangeOperatorProxy::Make();
  data_exchange_proxy->SetToPartialMerge(50);
  EXPECT_EQ(data_exchange_proxy->Description(DescriptionMode::kSingleLine), "[DataExchange] Partial Merge, 50 objects");
  EXPECT_EQ(data_exchange_proxy->Description(DescriptionMode::kMultiLine), "[DataExchange]\nPartial Merge\n50 objects");
}

TEST_F(DataExchangeOperatorProxyTest, SetDataExchangeMode) {
  auto data_exchange_proxy = DataExchangeOperatorProxy::Make();
  data_exchange_proxy->SetToPartialMerge(100);
  EXPECT_EQ(data_exchange_proxy->GetDataExchangeMode(), DataExchangeMode::kPartialMerge);
  data_exchange_proxy->SetToFullMerge();
  EXPECT_EQ(data_exchange_proxy->GetDataExchangeMode(), DataExchangeMode::kFullMerge);
}

TEST_F(DataExchangeOperatorProxyTest, OutputObjectsCountFullMerge) {
  auto data_exchange_proxy = DataExchangeOperatorProxy::Make();
  EXPECT_EQ(data_exchange_proxy->OutputObjectsCount(), 1);
}

TEST_F(DataExchangeOperatorProxyTest, OutputObjectsCountPartialMerge) {
  auto data_exchange_proxy = DataExchangeOperatorProxy::Make();
  data_exchange_proxy->SetToPartialMerge(50);
  EXPECT_EQ(data_exchange_proxy->OutputObjectsCount(), 50);
}

TEST_F(DataExchangeOperatorProxyTest, DeepCopy) {
  // clang-format off
  auto data_exchange_proxy =
  DataExchangeOperatorProxy::Make(
    ImportOperatorProxy::Make("bucket_name", std::vector<std::string>{"import.orc"}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  data_exchange_proxy->SetToPartialMerge(50);
  auto data_exchange_proxy_copy = std::dynamic_pointer_cast<DataExchangeOperatorProxy>(data_exchange_proxy->DeepCopy());
  EXPECT_EQ(data_exchange_proxy_copy->GetDataExchangeMode(), data_exchange_proxy->GetDataExchangeMode());
  EXPECT_EQ(data_exchange_proxy_copy->OutputObjectsCount(), 50);
  EXPECT_EQ(data_exchange_proxy_copy->InputNodeCount(), 1);
  // Without input
  data_exchange_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(data_exchange_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(DataExchangeOperatorProxyTest, DisabledFunctionality) {
  // clang-format off
  auto data_exchange_proxy =
  DataExchangeOperatorProxy::Make(
    ImportOperatorProxy::Make("bucket_name", std::vector<std::string>{"import.orc"}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  EXPECT_THROW(data_exchange_proxy->ToJson(), std::logic_error);
  EXPECT_THROW(data_exchange_proxy->GetOrCreateOperatorInstance(), std::logic_error);
}

}  // namespace skyrise
