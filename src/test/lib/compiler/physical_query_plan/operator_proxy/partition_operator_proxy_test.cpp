#include "compiler/physical_query_plan/operator_proxy/partition_operator_proxy.hpp"

#include <set>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "operator/partition_operator.hpp"
#include "types.hpp"

namespace skyrise {

class PartitionOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {}

 protected:
  const std::set<ColumnId> partition_column_ids_ = {ColumnId{0}, ColumnId{1}};
  static inline const size_t kPartitionCount = 10;
  const std::shared_ptr<AbstractPartitioningFunction> partitioning_function_ =
      std::make_shared<HashPartitioningFunction>(partition_column_ids_, kPartitionCount);
  static inline const std::vector<ObjectReference> kObjectReferences = {ObjectReference("dummy_bucket", "import.orc")};
};

TEST_F(PartitionOperatorProxyTest, BaseProperties) {
  const auto partiton_proxy = PartitionOperatorProxy::Make(partitioning_function_);
  EXPECT_EQ(partiton_proxy->Type(), OperatorType::kPartition);
  EXPECT_EQ(partiton_proxy->PartitionCount(), kPartitionCount);
  EXPECT_EQ(partiton_proxy->PartitionColumnIds(), partition_column_ids_);
  EXPECT_FALSE(partiton_proxy->IsPipelineBreaker());
}

TEST_F(PartitionOperatorProxyTest, Description) {
  const auto partition_proxy = PartitionOperatorProxy::Make(partitioning_function_);
  EXPECT_EQ(partition_proxy->Description(DescriptionMode::kSingleLine), "[Partition] 10 partition(s) ColumnIds{0, 1}");
  EXPECT_EQ(partition_proxy->Description(DescriptionMode::kMultiLine), "[Partition]\n10 partition(s)\nColumnIds{0, 1}");
}

TEST_F(PartitionOperatorProxyTest, SerializeAndDeserialize) {
  const auto proxy = PartitionOperatorProxy::Make(partitioning_function_);
  // (1) Serialize
  const auto proxy_json = proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_proxy = PartitionOperatorProxy::FromJson(proxy_json);
  const auto deserialized_partition_proxy = std::dynamic_pointer_cast<PartitionOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(deserialized_partition_proxy->PartitionCount(), kPartitionCount);
  EXPECT_EQ(deserialized_partition_proxy->PartitionColumnIds(), partition_column_ids_);

  // (3) Serialize again
  EXPECT_EQ(proxy_json, deserialized_proxy->ToJson());
}

TEST_F(PartitionOperatorProxyTest, DeepCopy) {
  // clang-format off
  const auto partition_proxy =
  PartitionOperatorProxy::Make(partitioning_function_,
    ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  const auto partition_proxy_copy = std::dynamic_pointer_cast<PartitionOperatorProxy>(partition_proxy->DeepCopy());
  EXPECT_EQ(partition_proxy_copy->PartitionCount(), kPartitionCount);
  EXPECT_EQ(partition_proxy_copy->PartitionColumnIds(), partition_column_ids_);
  EXPECT_EQ(partition_proxy_copy->InputNodeCount(), 1);
  // Without input
  partition_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(partition_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(PartitionOperatorProxyTest, CreateOperatorInstance) {
  // clang-format off
  const auto partition_proxy =
  PartitionOperatorProxy::Make(partitioning_function_,
    ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  EXPECT_NE(partition_proxy->GetOrCreateOperatorInstance(), nullptr);
  EXPECT_EQ(partition_proxy->GetOrCreateOperatorInstance()->Type(), OperatorType::kPartition);
}

}  // namespace skyrise
