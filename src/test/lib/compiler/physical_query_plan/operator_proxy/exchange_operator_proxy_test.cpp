#include "compiler/physical_query_plan/operator_proxy/exchange_operator_proxy.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class ExchangeOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    for (size_t i = 0; i < 100; ++i) {
      object_references.emplace_back("dummy_bucket", "key.orc", "etag");
    }
    partitioning_function_ =
        std::make_shared<const HashPartitioningFunction>(kPartitionColumnIds, kTargetPartitionCount);
    exchange_proxy_broadcast_ = ExchangeOperatorProxy::Make(ExchangeType::kBroadcast, 1);
    exchange_proxy_combine_ = ExchangeOperatorProxy::Make(ExchangeType::kCombine, kTargetBucketCount);
    exchange_proxy_shuffle_ =
        ExchangeOperatorProxy::Make(ExchangeType::kShuffle, kTargetBucketCount, partitioning_function_);
  }

 protected:
  std::shared_ptr<const AbstractPartitioningFunction> partitioning_function_;
  static inline const std::vector<ColumnId> kPartitionColumnIds = {ColumnId{0}, ColumnId{1}};

  std::shared_ptr<ExchangeOperatorProxy> exchange_proxy_broadcast_;
  std::shared_ptr<ExchangeOperatorProxy> exchange_proxy_combine_;
  std::shared_ptr<ExchangeOperatorProxy> exchange_proxy_shuffle_;
  static const size_t kTargetBucketCount = 10;
  static const size_t kTargetPartitionCount = 20;

  std::vector<ObjectReference> object_references_;
  static inline const std::vector<ColumnId> kColumnIds = {ColumnId{0}, ColumnId{1}, ColumnId{3}};
};

TEST_F(ExchangeOperatorProxyTest, BaseProperties) {
  EXPECT_EQ(exchange_proxy_combine_->Type(), OperatorType::kExchange);
  EXPECT_FALSE(exchange_proxy_combine_->IsPipelineBreaker());

  // Broadcast
  EXPECT_THROW(ExchangeOperatorProxy::Make(ExchangeType::kBroadcast, 1, partitioning_function_), std::logic_error);
  EXPECT_THROW(ExchangeOperatorProxy::Make(ExchangeType::kBroadcast, 2), std::logic_error);
  EXPECT_EQ(exchange_proxy_broadcast_->GetExchangeType(), ExchangeType::kBroadcast);
  EXPECT_EQ(exchange_proxy_broadcast_->TargetBucketCount(), 1);
  EXPECT_EQ(exchange_proxy_broadcast_->TargetPartitioningFunction(), std::nullopt);

  // Combine
  EXPECT_THROW(ExchangeOperatorProxy::Make(ExchangeType::kCombine, 0), std::logic_error);
  EXPECT_EQ(exchange_proxy_combine_->GetExchangeType(), ExchangeType::kCombine);
  EXPECT_EQ(exchange_proxy_combine_->TargetBucketCount(), kTargetBucketCount);
  EXPECT_EQ(exchange_proxy_combine_->TargetPartitioningFunction(), std::nullopt);

  // Shuffle
  EXPECT_THROW(ExchangeOperatorProxy::Make(ExchangeType::kShuffle, 1), std::logic_error);
  EXPECT_EQ(exchange_proxy_shuffle_->GetExchangeType(), ExchangeType::kShuffle);
  EXPECT_EQ(exchange_proxy_shuffle_->TargetBucketCount(), kTargetBucketCount);
  EXPECT_EQ(exchange_proxy_shuffle_->TargetPartitioningFunction(), partitioning_function_);
}

TEST_F(ExchangeOperatorProxyTest, Description) {
  // Broadcast
  EXPECT_EQ(exchange_proxy_broadcast_->Description(DescriptionMode::kSingleLine), "[Exchange] Broadcast");
  EXPECT_EQ(exchange_proxy_broadcast_->Description(DescriptionMode::kMultiLine), "[Exchange]\nBroadcast");
  // Combine
  EXPECT_EQ(exchange_proxy_combine_->Description(DescriptionMode::kSingleLine), "[Exchange] Combine: 10 bucket(s)");
  EXPECT_EQ(exchange_proxy_combine_->Description(DescriptionMode::kMultiLine), "[Exchange]\nCombine\n10 bucket(s)");
  // Shuffle
  EXPECT_EQ(exchange_proxy_shuffle_->Description(DescriptionMode::kSingleLine),
            "[Exchange] Shuffle: 10 bucket(s), 20 partition(s)");
  EXPECT_EQ(exchange_proxy_shuffle_->Description(DescriptionMode::kMultiLine),
            "[Exchange]\nShuffle\n10 bucket(s),\n20 partition(s)");
}

TEST_F(ExchangeOperatorProxyTest, OutputDataTraits) {
  EXPECT_THROW(exchange_proxy_broadcast_->OutputDataTraits(), std::logic_error);

  std::vector<ObjectReference> object_references_single;
  object_references_single.emplace_back("dummy_bucket", "key.orc", "etag");
  const auto import_proxy_single_bucket = ImportOperatorProxy::Make(object_references_single, kColumnIds);

  ASSERT_TRUE(kTargetBucketCount < object_references_.size());
  const auto import_proxy = ImportOperatorProxy::Make(object_references_, kColumnIds);

  // Broadcast
  exchange_proxy_broadcast_->SetLeftInput(import_proxy_single_bucket);
  EXPECT_NO_THROW(exchange_proxy_broadcast_->OutputDataTraits());
  exchange_proxy_broadcast_->SetLeftInput(import_proxy);
  EXPECT_EQ(exchange_proxy_broadcast_->OutputDataTraits().bucket_count, 1);
  EXPECT_EQ(exchange_proxy_broadcast_->OutputDataTraits().column_count, kColumnIds.size());
  EXPECT_EQ(exchange_proxy_broadcast_->OutputDataTraits().partition_count, 1);
  EXPECT_FALSE(exchange_proxy_broadcast_->OutputDataTraits().PartitioningIsEnabled());

  // Combine
  exchange_proxy_combine_->SetLeftInput(import_proxy_single_bucket);
  EXPECT_THROW(exchange_proxy_combine_->OutputDataTraits(), std::logic_error);
  exchange_proxy_combine_->SetLeftInput(import_proxy);
  EXPECT_EQ(exchange_proxy_combine_->OutputDataTraits().bucket_count, kTargetBucketCount);
  EXPECT_EQ(exchange_proxy_combine_->OutputDataTraits().column_count, kColumnIds.size());
  EXPECT_EQ(exchange_proxy_combine_->OutputDataTraits().partition_count, 1);
  EXPECT_FALSE(exchange_proxy_combine_->OutputDataTraits().PartitioningIsEnabled());

  // Shuffle
  exchange_proxy_shuffle_->SetLeftInput(import_proxy_single_bucket);
  EXPECT_THROW(exchange_proxy_shuffle_->OutputDataTraits(), std::logic_error);
  exchange_proxy_shuffle_->SetLeftInput(import_proxy);
  EXPECT_EQ(exchange_proxy_shuffle_->OutputDataTraits().bucket_count, kTargetBucketCount);
  EXPECT_EQ(exchange_proxy_shuffle_->OutputDataTraits().column_count, kColumnIds.size());
  EXPECT_EQ(exchange_proxy_shuffle_->OutputDataTraits().partition_count, kTargetPartitionCount);
  EXPECT_TRUE(exchange_proxy_shuffle_->OutputDataTraits().PartitioningIsEnabled());
}

TEST_F(ExchangeOperatorProxyTest, DeepCopy) {
  const auto import_proxy = ImportOperatorProxy::Make(object_references_, kColumnIds);

  // Broadcast
  exchange_proxy_broadcast_->SetLeftInput(import_proxy);
  const auto exchange_proxy_broadcast_copy =
      std::dynamic_pointer_cast<ExchangeOperatorProxy>(exchange_proxy_broadcast_->DeepCopy());
  EXPECT_EQ(exchange_proxy_broadcast_copy->GetExchangeType(), ExchangeType::kBroadcast);
  EXPECT_EQ(exchange_proxy_broadcast_copy->TargetBucketCount(), kTargetBucketCount);
  EXPECT_EQ(exchange_proxy_broadcast_copy->TargetPartitioningFunction(), std::nullopt);
  // Combine
  exchange_proxy_combine_->SetLeftInput(import_proxy);
  const auto exchange_proxy_combine_copy =
      std::dynamic_pointer_cast<ExchangeOperatorProxy>(exchange_proxy_combine_->DeepCopy());
  EXPECT_EQ(exchange_proxy_combine_copy->GetExchangeType(), ExchangeType::kCombine);
  EXPECT_EQ(exchange_proxy_combine_copy->TargetBucketCount(), kTargetBucketCount);
  EXPECT_EQ(exchange_proxy_combine_copy->TargetPartitioningFunction(), std::nullopt);
  // Shuffle
  exchange_proxy_shuffle_->SetLeftInput(import_proxy);
  const auto exchange_proxy_shuffle_copy =
      std::dynamic_pointer_cast<ExchangeOperatorProxy>(exchange_proxy_shuffle_->DeepCopy());
  EXPECT_EQ(exchange_proxy_shuffle_copy->GetExchangeType(), ExchangeType::kShuffle);
  EXPECT_EQ(exchange_proxy_shuffle_copy->TargetBucketCount(), kTargetBucketCount);
  EXPECT_EQ(exchange_proxy_shuffle_copy->TargetPartitioningFunction(), partitioning_function_);

  // Check, if Import proxy was copied
  EXPECT_EQ(exchange_proxy_broadcast_copy->InputNodeCount(), 1);
}

TEST_F(ExchangeOperatorProxyTest, DisabledFunctionality) {
  // clang-format off
  const auto exchange_proxy =
  ExchangeOperatorProxy::Make(ExchangeType::kCombine, 10, std::nullopt,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  EXPECT_THROW(exchange_proxy->ToJson(), std::logic_error);
  EXPECT_THROW(exchange_proxy->GetOrCreateOperatorInstance(), std::logic_error);
}

}  // namespace skyrise
