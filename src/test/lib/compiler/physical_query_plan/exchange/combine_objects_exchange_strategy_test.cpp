#include "compiler/physical_query_plan/exchange/combine_objects_exchange_strategy.hpp"

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "compiler/compilation_context.hpp"
#include "configuration.hpp"
#include "metadata/mock_catalog.hpp"
#include "pqp_test_utils.hpp"

namespace skyrise {

class CombineObjectsExchangeStrategyTest : public ::testing::Test {
 public:
  void SetUp() override {
    SqlRequest request("SELECT * FROM XY", kMockUserName, std::chrono::system_clock::now());
    compilation_context_ = std::make_shared<CompilationContext>(request, std::make_shared<MockCatalog>());
  }

 protected:
  std::shared_ptr<CompilationContext> compilation_context_;
  const std::string kMockUserName = "mock_user";
  const std::string kMockExportBucketName = "mock_target_bucket";
};

TEST_F(CombineObjectsExchangeStrategyTest, Properties) {
  EXPECT_THROW(CombineObjectsExchangeStrategy::Create(0), std::logic_error);

  const auto full_merge_strategy = CombineObjectsExchangeStrategy::Create(1);
  EXPECT_EQ(full_merge_strategy->TargetPartitionCount(), 1);
  EXPECT_EQ(full_merge_strategy->TargetObjectCount(1), 1);
  EXPECT_EQ(full_merge_strategy->TargetObjectCount(200), 1);

  const auto partial_merge_strategy = CombineObjectsExchangeStrategy::Create(50);
  EXPECT_EQ(partial_merge_strategy->TargetPartitionCount(), 1);
  EXPECT_EQ(partial_merge_strategy->TargetObjectCount(1), 50);
  EXPECT_EQ(partial_merge_strategy->TargetObjectCount(50), 50);
  EXPECT_EQ(partial_merge_strategy->TargetObjectCount(200), 50);
}

TEST_F(CombineObjectsExchangeStrategyTest, ComputeExchangeResultSingleImportProxy) {
//  ExchangeResult ComputeExchangeResult(
//      const size_t pipeline_id, const std::shared_ptr<CompilationContext>& compilation_context,
//      const std::vector<std::shared_ptr<ImportOperatorProxy>>& import_proxies) const override;

  const auto mock_object_references = CreateMockObjectReferences("table_a", 100);
  const std::vector<ColumnId> import_column_ids = {ColumnId{1}, ColumnId{3}};
  const auto import_proxy = ImportOperatorProxy::Make(mock_object_references, import_column_ids);

  size_t pipeline_id = 1;

}

TEST_F(CombineObjectsExchangeStrategyTest, ComputeExchangeResultMultipleImportProxy) {

}

}  // namespace skyrise
