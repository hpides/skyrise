#include "tpch/tpch_mock_catalog.hpp"

#include <gtest/gtest.h>

#include "storage/table/table_column_definition.hpp"
#include "types.hpp"

namespace skyrise {

class TpchMockCatalogTest : public ::testing::Test {
 public:
  void SetUp() override { tpch_mock_catalog_ = TpchMockCatalog(); }

 protected:
  TpchMockCatalog tpch_mock_catalog_;
};

TEST_F(TpchMockCatalogTest, ContainsAllTables) {
  EXPECT_TRUE(tpch_mock_catalog_.TableExists("customer"));
  EXPECT_TRUE(tpch_mock_catalog_.TableExists("lineitem"));
  EXPECT_TRUE(tpch_mock_catalog_.TableExists("nation"));
  EXPECT_TRUE(tpch_mock_catalog_.TableExists("orders"));
  EXPECT_TRUE(tpch_mock_catalog_.TableExists("part"));
  EXPECT_TRUE(tpch_mock_catalog_.TableExists("partsupp"));
  EXPECT_TRUE(tpch_mock_catalog_.TableExists("region"));
  EXPECT_TRUE(tpch_mock_catalog_.TableExists("supplier"));
}

}  // namespace skyrise
