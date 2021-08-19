#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "mock_catalog.hpp"
#include "table_schema.hpp"

namespace skyrise {

/**
 * Provides TableSchema data for all TPC-H tables.
 */
class TpchMockCatalog : public MockCatalog {
 public:
  TpchMockCatalog();
};

}  // namespace skyrise