#pragma once

#include "metadata/mock_catalog.hpp"

namespace skyrise {

/**
 * Provides the schema for all TPC-H tables.
 */
class TpchMockCatalog : public MockCatalog {
 public:
  TpchMockCatalog();
};

}  // namespace skyrise
