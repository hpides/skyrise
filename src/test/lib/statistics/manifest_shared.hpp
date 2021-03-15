#pragma once

#include <memory>

#include <gtest/gtest.h>

#include "../storage/backend/mock_storage.hpp"
#include "statistics/statistics_collector.hpp"

namespace skyrise {

class ManifestTest : public ::testing::Test {
 protected:
  void SetUp() override;

  static constexpr auto kDataFile = "files/lineitem.orc";
  ObjectStatistics statistics_;
  std::shared_ptr<Storage> storage_;
  std::shared_ptr<TableColumnDefinitions> schema_;
};

}  // namespace skyrise
