#include "manifest_shared.hpp"

namespace skyrise {

void ManifestTest::SetUp() {
  // Schema independant statistics.
  statistics_.object_identifier = kDataFile;
  statistics_.etag = "etag";
  statistics_.last_modified = 1824789;
  statistics_.filesize = 1024;
  statistics_.num_rows = 10;

  // Schema itself (gets written into the orc footer).
  schema_ = std::make_shared<TableColumnDefinitions>();
  schema_->emplace_back(TableColumnDefinition("id", DataType::kLong, false));
  schema_->emplace_back(TableColumnDefinition("name", DataType::kString, false));
  schema_->emplace_back(TableColumnDefinition("comment", DataType::kString, true));
  schema_->emplace_back(TableColumnDefinition("price", DataType::kFloat, false));
  schema_->emplace_back(TableColumnDefinition("amount", DataType::kDouble, false));
  statistics_.schema = schema_;

  // Schema dependant statistics.
  statistics_.null_count = std::vector<size_t>{0, 0, 1, 0, 0};
  statistics_.minmax = std::vector<std::pair<AllTypeVariant, AllTypeVariant>>{
      std::make_pair(0L, 10L), std::make_pair("adam", "zerbert"), std::make_pair("x", "y"), std::make_pair(0.0f, 1.0f),
      std::make_pair(0.0, 1.0)};

  storage_ = std::make_shared<MockStorage>();
}

}  // namespace skyrise
