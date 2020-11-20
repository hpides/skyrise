#include "storage/formats/csv.hpp"

#include <memory>
#include <sstream>
#include <string>

#include "gtest/gtest.h"
#include "storage/types/value_segment.hpp"

namespace skyrise {

class CSVFormatterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_.push_back(TableColumnDefinition("id", DataType::kInt, false));
    schema_.push_back(TableColumnDefinition("text", DataType::kString, false));

    value_segment_int_ = std::make_shared<ValueSegment<int>>();
    value_segment_int_->Append(4);
    value_segment_int_->Append(6);
    value_segment_int_->Append(3);

    value_segment_str_ = std::make_shared<ValueSegment<std::string>>();
    value_segment_str_->Append("Hello");
    value_segment_str_->Append("world");
    value_segment_str_->Append("!");

    chunk_ = std::make_shared<Chunk>(Segments({value_segment_int_, value_segment_str_}));
  }

  std::shared_ptr<Chunk> chunk_;
  std::shared_ptr<BaseValueSegment> value_segment_int_;
  std::shared_ptr<BaseValueSegment> value_segment_str_;
  TableColumnDefinitions schema_;
};

TEST_F(CSVFormatterTest, FormatChunkAsCSV) {
  std::stringstream output;
  std::shared_ptr<std::stringstream> output_ptr(&output, [](auto /*unused*/) {});

  CSVFormatterOptions options;
  options.include_headers = true;
  options.field_separator = ",";
  options.record_separator = "\n";

  CSVFormatter formatter(options);
  formatter.SetOutput(output_ptr);

  formatter.Initialize(schema_);
  formatter.ProcessChunk(*chunk_);
  formatter.Finalize();

  ASSERT_EQ("id,text\n4,Hello\n6,world\n3,!\n", output.str());
}

}  // namespace skyrise
