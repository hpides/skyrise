#include <memory>
#include <sstream>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "storage/types/table_column_definition.hpp"
#include "storage/types/value_segment.hpp"

namespace skyrise {

class FormatterTest : public ::testing::Test {
 protected:
  static constexpr auto kStringExample1 = "Hello";
  static constexpr auto kStringExample2 = "world";
  static constexpr auto kStringExample3 = "!";
  static constexpr auto kColumn1Name = "id";
  static constexpr auto kColumn2Name = "text";

  void SetUp() override {
    schema_.push_back(TableColumnDefinition(kColumn1Name, DataType::kInt, false));
    schema_.push_back(TableColumnDefinition(kColumn2Name, DataType::kString, false));

    value_segment_int_ = std::make_shared<ValueSegment<int>>();
    value_segment_int_->Append(4);
    value_segment_int_->Append(6);
    value_segment_int_->Append(3);

    value_segment_str_ = std::make_shared<ValueSegment<std::string>>();
    value_segment_str_->Append(std::string(kStringExample1));
    value_segment_str_->Append(std::string(kStringExample2));
    value_segment_str_->Append(std::string(kStringExample3));

    chunk_ = std::make_shared<Chunk>(Segments({value_segment_int_, value_segment_str_}));
  }

  std::shared_ptr<Chunk> chunk_;
  std::shared_ptr<BaseValueSegment> value_segment_int_;
  std::shared_ptr<BaseValueSegment> value_segment_str_;
  TableColumnDefinitions schema_;
};

}  // namespace skyrise
