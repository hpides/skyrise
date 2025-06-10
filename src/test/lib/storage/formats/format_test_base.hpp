#include <memory>
#include <sstream>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class FormatterTest : public ::testing::Test {
 protected:
  inline static const std::string kStringExample1{"Hello"};
  inline static const std::string kStringExample2{"world"};
  inline static const std::string kStringExample3{"!"};
  inline static const std::string kColumn1Name{"id"};
  inline static const std::string kColumn2Name{"text"};

  void SetUp() override {
    schema_.emplace_back(kColumn1Name, DataType::kInt, false);
    schema_.emplace_back(kColumn2Name, DataType::kString, false);

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
