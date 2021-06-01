/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "storage/table/chunk.hpp"

#include <gtest/gtest.h>

#include "storage/table/value_segment.hpp"

namespace skyrise {

class StorageChunkTest : public ::testing::Test {
 protected:
  void SetUp() override {
    value_segment_int_ = std::make_shared<ValueSegment<int>>();
    value_segment_int_->Append(4);
    value_segment_int_->Append(6);
    value_segment_int_->Append(3);

    value_segment_str_ = std::make_shared<ValueSegment<std::string>>();
    value_segment_str_->Append(std::string("Hello,"));
    value_segment_str_->Append(std::string("world"));
    value_segment_str_->Append(std::string("!"));

    Segments empty_segments;
    empty_segments.emplace_back(std::make_shared<ValueSegment<int32_t>>());
    empty_segments.emplace_back(std::make_shared<ValueSegment<std::string>>());

    chunk_ = std::make_shared<Chunk>(empty_segments);
  }

  std::shared_ptr<Chunk> chunk_;
  std::shared_ptr<BaseValueSegment> value_segment_int_;
  std::shared_ptr<BaseValueSegment> value_segment_str_;
};

TEST_F(StorageChunkTest, AddSegmentToChunk) {
  EXPECT_EQ(chunk_->Size(), 0);
  chunk_ = std::make_shared<Chunk>(Segments({value_segment_int_, value_segment_str_}));
  EXPECT_EQ(chunk_->Size(), 3);
  EXPECT_EQ(chunk_->GetColumnCount(), 2);
}

TEST_F(StorageChunkTest, AddValuesToChunk) {
  chunk_ = std::make_shared<Chunk>(Segments({value_segment_int_, value_segment_str_}));
  chunk_->Append({2, std::string("two")});
  EXPECT_EQ(chunk_->Size(), 4);
}

TEST_F(StorageChunkTest, RetrieveSegment) {
  chunk_ = std::make_shared<Chunk>(Segments({value_segment_int_, value_segment_str_}));
  chunk_->Append({2, std::string("two")});

  auto abstract_segment = chunk_->GetSegment(ColumnId{0});
  EXPECT_EQ(abstract_segment->Size(), 4);
}

}  // namespace skyrise
