#include "utils/vector.hpp"

#include <gtest/gtest.h>

namespace skyrise {

TEST(VectorUtilsTest, SplitVectorIntoChunks) {
  const std::vector<std::string> vector = {"a", "b", "c", "d"};
  {
    const size_t chunk_size = 1;
    const auto vector_chunks = SplitVectorIntoChunks(vector, chunk_size);
    ASSERT_EQ(vector_chunks.size(), 4);
    EXPECT_EQ(vector_chunks.at(0), std::vector<std::string>({"a"}));
    EXPECT_EQ(vector_chunks.at(1), std::vector<std::string>({"b"}));
    EXPECT_EQ(vector_chunks.at(2), std::vector<std::string>({"c"}));
    EXPECT_EQ(vector_chunks.at(3), std::vector<std::string>({"d"}));
  }
  {
    const size_t chunk_size = 2;
    const auto vector_chunks = SplitVectorIntoChunks(vector, chunk_size);
    ASSERT_EQ(vector_chunks.size(), 2);
    EXPECT_EQ(vector_chunks.at(0), std::vector<std::string>({"a", "b"}));
    EXPECT_EQ(vector_chunks.at(1), std::vector<std::string>({"c", "d"}));
  }
  {
    const size_t chunk_size = 3;
    const auto vector_chunks = SplitVectorIntoChunks(vector, chunk_size);
    ASSERT_EQ(vector_chunks.size(), 2);
    EXPECT_EQ(vector_chunks.at(0), std::vector<std::string>({"a", "b", "c"}));
    EXPECT_EQ(vector_chunks.at(1), std::vector<std::string>({"d"}));
  }
  {
    const size_t chunk_size = 4;
    const auto vector_chunks = SplitVectorIntoChunks(vector, chunk_size);
    ASSERT_EQ(vector_chunks.size(), 1);
    EXPECT_EQ(vector_chunks.at(0), vector);
  }
  {
    const size_t chunk_size = 5;
    const auto vector_chunks = SplitVectorIntoChunks(vector, chunk_size);
    ASSERT_EQ(vector_chunks.size(), 1);
    EXPECT_EQ(vector_chunks.at(0), vector);
  }
}

}  // namespace skyrise
