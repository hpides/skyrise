#include "storage/io_handle/byte_buffer.hpp"

#include <array>

#include <gtest/gtest.h>

namespace skyrise {

TEST(ByteBufferTest, WriteToView) {
  std::array<uint8_t, 8> memory{};
  ByteBuffer buffer(memory.data(), 8);
  EXPECT_EQ(buffer.Data(), memory.data());
  EXPECT_EQ(buffer.Size(), 0);
}

TEST(ByteBufferTest, ResizeInBounds) {
  std::array<uint8_t, 8> memory{};
  ByteBuffer buffer(memory.data(), 8);

  buffer.Resize(0);
  EXPECT_EQ(buffer.Data(), memory.data());
  EXPECT_EQ(buffer.Size(), 0);

  buffer.Resize(5);
  EXPECT_EQ(buffer.Data(), memory.data());
  EXPECT_EQ(buffer.Size(), 5);
}

TEST(ByteBufferTest, ResizeOutOfBounds) {
  std::array<uint8_t, 8> memory{1, 2, 3, 4, 5, 6, 7, 8};
  ByteBuffer buffer(memory.data(), 8);

  buffer.Resize(0);
  EXPECT_EQ(buffer.Data(), memory.data());
  EXPECT_EQ(buffer.Size(), 0);

  buffer.Resize(8);
  EXPECT_EQ(buffer.Data(), memory.data());
  EXPECT_EQ(buffer.Size(), 8);

  buffer.Resize(10);
  EXPECT_NE(buffer.Data(), memory.data());
  EXPECT_EQ(buffer.Size(), 10);
  EXPECT_EQ(buffer.Data()[0], 1);
  EXPECT_EQ(buffer.Data()[7], 8);
}

TEST(ByteBufferTest, UpAndDownResizing) {
  std::array<uint8_t, 8> memory{1, 2, 3, 4, 5, 6, 7, 8};
  ByteBuffer buffer(memory.data(), 8);
  buffer.Resize(10);
  buffer.Data()[0] = 10;
  buffer.Data()[1] = 11;
  buffer.Resize(1);
  EXPECT_EQ(buffer.Data(), memory.data());
  EXPECT_EQ(memory[0], 10);
  EXPECT_EQ(memory[1], 2);
}

TEST(ByteBufferTest, OnlyInternalStorage) {
  ByteBuffer buffer(8);
  EXPECT_EQ(buffer.Size(), 0);
  buffer.Resize(10);
  EXPECT_EQ(buffer.Size(), 10);
}

}  // namespace skyrise
