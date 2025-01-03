#include "storage/io_handle/object_buffer.hpp"

#include <gtest/gtest.h>

namespace skyrise {

class ObjectBufferTest : public ::testing::Test {
 protected:
  void SetUp() override {
    data_x_ = std::vector<char>(size_, 'x');
    data_y_ = std::vector<char>(size_, 'y');
    data_z_ = std::vector<char>(size_, 'z');

    first_buffer_ = std::make_shared<ByteBuffer>(size_);
    second_buffer_ = std::make_shared<ByteBuffer>(size_);
    third_buffer_ = std::make_shared<ByteBuffer>(size_);

    first_buffer_->Resize(size_);
    second_buffer_->Resize(size_);
    third_buffer_->Resize(size_);

    std::copy_n(data_x_.data(), size_, first_buffer_->Data());
    std::copy_n(data_y_.data(), size_, second_buffer_->Data());
    std::copy_n(data_z_.data(), size_, third_buffer_->Data());
  }

  const size_t size_ = 100;
  std::shared_ptr<ByteBuffer> first_buffer_;
  std::shared_ptr<ByteBuffer> second_buffer_;
  std::shared_ptr<ByteBuffer> third_buffer_;
  std::vector<char> data_x_;
  std::vector<char> data_y_;
  std::vector<char> data_z_;
};

TEST_F(ObjectBufferTest, TestReadFromSingleBuffer) {
  const std::map<Range, std::shared_ptr<ByteBuffer>> request_buffer = {{{0, size_}, first_buffer_}};
  ObjectBuffer object_buffer(request_buffer);
  auto result = object_buffer.Read(0, size_);

  EXPECT_EQ(strncmp(result->CharData(), data_x_.data(), 100), 0);
  EXPECT_EQ(size_, result->Size());
  EXPECT_EQ(first_buffer_->Size(), data_x_.size());
}

TEST_F(ObjectBufferTest, TestReadAllDataFromMultipleMergedBuffers) {
  const size_t read_offset = 0;
  const size_t n_bytes = 200;

  ObjectBuffer object_buffer({{{0, size_}, first_buffer_}, {{size_, 2 * size_}, second_buffer_}});
  auto result = object_buffer.Read(read_offset, n_bytes);

  EXPECT_EQ(n_bytes, result->Size());
  EXPECT_EQ(strncmp(result->CharData(), data_x_.data(), 100), 0);
  EXPECT_EQ(strncmp(result->CharData() + 100, data_y_.data(), 100), 0);
  EXPECT_EQ(first_buffer_->Size(), data_x_.size());
  EXPECT_EQ(second_buffer_->Size(), data_y_.size());
}

TEST_F(ObjectBufferTest, TestReadDataPartlyFromMultipleMergedBuffers) {
  const size_t read_offset = 0;
  const size_t n_bytes = 250;

  ObjectBuffer object_buffer(
      {{{0, size_}, first_buffer_}, {{size_, 2 * size_}, second_buffer_}, {{size_, 3 * size_}, third_buffer_}});
  auto result = object_buffer.Read(read_offset, n_bytes);

  EXPECT_EQ(n_bytes, result->Size());
  EXPECT_EQ(strncmp(result->CharData(), data_x_.data(), 100), 0);
  EXPECT_EQ(strncmp(result->CharData() + 100, data_y_.data(), 100), 0);
  EXPECT_EQ(strncmp(result->CharData() + 200, data_z_.data(), 50), 0);
  EXPECT_EQ(first_buffer_->Size(), data_x_.size());
  EXPECT_EQ(second_buffer_->Size(), data_y_.size());
  EXPECT_EQ(third_buffer_->Size(), data_z_.size());
}

TEST_F(ObjectBufferTest, TestReadDataPartlyInBetweenFromMultipleMergedBuffers) {
  const size_t read_offset = 175;
  const size_t n_bytes = 50;

  ObjectBuffer object_buffer(
      {{{0, size_}, first_buffer_}, {{size_, 2 * size_}, second_buffer_}, {{2 * size_, 3 * size_}, third_buffer_}});
  auto result = object_buffer.Read(read_offset, n_bytes);

  EXPECT_EQ(n_bytes, result->Size());
  EXPECT_EQ(strncmp(result->CharData(), data_y_.data(), 25), 0);
  EXPECT_EQ(strncmp(result->CharData() + 25, data_z_.data(), 25), 0);
  EXPECT_EQ(first_buffer_->Size(), data_x_.size());
  EXPECT_EQ(second_buffer_->Size(), data_y_.size());
  EXPECT_EQ(third_buffer_->Size(), data_z_.size());
}

TEST_F(ObjectBufferTest, TestOrderOfBuffers) {
  const size_t read_offset = 175;
  const size_t n_bytes = 50;

  ObjectBuffer object_buffer;
  object_buffer.AddBuffer({{2 * size_, 3 * size_}, third_buffer_});
  object_buffer.AddBuffer({{size_, 2 * size_}, second_buffer_});
  object_buffer.AddBuffer({{0, size_}, first_buffer_});

  auto result = object_buffer.Read(read_offset, n_bytes);

  EXPECT_EQ(n_bytes, result->Size());
  EXPECT_EQ(strncmp(result->CharData(), data_y_.data(), 25), 0);
  EXPECT_EQ(strncmp(result->CharData() + 25, data_z_.data(), 25), 0);

  ObjectBuffer object_buffer_2(
      {{{2 * size_, 3 * size_}, third_buffer_}, {{0, size_}, first_buffer_}, {{size_, 2 * size_}, second_buffer_}});

  result = object_buffer.Read(read_offset, n_bytes);

  EXPECT_EQ(n_bytes, result->Size());
  EXPECT_EQ(strncmp(result->CharData(), data_y_.data(), 25), 0);
  EXPECT_EQ(strncmp(result->CharData() + 25, data_z_.data(), 25), 0);
  EXPECT_EQ(first_buffer_->Size(), data_x_.size());
  EXPECT_EQ(second_buffer_->Size(), data_y_.size());
  EXPECT_EQ(third_buffer_->Size(), data_z_.size());
}

TEST_F(ObjectBufferTest, TestReadExceedsBufferSizeFromMultipleMergedBuffers) {
  const size_t read_offset = 75;
  const size_t n_bytes = 126;
  const size_t expected_size = 125;

  ObjectBuffer object_buffer({{{0, size_}, first_buffer_}, {{size_, 2 * size_}, second_buffer_}});

  auto result = object_buffer.Read(read_offset, n_bytes);

  EXPECT_EQ(expected_size, result->Size());
  EXPECT_EQ(strncmp(result->CharData(), data_x_.data(), 25), 0);
  EXPECT_EQ(strncmp(result->CharData() + 25, data_y_.data(), 100), 0);
}

}  // namespace skyrise
