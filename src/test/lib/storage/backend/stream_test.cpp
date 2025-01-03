#include "storage/backend/stream.hpp"

#include <gtest/gtest.h>

#include "mock_storage.hpp"

namespace skyrise {

class StreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::vector<char> data;

    // Make data large enough to trigger underflow two times. So we have abbb...bccc...cd
    data.reserve(kTestObjectLargeSize);
    data.push_back('a');
    data.insert(data.end(), kTestObjectLargeBlockSize, 'b');
    data.insert(data.end(), kTestObjectLargeBlockSize, 'c');
    data.push_back('d');

    auto writer = storage_.OpenForWriting(kTestObjectNameLarge);
    writer->Write(data.data(), data.size());
    writer->Close();

    writer = storage_.OpenForWriting(kTestObjectNameSmall);
    writer->Write("abcd", kTestObjectSmallSize);
    writer->Close();
  }

  ObjectReaderStream GetStreamLarge() { return ObjectReaderStream(storage_.OpenForReading(kTestObjectNameLarge)); }

  std::unique_ptr<MockReader> GetMockReaderSmall() {
    return std::unique_ptr<MockReader>(
        dynamic_cast<MockReader*>(storage_.OpenForReading(kTestObjectNameSmall).release()));
  }

  ObjectReaderStream GetStreamWithWrongName() {
    return ObjectReaderStream(storage_.OpenForReading("objectNonExistent"));
  }

  static constexpr auto kTestObjectNameLarge = "objectLarge";
  static constexpr auto kTestObjectNameSmall = "objectSmall";
  static constexpr auto kTestObjectLargeBlockSize = 20_MB;
  static constexpr auto kTestObjectLargeSize = 2 * kTestObjectLargeBlockSize + 2;
  static constexpr auto kTestObjectSmallSize = 4;
  MockStorage storage_;
};

TEST_F(StreamTest, TestSmallSequentialReads) {
  auto reader = GetMockReaderSmall();
  // Keep a reference to the counter to see the updated value when we have passed MockReader to ObjectReaderStream.
  const size_t& read_counter = reader->GetReadOperationCounter();
  ObjectReaderStream stream(std::move(reader));
  EXPECT_EQ(stream.get(), 'a');
  EXPECT_EQ(stream.get(), 'b');
  EXPECT_EQ(stream.get(), 'c');
  EXPECT_EQ(stream.get(), 'd');
  EXPECT_FALSE(stream.eof());
  EXPECT_EQ(stream.get(), std::char_traits<char>::eof());
  EXPECT_TRUE(stream.eof());

  // All reads only yielded in one single read operation.
  EXPECT_EQ(read_counter, 1);
}

TEST_F(StreamTest, TestSequentialReads) {
  auto stream = GetStreamLarge();
  EXPECT_EQ(stream.get(), 'a');
  for (size_t i = 0; i < kTestObjectLargeBlockSize; ++i) {
    EXPECT_EQ(stream.get(), 'b');
  }
  for (size_t i = 0; i < kTestObjectLargeBlockSize; ++i) {
    EXPECT_EQ(stream.get(), 'c');
  }
  EXPECT_EQ(stream.get(), 'd');

  EXPECT_FALSE(stream.eof());
  EXPECT_EQ(stream.get(), std::char_traits<char>::eof());
  EXPECT_TRUE(stream.eof());
}

TEST_F(StreamTest, TestAbsoluteSeeking) {
  auto stream = GetStreamLarge();
  stream.seekg(1);
  EXPECT_EQ(stream.get(), 'b');
  stream.seekg(0);
  EXPECT_EQ(stream.get(), 'a');
  stream.seekg(kTestObjectLargeSize - 1);
  EXPECT_EQ(stream.get(), 'd');
  stream.seekg(kTestObjectLargeBlockSize);
  EXPECT_EQ(stream.get(), 'b');
  EXPECT_EQ(stream.get(), 'c');
}

TEST_F(StreamTest, TestRelativeSeeking) {
  auto stream = GetStreamLarge();
  EXPECT_EQ(stream.get(), 'a');

  // Read first character again.
  stream.seekg(-1, std::ios_base::cur);
  EXPECT_EQ(stream.get(), 'a');

  // Read last two characters.
  stream.seekg(-2, std::ios_base::end);
  EXPECT_EQ(stream.get(), 'c');
  EXPECT_EQ(stream.get(), 'd');

  // Read second character.
  stream.seekg(1, std::ios_base::beg);
  EXPECT_EQ(stream.get(), 'b');
}

TEST_F(StreamTest, TestErrorHandling) {
  auto stream = GetStreamWithWrongName();
  EXPECT_TRUE(stream.good());
  EXPECT_EQ(stream.get(), std::char_traits<char>::eof());
  EXPECT_FALSE(stream.good());
}

TEST_F(StreamTest, TestGetFilesize) {
  // The AWS SDK uses this trick to determine the size of a payload.
  auto stream = GetStreamLarge();
  stream.seekg(0, std::ios_base::end);
  auto filesize = static_cast<size_t>(stream.tellg());
  EXPECT_EQ(filesize, kTestObjectLargeSize);
}

TEST(DelegateStreamTest, SimpleWriteRead) {
  ByteBuffer stream_buffer(3);
  DelegateStreamBuffer buffer_stream(&stream_buffer);
  std::iostream test_stream(&buffer_stream);

  test_stream << "AB";
  test_stream.put('C');

  EXPECT_TRUE(test_stream.good());
  EXPECT_TRUE(memcmp(stream_buffer.CharData(), "ABC", 3) == 0);

  EXPECT_EQ(test_stream.get(), 'A');
  EXPECT_EQ(test_stream.get(), 'B');
  EXPECT_EQ(test_stream.get(), 'C');
  EXPECT_EQ(test_stream.get(), std::char_traits<char>::eof());
}

TEST(DelegateStreamTest, RandomAccessReadWrite) {
  ByteBuffer stream_buffer;
  DelegateStreamBuffer buffer_stream(&stream_buffer);
  std::iostream test_stream(&buffer_stream);

  test_stream << "ABCDEFG";
  EXPECT_EQ(std::string(stream_buffer.CharData(), stream_buffer.Size()), "ABCDEFG");

  test_stream.seekg(1);
  EXPECT_EQ(test_stream.get(), 'B');

  test_stream.seekp(1);
  test_stream << "X";
  EXPECT_EQ(std::string(stream_buffer.CharData(), stream_buffer.Size()), "AXCDEFG");
  EXPECT_EQ(stream_buffer.Size(), 7);

  test_stream.seekg(1);
  EXPECT_EQ(test_stream.get(), 'X');
  EXPECT_EQ(test_stream.get(), 'C');

  test_stream.seekp(6);
  test_stream << "P";
  EXPECT_EQ(std::string(stream_buffer.CharData(), stream_buffer.Size()), "AXCDEFP");

  test_stream.seekp(6);
  test_stream << "AB";
  EXPECT_EQ(std::string(stream_buffer.CharData(), stream_buffer.Size()), "AXCDEFAB");
}

TEST(DelegateStreamTest, RelativeSeeking) {
  ByteBuffer stream_buffer;
  DelegateStreamBuffer buffer_stream(&stream_buffer);
  std::iostream test_stream(&buffer_stream);

  std::stringstream golden_stream;
  test_stream << "ABC";
  golden_stream << "ABC";

  test_stream.seekp(-1, std::ios::cur);
  golden_stream.seekp(-1, std::ios::cur);

  test_stream << "X";
  golden_stream << "X";
  EXPECT_EQ(golden_stream.str(), "ABX");
  EXPECT_EQ(golden_stream.str(), std::string(stream_buffer.CharData(), stream_buffer.Size()));
}

}  // namespace skyrise
