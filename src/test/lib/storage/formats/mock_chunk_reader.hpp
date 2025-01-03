#pragma once

#include "storage/formats/abstract_chunk_reader.hpp"

namespace skyrise {

class ValueSegmentGenerator {
 public:
  using LambdaGenerate = std::function<std::shared_ptr<AbstractSegment>()>;

  explicit ValueSegmentGenerator(LambdaGenerate generate) : generate_(std::move(generate)){};

  std::shared_ptr<AbstractSegment> Generate() const { return generate_(); }

 private:
  LambdaGenerate generate_;
};

struct MockChunkReaderConfiguration {
  /**
   * Marks whether the MockChunkReader should run into an error state.
   */
  bool error = false;

  /**
   * One generator for each column of a chunk. Each Generator is called exactly once.
   */
  std::vector<ValueSegmentGenerator> generators;

  /**
   * Number of chunks to be returned.
   */
  size_t num_chunks = 3;

  /**
   * Number of chunks until the MockChunkReader runs into an error state.
   * This configuration setting is only valid in combination with the error flag.
   */
  size_t num_chunks_until_error = 0;

  std::shared_ptr<const TableColumnDefinitions> schema = std::make_shared<const TableColumnDefinitions>();
};

/**
 * MockChunkReader returns a variable number of given Chunks and can be configured to fail at some point. While all
 * chunks will be different pointers the contained Segments are shared.
 */
class MockChunkReader : public AbstractChunkReader {
 public:
  using Configuration = MockChunkReaderConfiguration;

  // The source parameter gets discarded and is only present to have a compatible interface.
  explicit MockChunkReader(const std::shared_ptr<ObjectBuffer>& source, const size_t object_size,
                           const Configuration& configuration = Configuration());
  bool HasNext() override;
  std::unique_ptr<Chunk> Next() override;

 private:
  void GenerateSegments();

  size_t available_chunks_;
  Segments segments_;
  Configuration config_;
};

}  // namespace skyrise
