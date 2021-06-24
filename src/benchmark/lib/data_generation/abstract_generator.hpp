#pragma once

#include "storage/backend/abstract_storage.hpp"
#include "storage/formats/abstract_chunk_writer.hpp"
#include "storage/table/chunk_writer.hpp"
#include "table_builder.hpp"

namespace skyrise {

class AbstractDataGenerator {
 public:
  explicit AbstractDataGenerator(PartitionedChunkWriterFactory&& chunk_writer_factory)
      : chunk_writer_factory_(std::move(chunk_writer_factory)) {}
  virtual ~AbstractDataGenerator() = default;

  virtual void Generate() = 0;

 protected:
  const PartitionedChunkWriterFactory& GetPartitionedChunkWriterFactory() { return chunk_writer_factory_; }

 private:
  PartitionedChunkWriterFactory chunk_writer_factory_;
};

}  // namespace skyrise
