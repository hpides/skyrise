#pragma once

#include <functional>
#include <queue>

#include "storage/backend/abstract_storage.hpp"
#include "storage/formats/abstract_chunk_reader.hpp"

namespace skyrise {

class AbstractChunkReaderFactory {
 public:
  virtual ~AbstractChunkReaderFactory() = default;
  virtual std::unique_ptr<AbstractChunkReader> Get(std::unique_ptr<ObjectReader> source) = 0;
};

template <typename Formatter>
class FormatReaderFactory : public AbstractChunkReaderFactory {
 public:
  explicit FormatReaderFactory(
      const typename Formatter::Configuration configuration = typename Formatter::Configuration())
      : configuration_(std::move(configuration)) {}

  std::unique_ptr<AbstractChunkReader> Get(std::unique_ptr<ObjectReader> source) override {
    return std::make_unique<Formatter>(std::move(source), configuration_);
  }

 private:
  typename Formatter::Configuration configuration_;
};

/**
 * ChunkReader successively loads chunks from multiple objects. It provides an iterator-like interface which returns
 * chunks from a pool of objects in no specific order. Depending on the objects containing the chunks the size between
 * chunks may differ. If an error occurs with any of the objects, no further data is read.
 */
class ChunkReader : public AbstractChunkReader {
 public:
  using LazyReaderConstructor = std::function<std::unique_ptr<AbstractChunkReader>()>;
  ChunkReader() = default;

  void AddObjects(const std::shared_ptr<AbstractChunkReaderFactory>& factory, const std::shared_ptr<Storage>& storage,
                  const std::vector<std::string>& object_list);

  bool HasNext() override;
  std::unique_ptr<Chunk> Next() override;

 private:
  std::unique_ptr<AbstractChunkReader> InitializeNextReader();

  std::unique_ptr<AbstractChunkReader> active_reader_;
  std::queue<LazyReaderConstructor> uninitialized_readers_;
};

}  // namespace skyrise
