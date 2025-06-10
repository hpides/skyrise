#pragma once

#include <fstream>
#include <iostream>
#include <vector>

#include "abstract_storage.hpp"
#include "utils/literal.hpp"

namespace skyrise {
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class FilesystemWriter : public ObjectWriter {
 public:
  explicit FilesystemWriter(const std::string& filename);
  FilesystemWriter(const FilesystemWriter&) = delete;  // No copy
  ~FilesystemWriter() override;
  StorageError Write(const char* data, size_t length) override;
  StorageError Close() override;

 private:
  std::ofstream out_;
};

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class FilesystemReader : public ObjectReader {
 public:
  explicit FilesystemReader(const std::string& filename, size_t num_characters_hidden = 0);
  FilesystemReader(const FilesystemReader&) = delete;
  ~FilesystemReader() override;
  StorageError Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) override;
  const ObjectStatus& GetStatus() override;
  StorageError Close() override;

 private:
  std::ifstream in_;
  StorageError error_;
  std::string filename_;
  size_t num_characters_hidden_;
};

class FilesystemStorage : public Storage {
 public:
  FilesystemStorage() : root_directory_("./") {}
  explicit FilesystemStorage(std::string root_directory) : root_directory_(std::move(root_directory)) {}

  std::unique_ptr<ObjectWriter> OpenForWriting(const std::string& object_identifier) override;
  std::unique_ptr<ObjectReader> OpenForReading(const std::string& object_identifier) override;
  StorageError Delete(const std::string& object_identifier) override;
  std::pair<std::vector<ObjectStatus>, StorageError> List(const std::string& object_prefix) override;

 private:
  StorageError ListDirectoryRecursively(const std::string& directory_name, const std::string& prefix,
                                        std::vector<ObjectStatus>* output_vector);

  // JoinPath joins two path components and ensures that there is exactly a single '/' between them.
  static std::string JoinPath(const std::string& part_a, const std::string& part_b);
  std::string root_directory_;
};

}  // namespace skyrise
