#pragma once

#include "abstract_storage.hpp"

#if defined(__linux__)
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#else
#error "FilesystemStorage is not implemented on your platform."
#endif

#include <fstream>
#include <iostream>
#include <vector>

#include "utils/literal.hpp"

namespace skyrise {
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

class FilesystemReader : public ObjectReader {
 public:
  static constexpr size_t kReadBufferSize = 16_KB;

  explicit FilesystemReader(const std::string& filename);
  FilesystemReader(const FilesystemReader&) = delete;
  ~FilesystemReader() override;
  StorageError Read(size_t first_byte, size_t last_byte,
                    std::function<void(const char* data, size_t length)> callback) override;
  StorageError Close() override;

 private:
  std::ifstream in_;
  StorageError error_;
  std::vector<char> buffer_;
};

class FilesystemStorage : public Storage {
 public:
  FilesystemStorage() : root_directory_("./") {}
  FilesystemStorage(std::string root_directory) : root_directory_(std::move(root_directory)) {}

  // TODO(Jan.Siebert): Implement support for '/' in filenames
  std::unique_ptr<ObjectWriter> OpenForWriting(const std::string& object_identifier) override;
  std::unique_ptr<ObjectReader> OpenForReading(const std::string& object_identifier) override;
  ObjectStatus GetStatus(const std::string& object_identifier) override;
  StorageError Delete(const std::string& object_identifier) override;
  std::pair<std::vector<ObjectStatus>, StorageError> List(const std::string& object_prefix) override;

 private:
  ObjectStatus StatFile(const std::string& filename);
  StorageError ListDirectoryRecursively(const std::string& directory_name, const std::string& prefix,
                                        std::vector<ObjectStatus>* output_vector);

  // JoinPath joins two path components and ensures that there is exactly a single '/' between them.
  static std::string JoinPath(const std::string& part_a, const std::string& part_b);
  std::string root_directory_;
};

}  // namespace skyrise
