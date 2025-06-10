#pragma once

#include "abstract_provider.hpp"
#include "storage/backend/filesystem_storage.hpp"

namespace skyrise {

class FilesystemStorageProvider : public StorageProvider {
 public:
  Storage& GetStorage() override { return *storage_; }

  void SetUp() override { storage_ = std::make_unique<FilesystemStorage>("/tmp/"); }
  void TearDown() override { storage_.reset(nullptr); }

 private:
  std::unique_ptr<FilesystemStorage> storage_;
};

}  // namespace skyrise
