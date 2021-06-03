#pragma once

#include "statistics/manifest_reader.hpp"
#include "statistics/manifest_writer.hpp"
#include "storage/backend/abstract_storage.hpp"
namespace skyrise {
class ManifestMerger {
 public:
  ManifestMerger(std::shared_ptr<Storage> storage);

  bool Merge(const std::vector<std::string>& objects, const std::string& target_file_name);
  StorageError GetError();

 private:
  std::shared_ptr<Storage> storage_;

  StorageError error_;
};

}  // namespace skyrise
