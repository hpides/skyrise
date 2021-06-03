#include "manifest_merger.hpp"

namespace skyrise {

ManifestMerger::ManifestMerger(std::shared_ptr<Storage> storage)
    : storage_(std::move(storage)), error_(StorageError::Success()) {}

bool ManifestMerger::Merge(const std::vector<std::string>& objects, const std::string& target_file_name) {
  ManifestWriter manifest_writer(storage_->OpenForWriting(target_file_name));

  TableColumnDefinitions base_schema;

  for (const auto& object : objects) {
    ManifestReader reader(storage_->OpenForReading(object));
    const auto original_schema = reader.GetOriginalSchema();

    if (base_schema.empty()) {
      base_schema = *original_schema;
    } else if (base_schema != *original_schema) {
      // Schemas of passed manifest objects do not match
      manifest_writer.Close();
      storage_->Delete(target_file_name);
      return false;
    }

    while (reader.HasNextPartition()) {
      manifest_writer.WritePartition(reader.ReadNextPartition());
    }
  }

  if (!manifest_writer.Close()) {
    return false;
  }

  for (const auto& object : objects) {
    storage_->Delete(object);
  }

  return true;
}

StorageError ManifestMerger::GetError() { return error_; }

}  // namespace skyrise
