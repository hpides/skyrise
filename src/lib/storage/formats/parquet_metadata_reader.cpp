#include "parquet_metadata_reader.hpp"

#include <cstdint>
#include <optional>
#include <string>

#include "configuration.hpp"
#include "constants.hpp"
#include "utils/assert.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

ParquetFormatMetadataReader::ParquetFormatMetadataReader(const std::shared_ptr<ObjectBuffer>& source,
                                                         const size_t object_size, const Configuration& configuration,
                                                         const size_t footer_length)
    : ParquetFormatReader(source, footer_length, configuration), object_size_(object_size) {
  auto input_stream = std::make_shared<ParquetInputProxy>(source, footer_length);
  try {
    auto file_source = arrow::dataset::FileSource(input_stream);
    auto parquet_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    parquet_fragment_ = std::static_pointer_cast<arrow::dataset::ParquetFileFragment>(
        parquet_format->MakeFragment(file_source).ValueOrDie());

    // Consider only specific partitions if row group ids are provided.
    if (configuration_.row_group_ids.has_value()) {
      auto row_group_subset = parquet_fragment_->Subset(configuration_.row_group_ids.value());
      Assert(row_group_subset.ok(), row_group_subset.status().ToString());
      parquet_fragment_ = std::static_pointer_cast<arrow::dataset::ParquetFileFragment>(row_group_subset.ValueOrDie());
    }
  } catch (const std::logic_error& error) {
    SetError(StorageError(StorageErrorType::kIOError, error.what()));
  } catch (const parquet::ParquetInvalidOrCorruptedFileException& error) {
    SetError(StorageError(StorageErrorType::kIOError, error.what()));
  }
}

std::vector<std::pair<size_t, size_t>> ParquetFormatMetadataReader::CalculatePageOffsets(
    const std::shared_ptr<ObjectReader>& object_reader, const size_t object_size, const Configuration& configuration,
    const size_t footer_length) {
  uint32_t footer_request_size = std::min(object_size, static_cast<size_t>(footer_length));
  Assert(footer_request_size >= 8, "Footer request size needs to be at least 8 bytes.");
  auto footer_buffer = std::make_shared<ByteBuffer>(footer_request_size);
  object_reader->ReadTail(footer_request_size, footer_buffer.get());

  auto object_buffer = std::make_shared<ObjectBuffer>();
  object_buffer->AddBuffer({{0, footer_request_size}, footer_buffer});

  // The file metadata size is 4 bytes at position -8 to -4 of the footer tail.
  const uint32_t footer_size = *reinterpret_cast<uint32_t*>(
      object_buffer->Read(footer_request_size - kParquetFooterMetadataOffsetBytes, kParquetFooterMetadataSizeBytes)
          ->Data());

  if (footer_size > footer_request_size - kParquetFooterMetadataOffsetBytes) {
    // Reread footer  with adapted file metadata size.
    footer_request_size = footer_size + kParquetFooterMetadataOffsetBytes;
    footer_buffer = std::make_shared<ByteBuffer>(footer_request_size);
    object_reader->ReadTail(footer_request_size, footer_buffer.get());

    object_buffer = std::make_shared<ObjectBuffer>();
    object_buffer->AddBuffer({{0, footer_request_size}, footer_buffer});
  }

  return std::make_shared<ParquetFormatMetadataReader>(object_buffer, object_size, configuration, footer_request_size)
      ->CalculatePageOffsetsForColumnIds();
}

std::vector<std::pair<size_t, size_t>> ParquetFormatMetadataReader::CalculatePageOffsetsForColumnIds() {
  // Calculate required byte ranges for projection pushdown.
  Assert(parquet_fragment_->EnsureCompleteMetadata().ok(), "Unable to extract metadata.");
  std::vector<std::pair<size_t, size_t>> ranges;

  if (configuration_.include_columns.has_value()) {
    if (static_cast<size_t>(parquet_fragment_->metadata()->num_columns()) ==
        configuration_.include_columns.value().size()) {
      // TODO(anyone): This is a temporary fix for the ParquetFormatMetadataReader, which currently miscalculates the
      // page offsets, if all columns that exist in a Parquet file are loaded. This is mainly the case for
      // intermediate results that are > 16MB in size. The fix is to load the entire object, i.e., skipping the
      // projection pushdown on S3 level.
      const int64_t padding = MiBToByte(1);
      for (const auto& row_group : parquet_fragment_->row_groups()) {
        auto partition_metadata = parquet_fragment_->metadata()->RowGroup(row_group);
        int64_t start_offset = partition_metadata->file_offset();
        Assert(start_offset > 0, "Invalid start offset.");
        int64_t compressed_size = partition_metadata->total_compressed_size();
        int64_t end_offset = std::min<int64_t>(
            start_offset + padding + (compressed_size == 0 ? partition_metadata->total_byte_size() : compressed_size),
            object_size_);
        Assert(end_offset - start_offset < kS3ReadRequestSizeBytes,
               "Large request should be split into multiple smaller.");
        ranges.emplace_back(start_offset, end_offset);
      }
      return ranges;
    }

    for (const auto& row_group : parquet_fragment_->row_groups()) {
      for (int column : configuration_.include_columns.value()) {
        ComputeByteRange(parquet_fragment_->metadata().get(), object_size_, row_group, column, ranges);
      }
    }
  }
  return ranges;
}

void ParquetFormatMetadataReader::ComputeByteRange(parquet::FileMetaData* file_metadata, int64_t source_size,
                                                   int row_group_index, int column_index,
                                                   std::vector<std::pair<size_t, size_t>>& ranges) {
  // Adapted from https://github.com/apache/arrow/blob/master/cpp/src/parquet/file_reader.cc
  auto row_group_metadata = file_metadata->RowGroup(row_group_index);
  auto column_metadata = row_group_metadata->ColumnChunk(column_index);

  int64_t column_start = column_metadata->data_page_offset();
  if (column_metadata->has_dictionary_page() && column_metadata->dictionary_page_offset() > 0 &&
      column_start > column_metadata->dictionary_page_offset()) {
    column_start = column_metadata->dictionary_page_offset();
  }

  int64_t column_length = column_metadata->total_compressed_size();
  int64_t column_end = 0;
  if (column_start < 0 || column_length < 0) {
    throw std::logic_error("Invalid column metadata (corrupt file?)");
  }
  if (column_start + column_length > std::numeric_limits<int64_t>::max() || column_end > source_size) {
    throw std::logic_error("Invalid column metadata (corrupt file?)");
  }

  column_end = column_start + column_length;

  // PARQUET-816 workaround for old files created by older parquet-mr
  const parquet::ApplicationVersion& version = file_metadata->writer_version();
  if (version.VersionLt(parquet::ApplicationVersion::PARQUET_816_FIXED_VERSION())) {
    int64_t bytes_remaining = source_size - column_end;
    int64_t padding = std::min<int64_t>(kMaxDictionaryHeaderSize, bytes_remaining);
    column_length += padding;
  }

  // TODO(tobodner): Consider more edge cases here when building the byte ranges.
  size_t start = column_start;
  const size_t end = column_start + column_length;

  if (ranges.empty()) {
    ranges.emplace_back(start, end);
    return;
  }
  if (start - ranges.back().second > kReadRequestPaddingSizeBytes ||
      ranges.back().second - ranges.back().first >= kS3ReadRequestSizeBytes) {
    ranges.emplace_back(start, end);
  } else {
    start = ranges.back().first;
    ranges.back() = {start, end};
  }
}

}  // namespace skyrise
