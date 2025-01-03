/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "table.hpp"

#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace skyrise {

std::shared_ptr<Table> Table::CreateDummyTable(const TableColumnDefinitions& column_definitions) {
  return std::make_shared<Table>(column_definitions);
}

Table::Table(const TableColumnDefinitions& column_definitions) : column_definitions_(column_definitions) {}

Table::Table(const TableColumnDefinitions& column_definitions, std::vector<std::shared_ptr<Chunk>>&& chunks)
    : Table(column_definitions) {
  chunks_ = {chunks.begin(), chunks.end()};

  if constexpr (SKYRISE_DEBUG) {
    const size_t chunk_count = chunks_.size();
    for (ChunkId chunk_id = 0; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = GetChunk(chunk_id);
      if (!chunk) {
        continue;
      }

      Assert(chunk->GetColumnCount() == GetColumnCount(), "Invalid Chunk column count.");

      for (ColumnId column_id = 0; column_id < GetColumnCount(); ++column_id) {
        Assert(chunk->GetSegment(column_id)->GetDataType() == ColumnDataType(column_id), "Invalid Segment DataType.");
      }
    }
  }
}

const TableColumnDefinitions& Table::ColumnDefinitions() const { return column_definitions_; }

ColumnCount Table::GetColumnCount() const {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
  return static_cast<ColumnCount>(column_definitions_.size());
}

const std::string& Table::ColumnName(const ColumnId column_id) const {
  DebugAssert(column_id < column_definitions_.size(), "ColumnId is out of range.");

  return column_definitions_[column_id].name;
}

std::vector<std::string> Table::ColumnNames() const {
  std::vector<std::string> names;
  names.reserve(column_definitions_.size());

  for (const auto& column_definition : column_definitions_) {
    names.emplace_back(column_definition.name);
  }

  return names;
}

DataType Table::ColumnDataType(const ColumnId column_id) const {
  DebugAssert(column_id < column_definitions_.size(), "ColumnId is out of range.");

  return column_definitions_[column_id].data_type;
}

std::vector<DataType> Table::ColumnDataTypes() const {
  std::vector<DataType> types;
  types.reserve(column_definitions_.size());

  for (const auto& column_definition : column_definitions_) {
    types.emplace_back(column_definition.data_type);
  }

  return types;
}

bool Table::ColumnIsNullable(const ColumnId column_id) const {
  DebugAssert(column_id < column_definitions_.size(), "ColumnId is out of range.");

  return column_definitions_[column_id].nullable;
}

std::vector<bool> Table::ColumnsAreNullable() const {
  std::vector<bool> nullable(GetColumnCount());

  for (ColumnId column_id = 0; column_id < GetColumnCount(); ++column_id) {
    nullable[column_id] = column_definitions_[column_id].nullable;
  }

  return nullable;
}

ColumnId Table::ColumnIdByName(const std::string& column_name) const {
  const auto iter = std::find_if(column_definitions_.begin(), column_definitions_.end(),
                                 [&](const auto& column_definition) { return column_definition.name == column_name; });
  Assert(iter != column_definitions_.end(), "Could not find column with name '" + column_name + "'.");

  return static_cast<ColumnId>(std::distance(column_definitions_.begin(), iter));
}

size_t Table::RowCount() const {
  // TODO(tobodner): if with SKYRISE_DEBUG?
  if (cached_row_count_ && !SKYRISE_DEBUG) {
    return *cached_row_count_;
  }

  size_t row_count = 0;
  const size_t chunk_count = chunks_.size();
  for (ChunkId chunk_id = 0; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = GetChunk(chunk_id);
    if (chunk) {
      row_count += chunk->Size();
    }
  }

  // After being created, tables should never be changed again.
  DebugAssert(!cached_row_count_ || row_count == *cached_row_count_, "Size of reference table has changed.");

  // RowCount() is called by AbstractOperator after the operator has finished to fill the performance data. As such,
  // no synchronization is necessary.
  cached_row_count_ = row_count;

  return row_count;
}

bool Table::Empty() const { return RowCount() == 0ULL; }

ChunkId Table::ChunkCount() const { return static_cast<ChunkId>(chunks_.size()); }

std::shared_ptr<Chunk> Table::GetChunk(ChunkId chunk_id) {
  DebugAssert(chunk_id < chunks_.size(), "ChunkId " + std::to_string(chunk_id) + " is out of range.");
  return std::atomic_load(&chunks_[chunk_id]);
}

std::shared_ptr<const Chunk> Table::GetChunk(ChunkId chunk_id) const {
  DebugAssert(chunk_id < chunks_.size(), "ChunkId " + std::to_string(chunk_id) + " is out of range.");
  return std::atomic_load(&chunks_[chunk_id]);
}

std::shared_ptr<Chunk> Table::LastChunk() const {
  DebugAssert(!chunks_.empty(), "LastChunk() called on Table without chunks.");
  return std::atomic_load(&chunks_.back());
}

void Table::AppendChunk(const Segments& segments) {
  AssertInput(static_cast<ColumnCount>(segments.size()) == GetColumnCount(),
              "Input does not have the same number of columns.");

  const std::lock_guard<std::mutex> lock(chunks_mutex_);
  chunks_.push_back(std::make_shared<Chunk>(segments));
}

size_t Table::MemoryUsageBytes() const {
  size_t bytes = sizeof(*this);

  const size_t chunk_count = chunks_.size();
  for (ChunkId chunk_id = 0; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = GetChunk(chunk_id);
    if (!chunk) {
      continue;
    }

    bytes += chunk->MemoryUsageBytes();
  }

  for (const auto& column_definition : column_definitions_) {
    bytes += column_definition.name.size();
  }

  return bytes;
}

}  // namespace skyrise
