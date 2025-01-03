/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "chunk.hpp"
#include "table_column_definition.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * A Table is partitioned horizontally into a number of chunks.
 */
class Table : private Noncopyable {
 public:
  static std::shared_ptr<Table> CreateDummyTable(const TableColumnDefinitions& column_definitions);

  explicit Table(const TableColumnDefinitions& column_definitions);

  Table(const TableColumnDefinitions& column_definitions, std::vector<std::shared_ptr<Chunk>>&& chunks);

  /**
   * @return The column definitions for the table.
   */
  const TableColumnDefinitions& ColumnDefinitions() const;

  ColumnCount GetColumnCount() const;

  const std::string& ColumnName(const ColumnId column_id) const;
  std::vector<std::string> ColumnNames() const;

  DataType ColumnDataType(const ColumnId column_id) const;
  std::vector<DataType> ColumnDataTypes() const;

  bool ColumnIsNullable(const ColumnId column_id) const;
  std::vector<bool> ColumnsAreNullable() const;

  /**
   * @return The ColumnId for the given name.
   * Fails, if there is no column of that name.
   */
  ColumnId ColumnIdByName(const std::string& column_name) const;

  /**
   * @return The number of rows.
   */
  size_t RowCount() const;

  /**
   * Shorthand checking for RowCount() == 0.
   */
  bool Empty() const;

  /**
   * @return The number of chunks, or, more correctly, the ChunkId of the last chunk plus one.
   * This cannot exceed ChunkId (uint32_t).
   */
  ChunkId ChunkCount() const;

  /**
   * @return The chunk with the given ChunkId.
   */
  std::shared_ptr<Chunk> GetChunk(ChunkId chunk_id);
  std::shared_ptr<const Chunk> GetChunk(ChunkId chunk_id) const;

  /**
   * @return The last chunk from chunks_.
   */
  std::shared_ptr<Chunk> LastChunk() const;

  /**
   * Creates a new Chunk from a set of segments and appends it to this table.
   * When implementing operators, prefer building the Chunks upfront and adding them to the output table on
   * construction of the Table. This avoids having to append repeatedly to the vector storing the Chunks.
   */
  void AppendChunk(const Segments& segments);

  /**
   * For debugging purposes, makes an estimation about the memory used by this Table (including Chunk and Segments).
   */
  size_t MemoryUsageBytes() const;

 protected:
  const TableColumnDefinitions column_definitions_;

  std::vector<std::shared_ptr<Chunk>> chunks_;

  /**
   * For tables with type_ == kReference, the row count will not vary. As such, there is no need to iterate over all
   * chunks more than once.
   */
  mutable std::optional<uint64_t> cached_row_count_;

  std::mutex chunks_mutex_;
};

}  // namespace skyrise
