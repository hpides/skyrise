#include "alias_operator.hpp"

using namespace std::string_literals;  // NOLINT

namespace {

const std::string kName = "Alias";

}  // namespace

namespace skyrise {

AliasOperator::AliasOperator(std::shared_ptr<const AbstractOperator> input_operator,
                             const std::vector<ColumnId>& column_ids, const std::vector<std::string>& aliases)
    : AbstractOperator(OperatorType::kAlias, std::move(input_operator)), column_ids_(column_ids), aliases_(aliases) {
  Assert(column_ids_.size() == aliases_.size(), "Expected as many aliases as columns.");
}

const std::string& AliasOperator::Name() const { return kName; }

std::shared_ptr<const Table> AliasOperator::OnExecute(const std::shared_ptr<OperatorExecutionContext>& /*context*/) {
  const std::shared_ptr<const Table>& input_table = LeftInputTable();
  Assert(input_table->GetColumnCount() == column_ids_.size(), "Expected as many column ids as the input table size.");

  // (1) Generate the new TableColumnDefinitions with the new names for the columns.
  std::vector<TableColumnDefinition> output_column_definitions;
  output_column_definitions.reserve(input_table->GetColumnCount());

  for (size_t i = 0; i < input_table->GetColumnCount(); ++i) {
    const auto& input_column_definition = input_table->ColumnDefinitions()[column_ids_[i]];

    output_column_definitions.emplace_back(aliases_[i], input_column_definition.data_type,
                                           input_column_definition.nullable);
  }

  // (2) Generate the output table, forwarding segments from the input chunks and ordering them according to
  // column_ids_.
  std::vector<std::shared_ptr<Chunk>> output_chunks(input_table->ChunkCount());

  for (ChunkId chunk_id = 0; chunk_id < input_table->ChunkCount(); ++chunk_id) {
    Segments output_segments;
    output_segments.reserve(input_table->GetColumnCount());

    for (const auto& column_id : column_ids_) {
      output_segments.push_back(input_table->GetChunk(chunk_id)->GetSegment(column_id));
    }

    output_chunks[chunk_id] = std::make_shared<Chunk>(std::move(output_segments));
  }

  return std::make_shared<Table>(output_column_definitions, std::move(output_chunks));
}

}  // namespace skyrise
