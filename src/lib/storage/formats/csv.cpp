#include "csv.hpp"

namespace skyrise {

CSVFormatter::CSVFormatter(CSVFormatterOptions options) : options_(std::move(options)) {}

void CSVFormatter::Initialize(const TableColumnDefinitions& schema) {
  num_fields_ = schema.size();

  if (options_.include_headers) {
    std::stringstream buffer;

    for (size_t i = 0; i < num_fields_; i++) {
      if (i > 0) {
        buffer << options_.field_separator;
      }
      buffer << schema[i].name;
    }

    buffer << options_.record_separator;

    WriteToOutput(buffer.str().c_str(), buffer.str().size());
  }
}

void CSVFormatter::ProcessChunk(const Chunk& chunk) {
  if (num_fields_ == 0) {
    return;
  }

  Assert(chunk.GetColumnCount() == num_fields_, "All chunks must have the same number of columns");

  std::stringstream buffer;
  for (size_t row_id = 0; row_id < chunk.Size(); row_id++) {
    for (size_t column_id = 0; column_id < num_fields_; column_id++) {
      std::shared_ptr<AbstractSegment> column = chunk.GetSegment(column_id);
      AllTypeVariant value = (*column)[row_id];
      if (column_id != 0) {
        buffer << options_.field_separator;
      }

      // Get the type of the variant, call the lambda function with it and write it to the buffer
      std::visit([&buffer](auto&& unpacked_value) { buffer << unpacked_value; }, value);
    }
    buffer << options_.record_separator;

    WriteToOutput(buffer.str().c_str(), buffer.str().size());
    buffer.str("");
  }
}

void CSVFormatter::Finalize() {
  // Nothing to do here.
}

}  // namespace skyrise
