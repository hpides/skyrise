#include "abstract_format_reader.hpp"

namespace skyrise {

AbstractFormatReader::AbstractFormatReader() : error_(StorageErrorType::kNoError) {}

void AbstractFormatReader::SetError(StorageError error) { error_ = std::move(error); }

bool AbstractFormatReader::HasError() const { return error_.IsError(); }

const StorageError& AbstractFormatReader::GetError() const { return error_; }

const std::shared_ptr<const TableColumnDefinitions>& AbstractFormatReader::GetSchema() const { return schema_; }

}  // namespace skyrise
