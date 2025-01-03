#pragma once

#include "binary_serialization_stream.hpp"
#include "storage/table/table_column_definition.hpp"

namespace skyrise {

BinarySerializationStream& operator<<(BinarySerializationStream& stream, const TableColumnDefinition& value);
BinarySerializationStream& operator>>(BinarySerializationStream& stream, TableColumnDefinition& value);

BinarySerializationStream& operator<<(BinarySerializationStream& stream, const TableColumnDefinitions& values);
BinarySerializationStream& operator>>(BinarySerializationStream& stream, TableColumnDefinitions& values);

}  // namespace skyrise
