#include "schema_serialization.hpp"

#include <magic_enum/magic_enum.hpp>

namespace skyrise {

BinarySerializationStream& operator<<(BinarySerializationStream& stream, const TableColumnDefinition& value) {
  return stream << value.name << std::string(magic_enum::enum_name(value.data_type)) << value.nullable;
}

BinarySerializationStream& operator>>(BinarySerializationStream& stream, TableColumnDefinition& value) {
  std::string enum_value;
  stream >> value.name >> enum_value >> value.nullable;
  value.data_type = magic_enum::enum_cast<skyrise::DataType>(enum_value).value();
  return stream;
}

BinarySerializationStream& operator<<(BinarySerializationStream& stream, const TableColumnDefinitions& values) {
  stream << static_cast<int64_t>(values.size());
  for (const auto& value : values) {
    stream << value;
  }
  return stream;
}

BinarySerializationStream& operator>>(BinarySerializationStream& stream, TableColumnDefinitions& values) {
  int64_t number_of_definitions = 0;
  stream >> number_of_definitions;
  values.resize(number_of_definitions);
  for (int64_t i = 0; i < number_of_definitions; ++i) {
    stream >> values[i];
  }
  return stream;
}

}  // namespace skyrise
