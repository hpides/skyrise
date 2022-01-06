#include "all_type_variant.hpp"

#include "constant_mappings.hpp"

namespace skyrise {

std::ostream& operator<<(std::ostream& stream, const DataType data_type) {
  return stream << kDataTypeToString.left.at(data_type);
}

std::ostream& operator<<(std::ostream& stream, const NullValue /*null_value*/) { return stream << "NULL"; }

std::ostream& operator<<(std::ostream& stream, const AllTypeVariant& value) {
  switch (DataTypeFromAllTypeVariant(value)) {
    case DataType::kString:
      stream << "'" << std::get<std::string>(value) << "'";
      break;
    case DataType::kInt:
      stream << std::get<int32_t>(value);
      break;
    case DataType::kLong:
      stream << std::get<int64_t>(value) << "L";
      break;
    case DataType::kFloat:
      stream << std::get<float>(value) << "F";
      break;
    case DataType::kDouble:
      stream << std::get<double>(value);
      break;
    case DataType::kNull:
      stream << std::get<NullValue>(value);
      break;
    default:
      Fail("Unsupported AllTypeVariant type.");
      break;
  }

  return stream;
}

}  // namespace skyrise
