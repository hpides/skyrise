#include "csv_reader.hpp"

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <boost/convert.hpp>
#include <boost/convert/strtol.hpp>

#include "storage/table/value_segment.hpp"

struct boost::cnv::by_default : public boost::cnv::strtol {};

namespace skyrise {

namespace {

const std::array<char, 5> kPossibleDelimiters{'|', ';', ',', '\t', ' '};

/**
 * Split tokenizes an input given a particular delimiter. It follows a zero-copy policy by using std::string_view.
 **/
void Split(std::string_view data, char delimiter,
           const std::function<void(std::string_view split_element, size_t counter)>& callback,
           size_t max_split_elements = 0) {
  size_t current_offset = 0;
  // Number of current split elements
  size_t split_element_counter = 0;
  do {
    const size_t next_newline = data.find(delimiter, current_offset);
    const std::string_view line = data.substr(current_offset, next_newline - current_offset);

    callback(line, split_element_counter);

    current_offset = next_newline;
    ++split_element_counter;

    // Continues if the current offset is not out of bounds and the current number of splits is smaller than the maximal
    // number of split elements (if applicable).
  } while (current_offset++ != std::string_view::npos &&
           (max_split_elements == 0 || split_element_counter < max_split_elements));
}

template <typename NumberType>
NumberType ToNumber(std::string_view data) {
  boost::optional<NumberType> result = boost::convert<NumberType>(data);

  if (!result.has_value()) {
    throw std::invalid_argument("ToNumber failed.");
  }
  return result.value();
}

template <typename TargetSegmentType>
std::shared_ptr<AbstractSegment> CreateSegment(
    std::vector<std::string_view>* column_values, size_t num_skip_lines,
    const std::function<TargetSegmentType(std::string_view data)>& callback) {
  auto result = std::make_shared<ValueSegment<TargetSegmentType>>(false, column_values->size());
  auto& destination = result->Values();
  for (size_t i = num_skip_lines; i < column_values->size(); ++i) {
    destination.emplace_back(callback(column_values->at(i)));
  }
  return result;
}

std::shared_ptr<AbstractSegment> ParseSegmentForDataType(DataType type, std::vector<std::string_view>* column,
                                                         size_t skip_lines = 0) {
  switch (type) {
    case DataType::kString:
      return CreateSegment<std::string>(column, skip_lines, [](std::string_view value) { return std::string(value); });
    case DataType::kLong:
      return CreateSegment<int64_t>(column, skip_lines, [](std::string_view value) {
        // NOLINTNEXTLINE(clang-analyzer-optin.cplusplus.UninitializedObject)
        return ToNumber<int64_t>(value);
      });
    case DataType::kInt:
      return CreateSegment<int32_t>(column, skip_lines, [](std::string_view value) {
        // NOLINTNEXTLINE(clang-analyzer-optin.cplusplus.UninitializedObject)
        return ToNumber<int32_t>(value);
      });
    case DataType::kFloat:
      return CreateSegment<float>(column, skip_lines, [](std::string_view value) {
        // NOLINTNEXTLINE(clang-analyzer-optin.cplusplus.UninitializedObject)
        return ToNumber<float>(value);
      });
    case DataType::kDouble:
      return CreateSegment<double>(column, skip_lines, [](std::string_view value) {
        // NOLINTNEXTLINE(clang-analyzer-optin.cplusplus.UninitializedObject)
        return ToNumber<double>(value);
      });
    default:
      Fail("Encountered invalid type");
  }
}

DataType IdentifierToType(std::string_view identifier) {
  if (identifier == "int") {
    return DataType::kInt;
  }
  if (identifier == "long") {
    return DataType::kLong;
  }
  if (identifier == "float") {
    return DataType::kFloat;
  }
  if (identifier == "double") {
    return DataType::kDouble;
  }
  if (identifier == "string") {
    return DataType::kString;
  }

  Fail("Invalid type found in type definition.");
}

}  // namespace

bool CsvFormatReader::GuessHasTypeInformation(const Columns& columns) {
  if (columns.size() < 2) {
    return false;
  }

  return std::all_of(columns.cbegin(), columns.cend(), [](const Lines& lines) {
    if (lines.size() < 2) {
      return false;
    }
    if (!(lines[1] == "int" || lines[1] == "long" || lines[1] == "float" || lines[1] == "double" ||
          lines[1] == "string")) {
      return false;
    }
    return true;
  });
}

bool CsvFormatReader::GuessHasHeader(const Columns& columns) {
  // The idea behind this function is that if there is a header, the values will behave like identifier and will start
  // with [a-z].
  if (columns.empty() || columns[0].empty()) {
    return false;
  }

  for (const auto& column : columns) {
    if (column.empty() || column[0].empty() || !std::isalpha(column[0][0])) {
      return false;
    }
  }

  // If we see that the values in any other line do not look like an identifier, we see that the first line is
  // special and is most likely a header.
  const size_t num_lines_look_ahead = 5;
  for (const auto& column : columns) {
    for (size_t i = 1; i < std::min(column.size(), num_lines_look_ahead); ++i) {
      if (column[i].empty() || !std::isalpha(column[i][0])) {
        return true;
      }
    }
  }

  // If we are not sure, we will return false.
  return false;
}

char CsvFormatReader::GuessDelimiter(const Lines& lines) {
  std::array<int, kPossibleDelimiters.size()> delimiter_probability{};
  delimiter_probability.fill(2);

  if (lines.empty()) {
    return kPossibleDelimiters[0];
  }

  for (size_t i = 0; i < kPossibleDelimiters.size(); ++i) {
    const size_t num_occurences_in_first_line = std::count(lines[0].cbegin(), lines[0].cend(), kPossibleDelimiters[i]);

    if (num_occurences_in_first_line == 0) {
      delimiter_probability[i] = 1;
    }

    for (size_t j = 1; j < lines.size(); ++j) {
      const size_t occurrence_in_other_lines = std::count(lines[j].cbegin(), lines[j].cend(), kPossibleDelimiters[i]);
      if (num_occurences_in_first_line != occurrence_in_other_lines) {
        delimiter_probability[i] = 0;
        break;
      }
    }
  }

  const size_t maximum_index = std::distance(
      delimiter_probability.cbegin(), std::max_element(delimiter_probability.cbegin(), delimiter_probability.cend()));
  return kPossibleDelimiters[maximum_index];
}

CsvFormatReader::CsvFormatReader(std::shared_ptr<ObjectBuffer> source, size_t /*object_size*/,
                                 Configuration configuration)
    : configuration_(std::move(configuration)), source_(std::move(source)) {
  schema_ = configuration_.expected_schema;
  buffer_.reserve(configuration_.read_buffer_size);
  const StorageError maybe_error = FillBuffer();

  if (maybe_error.IsError()) {
    SetError(maybe_error);
    return;
  }

  InitialSetup();
}

void CsvFormatReader::BuildColumnTypes(TableColumnDefinitions* schema) {
  if (configuration_.has_types) {
    for (size_t i = 0; i < schema->size(); ++i) {
      (*schema)[i].data_type = IdentifierToType(columns_[i][1]);
    }
  } else {
    // If we do not have more information, everything is a string.
    for (auto& column_definition : *schema) {
      column_definition.data_type = DataType::kString;
    }
  }
}

void CsvFormatReader::BuildColumnNames(TableColumnDefinitions* schema) {
  if (configuration_.has_header) {
    for (const auto& headline : columns_) {
      if (headline.empty()) {
        break;
      }
      schema->emplace_back();
      schema->back().name = headline.front();
    }
  } else {
    std::stringstream headline;
    for (size_t i = 0; i < columns_.size(); ++i) {
      headline.clear();
      headline << "Column" << (i + 1);
      schema->emplace_back();
      schema->back().name = headline.str();
    }
  }
}

void CsvFormatReader::BuildSchema() {
  auto schema = std::make_shared<TableColumnDefinitions>();
  BuildColumnNames(schema.get());
  BuildColumnTypes(schema.get());
  schema_ = std::move(schema);
}

void CsvFormatReader::InitialSetup() {
  Lines lines;
  const std::string_view buffer_content(buffer_.data(), buffer_.size());
  const size_t num_lines_look_ahead = 5;
  const auto line_handler = [&lines](std::string_view data, size_t /*counter*/) { lines.emplace_back(data); };
  Split(buffer_content, '\n', line_handler, num_lines_look_ahead);

  if (configuration_.guess_delimiter) {
    configuration_.delimiter = GuessDelimiter(lines);
    configuration_.guess_delimiter = false;
  }

  ExtractColumns();

  if (configuration_.guess_has_header) {
    configuration_.has_header = GuessHasHeader(columns_);
    configuration_.guess_has_header = false;
  }

  if (configuration_.has_header) {
    num_ignore_lines_in_next_chunk_ = 1;
  }

  if (configuration_.has_header && configuration_.guess_has_types) {
    configuration_.has_types = GuessHasTypeInformation(columns_);
  }

  configuration_.guess_has_types = false;

  if (configuration_.has_types) {
    num_ignore_lines_in_next_chunk_++;
  }

  if (configuration_.expected_schema == nullptr) {
    BuildSchema();
  }
}

StorageError CsvFormatReader::FillBuffer() {
  buffer_.clear();
  if (read_full_file_) {
    return StorageError::Success();
  }

  buffer_.resize(configuration_.read_buffer_size);
  auto buffer_view = source_->Read(chunk_offset_, configuration_.read_buffer_size);
  std::copy_n(buffer_view->Data(), buffer_view->Size(), buffer_.data());
  buffer_.resize(buffer_view->Size());

  // If we did not reach the end of the file, we have to back up to avoid having unfinished lines in the buffer.
  if (buffer_.size() == configuration_.read_buffer_size) {
    auto newline_offset = std::find(buffer_.rbegin(), buffer_.rend(), '\n');
    if (newline_offset != buffer_.rend()) {
      buffer_.resize(buffer_.size() - std::distance(buffer_.rbegin(), newline_offset));
    }
  } else {
    read_full_file_ = true;
  }

  //  We do not want our buffer to end with a newline.
  size_t num_truncate_lines = 0;
  while (buffer_.size() > num_truncate_lines && buffer_[buffer_.size() - num_truncate_lines - 1] == '\n') {
    ++num_truncate_lines;
  }

  buffer_.resize(buffer_.size() - num_truncate_lines);

  chunk_offset_ += buffer_.size();
  return StorageError::Success();
}

void CsvFormatReader::ExtractColumns() {
  columns_.clear();
  const std::string_view buffer_content(buffer_.data(), buffer_.size());
  Split(buffer_content, '\n', [this](std::string_view line, size_t /*line_num*/) {
    if (!line.empty()) {
      Split(line, configuration_.delimiter, [this](std::string_view column, size_t column_counter) {
        if (columns_.size() == column_counter) {
          columns_.emplace_back();
        }

        columns_[column_counter].emplace_back(column);
      });
    }
  });
}

bool CsvFormatReader::HasNext() {
  return !HasError() && !buffer_.empty() && !columns_.empty() && num_ignore_lines_in_next_chunk_ < columns_[0].size();
}

std::unique_ptr<Chunk> CsvFormatReader::Next() {
  Segments segments;
  bool has_error = false;
  try {
    for (size_t i = 0; i < schema_->size(); ++i) {
      segments.emplace_back(
          ParseSegmentForDataType(schema_->at(i).data_type, &columns_.at(i), num_ignore_lines_in_next_chunk_));
    }
  } catch (const std::out_of_range& /*e*/) {
    has_error = true;
  } catch (const std::invalid_argument& /*e*/) {
    has_error = true;
  }

  if (has_error) {
    SetError(StorageError(StorageErrorType::kInvalidArgument,
                          "Error while converting values in file. Make sure the file does match the schema."));
    return nullptr;
  }

  num_ignore_lines_in_next_chunk_ = 0;

  const StorageError maybe_error = FillBuffer();
  if (maybe_error.IsError()) {
    SetError(maybe_error);
  }

  ExtractColumns();
  return std::make_unique<Chunk>(segments);
}

}  // namespace skyrise
