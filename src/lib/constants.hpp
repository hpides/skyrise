#pragma once

#include "utils/literal.hpp"

namespace skyrise {

inline constexpr std::string_view kCsvExtension = ".csv";
inline constexpr std::string_view kOrcExtension = ".orc";
inline constexpr std::string_view kParquetExtension = ".parquet";

/**
 * The maximum item size in DynamoDB.
 */
inline constexpr size_t kDynamoDbMaxItemSize = 400_KB;

/**
 *  Hard limits in DynamoDB for batch requests.
 *  Exceeding them will either cause a ValidationException or the entire batch operations is rejected.
 */
inline constexpr uint kDynamoDbBatchGetItemLimit = 100;
inline constexpr uint kDynamoDbBatchWriteItemLimit = 25;

inline constexpr std::string_view kConstPrefix = "k";

}  // namespace skyrise
