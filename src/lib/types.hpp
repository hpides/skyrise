/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <ostream>
#include <regex>
#include <vector>

#include <aws/core/utils/json/JsonSerializer.h>

#include "utils/assert.hpp"
#include "utils/json.hpp"

namespace skyrise {

class Noncopyable {
 public:
  Noncopyable() = default;
  Noncopyable(const Noncopyable&) = delete;
  Noncopyable(Noncopyable&&) noexcept = default;

  Noncopyable& operator=(Noncopyable&&) noexcept = default;
  const Noncopyable& operator=(const Noncopyable&) = delete;

  ~Noncopyable() = default;
};

enum class DescriptionMode : std::uint8_t { kSingleLine, kMultiLine };

using ChunkId = uint32_t;
using ChunkOffset = uint32_t;
using ColumnCount = uint32_t;
using ColumnId = uint32_t;
using TaskId = uint32_t;

inline constexpr ColumnId kInvalidColumnId = std::numeric_limits<ColumnId>::max();
inline constexpr TaskId kInvalidTaskId = std::numeric_limits<TaskId>::max();

/**
 * Outputs @param column_ids to @param stream as a comma-separated list.
 * Function is used by multiple Description() implementations.
 */
std::ostream& operator<<(std::ostream& stream, const std::vector<ColumnId>& column_ids);

enum class PredicateCondition : std::uint8_t {
  kEquals,
  kNotEquals,
  kLessThan,
  kLessThanEquals,
  kGreaterThan,
  kGreaterThanEquals,
  kBetweenInclusive,
  kBetweenLowerExclusive,
  kBetweenUpperExclusive,
  kBetweenExclusive,
  kIn,
  kNotIn,
  kLike,
  kNotLike,
  kIsNull,
  kIsNotNull
};

enum class SchedulePriority : std::uint8_t {
  kDefault = 1,  // Schedule task at the end of the queue.
  kHigh = 0      // Schedule task at the beginning of the queue.
};

std::ostream& operator<<(std::ostream& stream, const PredicateCondition predicate_condition);

/**
 * @return Whether the PredicateCondition takes exactly two arguments.
 */
bool IsBinaryPredicateCondition(const PredicateCondition predicate_condition);

/**
 * @return Whether the PredicateCondition takes exactly two arguments and is not one of LIKE or IN.
 */
bool IsBinaryNumericPredicateCondition(const PredicateCondition predicate_condition);

bool IsBetweenPredicateCondition(PredicateCondition predicate_condition);

bool IsLowerInclusiveBetween(PredicateCondition predicate_condition);

bool IsUpperInclusiveBetween(PredicateCondition predicate_condition);

/**
 * Flip condition: ">" becomes "<" etc.
 */
PredicateCondition FlipPredicateCondition(const PredicateCondition predicate_condition);

/**
 * Flip condition: ">" becomes "<=" etc.
 */
PredicateCondition InversePredicateCondition(const PredicateCondition predicate_condition);

/**
 * Split up, e.g., BetweenUpperExclusive into {GreaterThanEquals, LessThan}.
 */
std::pair<PredicateCondition, PredicateCondition> BetweenToConditions(const PredicateCondition predicate_condition);

/**
 * Join, e.g., {GreaterThanEquals, LessThan} into BetweenUpperExclusive.
 */
PredicateCondition ConditionsToBetween(const PredicateCondition lower, const PredicateCondition upper);

/**
 * Supported aggregate functions. In addition to the default SQL functions (e.g., MIN(), MAX()), Skyrise internally uses
 * the ANY() function, which expects all values in the group to be equal and returns that value. In SQL terms, this
 * would be an additional, but unnecessary GROUP BY column. This function is only used by the optimizer in case that
 * all values of the group are known to be equal.
 */
enum class AggregateFunction : std::uint8_t {
  kAny,
  kAvg,
  kCount,
  kCountDistinct,
  kMax,
  kMin,
  kStandardDeviationSample,
  kStringAgg,
  kSum
};
std::ostream& operator<<(std::ostream& stream, const AggregateFunction aggregate_function);

enum class ExchangeMode : std::uint8_t { kFullMerge, kPartialMerge };
std::ostream& operator<<(std::ostream& stream, ExchangeMode exchange_mode);

/**
 * Let R and S be two tables and we want to perform R <JoinMode> S ON <condition>
 * kAntiNullAsTrue:    If for a tuple Ri in R, there is a tuple Sj in S so that <condition> is NULL or TRUE, Ri is
 *                      dropped. This behavior mirrors NOT IN.
 * kAntiNullAsFalse:   If for a tuple Ri in R, there is a tuple Sj in S so that <condition> is TRUE, Ri is
 *                      dropped. This behavior mirrors NOT EXISTS
 */
enum class JoinMode : std::uint8_t {
  kAntiNullAsFalse,
  kAntiNullAsTrue,
  kCross,
  kFullOuter,
  kInner,
  kLeftOuter,
  kRightOuter,
  kSemi
};
std::ostream& operator<<(std::ostream& stream, const JoinMode join_mode);

/**
 * SQL set operations come in two flavors, with and without `ALL`, e.g., `UNION` and `UNION ALL`.
 */
enum class SetOperationMode : std::uint8_t { kAll, kUnique };
std::ostream& operator<<(std::ostream& stream, SetOperationMode set_operation_mode);

/**
 * According to the SQL standard, the position of NULLs is implementation-defined. In Skyrise, NULLs come before all
 * values, both for ascending and descending sorts.
 */
enum class SortMode : std::uint8_t { kAscending, kDescending };
std::ostream& operator<<(std::ostream& stream, SortMode sort_mode);

/**
 * Defines in which order a certain column should be or is sorted.
 */
struct SortColumnDefinition final {
  explicit SortColumnDefinition(ColumnId column_id, SortMode sort_mode = SortMode::kAscending)
      : column_id(column_id), sort_mode(sort_mode) {}

  size_t Hash() const;

  ColumnId column_id;
  SortMode sort_mode;
};

inline bool operator==(const SortColumnDefinition& lhs, const SortColumnDefinition& rhs) {
  return lhs.column_id == rhs.column_id && lhs.sort_mode == rhs.sort_mode;
}

/**
 * Defines the file formats supported by the ExportOperator.
 */
enum class FileFormat : std::uint8_t { kCsv, kOrc, kParquet };

inline std::string GetFormatName(const FileFormat& format) {
  std::string format_name;
  switch (format) {
    case FileFormat::kCsv:
      format_name = "csv";
      break;
    case FileFormat::kOrc:
      format_name = "orc";
      break;
    case FileFormat::kParquet:
      format_name = "parquet";
      break;
    default:
      Fail("Format is not supported.");
  };
  return format_name;
}

inline std::string GetFormatExtension(const FileFormat& format) { return std::string(".") + GetFormatName(format); }

/**
 * Defines a general reference to an object stored in S3.
 */
struct ObjectReference {
  explicit ObjectReference(std::string init_bucket_name, std::string init_identifier, const std::string& init_etag = "",
                           std::optional<std::vector<int32_t>> init_partitions = std::nullopt)
      : bucket_name(std::move(init_bucket_name)),
        identifier(std::move(init_identifier)),
        etag(init_etag),
        partitions(std::move(init_partitions)) {
    Assert(!bucket_name.empty(), "ObjectReference requires a non-empty bucket name.");
    Assert(!identifier.empty(), "ObjectReference requires a non-empty object identifier.");
  }

  explicit ObjectReference(const std::string& s3_uri) {
    std::smatch match;
    std::regex_search(s3_uri, match, std::regex("^s3://([\\w-]+)/(.*)$"));
    DebugAssert(match.size() == 3, "Unable to extract the S3 bucket and prefix from \"" + s3_uri + "\".");

    bucket_name = match[1];
    identifier = match[2];
  }

  explicit ObjectReference(const Aws::Utils::Json::JsonView& json)
      : ObjectReference(json.GetString("bucket_name"), json.GetString("identifier"), json.GetString("etag"),
                        json.KeyExists("partitions")
                            ? std::make_optional(JsonArrayToVector<int32_t>(json.GetArray("partitions")))
                            : std::nullopt) {}

  std::string S3Uri() const { return "s3://" + bucket_name + "/" + identifier; }

  Aws::Utils::Json::JsonValue Serialize() const { return ToJson(); }

  bool operator==(const ObjectReference& other) const {
    return bucket_name == other.bucket_name && identifier == other.identifier && etag == other.etag &&
           partitions == other.partitions;
  }

  static ObjectReference FromJson(const Aws::Utils::Json::JsonView& json) {
    auto partitions = json.KeyExists("partitions")
                          ? std::make_optional(JsonArrayToVector<int32_t>(json.GetArray("partitions")))
                          : std::nullopt;
    return ObjectReference(json.GetString("bucket_name"), json.GetString("identifier"), json.GetString("etag"),
                           partitions);
  }

  Aws::Utils::Json::JsonValue ToJson() const {
    auto json = Aws::Utils::Json::JsonValue()
                    .WithString("bucket_name", bucket_name)
                    .WithString("identifier", identifier)
                    .WithString("etag", etag);
    if (partitions) {
      json = json.WithArray("partitions", VectorToJsonArray(*partitions));
    }
    return json;
  }

  std::string bucket_name;
  std::string identifier;
  std::string etag;
  std::optional<std::vector<int32_t>> partitions = std::nullopt;
};

enum class CompilerName : std::uint8_t { kEtl, kProcessMining, kSql, kTpch, kTpcxbb };

enum class QueryId : std::uint8_t {
  kEtlCopyTpchOrders,
  kProcessMiningQ3,
  kProcessMiningQ9,
  kTpchQ1,
  kTpchQ6,
  kTpchQ12,
  kTpcxbbQ3,
  kTpcxbbQ8
};

enum class ScaleFactor : std::uint8_t { kSf1, kSf10, kSf100, kSf1000 };

}  // namespace skyrise
