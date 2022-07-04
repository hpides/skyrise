#include "query_context.hpp"

#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

#include "utils/assert.hpp"
#include "utils/time.hpp"

namespace {

const std::string kFinalResultsPrefix = "final_results/";
const std::string kIntermediateResultsPrefix = "intermediate_results/";
const std::string kOrcExtension = ".orc";
const std::string kCsvExtension = ".csv";

}  // namespace

namespace skyrise {

QueryContext::QueryContext(std::string query_string, std::shared_ptr<AbstractCatalog> catalog,
                           std::string target_bucket_name)
    : query_string_(std::move(query_string)),
      catalog_(std::move(catalog)),
      target_bucket_name_(std::move(target_bucket_name)) {
  Assert(!query_string_.empty(), "Unexpected empty query string.");
  Assert(!target_bucket_name_.empty(), "Unexpected empty target bucket name.");
  Assert(catalog_, "A catalog instance is required to create a valid QueryContext.");

  // Generate query identity, which is used to generate object keys for intermediate and final results. We must prevent
  // name conflicts with other query results, which might have the same query string. Therefore, we create the query
  // identity string from the following components:
  //  - a timestamp including microseconds
  //  - a hash of the query string
  std::stringstream stream;
  const auto wall_clock = std::chrono::high_resolution_clock::now();
  const auto time_in_seconds = std::chrono::system_clock::to_time_t(wall_clock);
  stream << GetFormattedTimestamp(time_in_seconds, "%Y/%m/%d_%H:%M:%S");
  stream << "'"
         << std::chrono::duration_cast<std::chrono::microseconds>(
                wall_clock - std::chrono::system_clock::from_time_t(time_in_seconds))
                .count();
  stream << "_" << boost::hash_value(query_string_);

  query_identity_ = stream.str();
}

const std::string& QueryContext::QueryString() const { return query_string_; }

const std::string& QueryContext::QueryIdentity() const { return query_identity_; };

std::shared_ptr<AbstractCatalog> QueryContext::Catalog() const { return catalog_; }

const std::string& QueryContext::TargetBucketName() const { return target_bucket_name_; }

ExportFormat QueryContext::TargetFormat() const { return target_format_; }

void QueryContext::SetTargetFormat(const ExportFormat target_format) { target_format_ = target_format; }

const std::string& QueryContext::TargetFileExtension() const {
  switch (target_format_) {
    case ExportFormat::kOrc:
      return kOrcExtension;
    case ExportFormat::kCsv:
      return kCsvExtension;
    default:
      Fail("Unknown file extension.");
  }
}

const std::string& QueryContext::FinalResultsKeyPrefix() { return kFinalResultsPrefix; }

const std::string& QueryContext::IntermediateResultsKeyPrefix() { return kIntermediateResultsPrefix; }

size_t QueryContext::MaxWorkerCount() const { return max_worker_count_; }

void QueryContext::SetMaxWorkerCount(const size_t max_worker_count) { max_worker_count_ = max_worker_count; }

std::string QueryContext::GenerateNextPipelineIdentity() {
  std::stringstream stream;
  stream << QueryIdentity() << "_pipeline_" << std::setfill('0') << std::setw(2) << pipeline_counter_;

  ++pipeline_counter_;
  return stream.str();
}

}  // namespace skyrise
