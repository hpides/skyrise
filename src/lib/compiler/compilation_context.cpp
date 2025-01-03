#include "compilation_context.hpp"

#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

#include "configuration.hpp"
#include "utils/assert.hpp"
#include "utils/time.hpp"

namespace skyrise {

CompilationContext::CompilationContext(SqlRequest sql_request, std::shared_ptr<AbstractCatalog> catalog)
    : sql_request_(std::move(sql_request)), catalog_(std::move(catalog)) {
  Assert(!sql_request_.query_string.empty(), "Unexpected empty query string.");
  Assert(catalog_, "A catalog instance is required to create a valid CompilationContext.");

  // Generate query identity, which is used to generate object key prefixes for pipeline results.
  query_identity_ = std::to_string(boost::hash_value(QueryString()));
}

const std::string& CompilationContext::QueryString() const { return sql_request_.query_string; }

const std::string& CompilationContext::QueryIdentity() const { return query_identity_; }

std::shared_ptr<AbstractCatalog> CompilationContext::Catalog() const { return catalog_; }

const std::string& CompilationContext::ExportBucketName() const { return export_bucket_name_; }

void CompilationContext::SetExportBucketName(std::string export_bucket_name) {
  Assert(!export_bucket_name.empty(), "Unexpected empty target bucket name.");
  export_bucket_name_ = std::move(export_bucket_name);
}

FileFormat CompilationContext::GetExportFormat() const { return target_format_; }

void CompilationContext::SetExportFormat(const FileFormat target_format) { target_format_ = target_format; }

std::string CompilationContext::ExportFileExtension() const {
  switch (target_format_) {
    case FileFormat::kOrc:
      return GetFormatExtension(FileFormat::kOrc);
    case FileFormat::kCsv:
      return GetFormatExtension(FileFormat::kCsv);
    default:
      Fail("Unknown file extension.");
  }
}

size_t CompilationContext::NextPipelineId() { return pipeline_counter_++; }

std::string CompilationContext::PipelineExportPrefix(size_t pipeline_id) {
  std::stringstream stream;
  if (pipeline_export_prefix_.empty()) {
    stream << kExportRootPrefix << sql_request_.user_name << '/';

    // Create unique query prefix consisting of
    //  - a timestamp including milliseconds
    //  - the query identity (hash of the query string)
    const auto time_in_seconds = std::chrono::system_clock::to_time_t(sql_request_.arrival_time);
    stream << GetFormattedTimestamp(time_in_seconds, "%Y-%m-%d_%H:%M:%S");
    stream << "'"
           << std::chrono::duration_cast<std::chrono::milliseconds>(
                  sql_request_.arrival_time - std::chrono::system_clock::from_time_t(time_in_seconds))
                  .count();
    stream << '_' << QueryIdentity() << '/' << "pipeline_";
    pipeline_export_prefix_ = stream.str();
  } else {
    stream << pipeline_export_prefix_;
  }

  stream << std::setfill('0') << std::setw(3) << pipeline_id << '/';
  return stream.str();
}

size_t CompilationContext::MaxWorkerCount() const { return max_worker_count_per_pipeline_; }

void CompilationContext::SetMaxWorkerCount(const size_t max_worker_count) {
  max_worker_count_per_pipeline_ = max_worker_count;
}

}  // namespace skyrise
