#pragma once

#include <string>

#include <boost/container_hash/hash.hpp>

#include "configuration.hpp"
#include "metadata/schema/abstract_catalog.hpp"
#include "physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

struct SqlRequest {
  SqlRequest(std::string init_query_string, std::string init_user_name,
             std::chrono::time_point<std::chrono::system_clock> init_arrival_time)
      : query_string(std::move(init_query_string)),
        user_name(std::move(init_user_name)),
        arrival_time(init_arrival_time) {}
  const std::string query_string;
  const std::string user_name;
  const std::chrono::time_point<std::chrono::system_clock> arrival_time;
};

class CompilationContext {
 public:
  CompilationContext(SqlRequest sql_request, std::shared_ptr<AbstractCatalog> catalog);

  const std::string& QueryString() const;

  /**
   * @returns a unique string that can be used to tag the result artifacts of the query.
   *          It is derived from the query string.
   */
  const std::string& QueryIdentity() const;

  /**
   * @returns the catalog that provides lookup information, such as TableSchema data, to compile the query.
   */
  std::shared_ptr<AbstractCatalog> Catalog() const;

  /**
   * The  bucket name into which intermediate and final results are written.
   */
  const std::string& ExportBucketName() const;
  void SetExportBucketName(std::string export_bucket_name);

  /**
   * Object format and according file extension of the final result's object(s).
   */
  FileFormat GetExportFormat() const;
  void SetExportFormat(const FileFormat target_format);
  std::string ExportFileExtension() const;

  /**
   * @returns the current pipeline counter, and increments it afterwards.
   */
  size_t NextPipelineId();

  /**
   * @returns the S3 object key prefix for the given @param pipeline_id results.
   */
  std::string PipelineExportPrefix(size_t pipeline_id);

  /**
   * Defines the level of intra-operator parallelism.
   */
  size_t MaxWorkerCount() const;
  void SetMaxWorkerCount(const size_t max_worker_count);

 private:
  const SqlRequest sql_request_;
  const std::shared_ptr<AbstractCatalog> catalog_;
  std::string export_bucket_name_ = std::string(kExportBucketName);
  FileFormat target_format_ = kFinalResultsExportFormat;
  size_t max_worker_count_per_pipeline_ = kWorkerMaximumCountPerPipeline;
  size_t pipeline_counter_ = 1;
  std::string query_identity_;
  std::string pipeline_export_prefix_;
};

}  // namespace skyrise
