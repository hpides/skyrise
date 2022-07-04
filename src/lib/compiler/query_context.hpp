#pragma once

#include <string>

#include <boost/container_hash/hash.hpp>

#include "metadata/abstract_catalog.hpp"
#include "physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class QueryContext {
 public:
  QueryContext(std::string query_string, std::shared_ptr<AbstractCatalog> catalog, std::string target_bucket_name);

  const std::string& QueryString() const;

  /**
   * @returns a unique string that identifies this context's query.
   */
  const std::string& QueryIdentity() const;

  /**
   * @returns the catalog that provides lookup information, such as TableSchema data, to compile the query.
   */
  std::shared_ptr<AbstractCatalog> Catalog() const;

  /**
   * The S3 bucket name into which intermediate & final results should be written.
   */
  const std::string& TargetBucketName() const;

  /**
   * Format and file extension of final result object(s).
   */
  ExportFormat TargetFormat() const;
  void SetTargetFormat(const ExportFormat target_format);
  const std::string& TargetFileExtension() const;

  /**
   * S3 object key prefixes for intermediate and final results of a query.
   */
  const std::string& FinalResultsKeyPrefix();
  const std::string& IntermediateResultsKeyPrefix();

  /**
   * Defines the level of intra-operator parallelism.
   */
  size_t MaxWorkerCount() const;
  void SetMaxWorkerCount(const size_t max_worker_count);

  /**
   * @returns unique PqpPipeline identity strings derived from this context's QueryIdentity.
   */
  std::string GenerateNextPipelineIdentity();

 private:
  const std::string query_string_;
  const std::shared_ptr<AbstractCatalog> catalog_;
  const std::string target_bucket_name_;
  ExportFormat target_format_ = ExportFormat::kCsv;
  size_t max_worker_count_ = 3000;
  size_t pipeline_counter_ = 1;
  std::string query_identity_;
};

}  // namespace skyrise
