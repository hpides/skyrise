#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "import_options.hpp"
#include "storage/table/table_column_definition.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * TODO Explain: Required for PQPs and the optimizer. In QE buckets refer to single Lambda function workers.
 */
enum class ObjectToBucketStrategy {
  SingleBucket,       // Lambda function workers read all objects. (e.g., for a broadcast join)
  MultipleBuckets,    // Lambda function workers read distinct object subsets. (e.g., for data parallelism)
  PartitionedBuckets  // Lambda function workers read all objects, but only distinct partition subsets.
                      // (e.g., for data shuffling purposes)
};

class ImportOperatorProxy : public EnableMakeForPlanNode<ImportOperatorProxy, AbstractOperatorProxy>,
                            public AbstractOperatorProxy {
 public:
  ImportOperatorProxy(std::vector<ObjectReference> object_references, std::vector<ColumnId> column_ids);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  const std::vector<ColumnId>& ColumnIds() const;
  const std::vector<ObjectReference>& ObjectReferences() const;
  void SetObjectReferences(std::vector<ObjectReference> object_references);

  // If desired, non-default options for reading CSV/ORC data can be set.
  std::shared_ptr<const ImportOptions> GetImportOptions() const;
  void SetImportOptions(std::shared_ptr<const ImportOptions> import_options);

  /**
   * Optimization-relevant attributes
   */
  const DataTraits& OutputDataTraits() const override;
  bool IsPipelineBreaker() const override;
  const std::optional<std::string>& OriginIdentifier() const;
  void SetOriginTraits(const std::string& origin_identifier, size_t partition_count);
  ObjectToBucketStrategy GetObjectToBucketStrategy() const;
  void SetObjectToBucketStrategy(ObjectToBucketStrategy object_to_bucket_strategy, size_t bucket_count);

  /**
   * Serialization / Deserialization
   */
  Aws::Utils::Json::JsonValue ToJson() const override;
  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json);

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  size_t ShallowHash() const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  const std::vector<ColumnId> column_ids_;
  std::vector<ObjectReference> object_references_;
  std::shared_ptr<const ImportOptions> import_options_;

  /**
   * Attributes for query compilation, and PQPs.
   */
  ObjectToBucketStrategy object_to_bucket_strategy_;

  // Mutable because the data structure is updated in the Getter.
  mutable DataTraits output_data_traits_;

  // Origin identifier is applicable to all object references specified.
  std::optional<std::string> origin_identifier_;

  // In PQPs, the object references from an Import proxy represent a pool of data, that either comes from a base table,
  // or a previously executed pipeline. In case of partitioned data, all objects have the same data layout, and thus
  // the same partition count.
  size_t expected_partition_count_;
};

}  // namespace skyrise
