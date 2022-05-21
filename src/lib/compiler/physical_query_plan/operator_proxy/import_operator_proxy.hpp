#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "import_options.hpp"
#include "storage/table/table_column_definition.hpp"
#include "types.hpp"

namespace skyrise {

class ImportOperatorProxy : public EnableMakeForPlanNode<ImportOperatorProxy, AbstractOperatorProxy>,
                            public AbstractOperatorProxy {
 public:
  ImportOperatorProxy(const std::vector<ObjectReference>& object_references, const std::vector<ColumnId>& column_ids);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  void SetObjectReferences(std::vector<ObjectReference> object_references);
  const std::vector<ObjectReference>& ObjectReferences() const;
  const std::vector<ColumnId>& ColumnIds() const;

  // If desired, non-default options for reading CSV/ORC data can be set.
  void SetImportOptions(std::shared_ptr<const ImportOptions> import_options);
  std::shared_ptr<const ImportOptions> GetImportOptions() const;

  /**
   * Optimization-relevant attributes
   */
  bool IsPipelineBreaker() const override;
  size_t OutputObjectsCount() const override;
  void SetOutputObjectsCount(size_t output_objects_count);
  size_t OutputColumnsCount() const override;

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
  size_t output_objects_count_ = std::numeric_limits<size_t>::max();
};

}  // namespace skyrise
