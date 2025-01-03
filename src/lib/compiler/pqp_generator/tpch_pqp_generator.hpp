#pragma once

#include "compiler/abstract_compiler.hpp"

namespace skyrise {

class TpchPqpGenerator : public AbstractCompiler {
 public:
  TpchPqpGenerator(const QueryId& query_id, const ScaleFactor& scale_factor,
                   const ObjectReference& shuffle_storage_prefix);

  std::vector<std::shared_ptr<PqpPipeline>> GeneratePqp() const final;

 private:
  std::vector<ObjectReference> ListTableObjects(const std::string& table_name, const FileFormat& import_format) const;

  std::vector<ObjectReference> GenerateOutputObjectIds(size_t count, const std::string& prefix,
                                                       const FileFormat export_format) const;

  static std::shared_ptr<PqpPipeline> GeneratePipeline(const std::string& pipeline_id, const std::string& import_id,
                                                       const std::shared_ptr<AbstractOperatorProxy>& pqp,
                                                       const FileFormat& export_format,
                                                       std::vector<ObjectReference> input_objects,
                                                       std::vector<ObjectReference> output_objects);

  // Query 1.
  static std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ1Pipeline1(
      const std::vector<ObjectReference>& input_objects);
  static std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ1Pipeline2(
      const std::vector<ObjectReference>& input_objects);
  static std::vector<std::shared_ptr<PqpPipeline>> GenerateQ1();

  // Query 6.
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ6Pipeline1(
      const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ6Pipeline2(
      const std::vector<ObjectReference>& input_objects) const;
  std::vector<std::shared_ptr<PqpPipeline>> GenerateQ6() const;

  // Query 12.
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ12Pipeline1(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ12Pipeline2(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ12Pipeline3(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects_left,
      const std::vector<ObjectReference>& input_objects_right) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ12Pipeline4(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects) const;
  std::vector<std::shared_ptr<PqpPipeline>> GenerateQ12() const;
};

class TpchPqpGeneratorConfig : public AbstractCompilerConfig {
 public:
  TpchPqpGeneratorConfig(const CompilerName& compiler_name, const QueryId& query_id, const ScaleFactor& scale_factor,
                         const ObjectReference& shuffle_storage_prefix);

  std::shared_ptr<AbstractCompiler> GenerateCompiler() const final;
  bool operator==(const TpchPqpGeneratorConfig& other) const;
};

}  // namespace skyrise
