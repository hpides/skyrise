#include "etl_pqp_generator.hpp"

#include <aws/s3/model/GetObjectAttributesRequest.h>
#include <aws/s3/model/GetObjectAttributesResult.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <magic_enum/magic_enum.hpp>

#include "compiler/abstract_compiler.hpp"

namespace skyrise {

EtlPqpGenerator::EtlPqpGenerator(const QueryId& query_id, const ScaleFactor& scale_factor,
                                 const ObjectReference& shuffle_storage_prefix)
    : AbstractCompiler(query_id, scale_factor, shuffle_storage_prefix) {}

std::vector<std::shared_ptr<PqpPipeline>> EtlPqpGenerator::GeneratePqp() const {
  Assert(query_id_ == QueryId::kEtlCopyTpchOrders, "Query is not implemented");

  const std::string import_id = "import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2, 3, 4, 5, 7});
  import_operator->SetIdentity(import_id);

  ObjectReference output_placeholder(kSkyriseTestBucket, "output.csv");
  const std::string export_id = "export";
  const auto export_operator = std::make_shared<ExportOperatorProxy>(output_placeholder, FileFormat::kCsv);
  export_operator->SetLeftInput(import_operator);
  export_operator->SetIdentity(export_id);
  size_t object_count = 0;
  std::string scale_factor_infix;
  switch (scale_factor_) {
    case ScaleFactor::kSf1: {
      object_count = 1;
      scale_factor_infix = "1";
    } break;
    case ScaleFactor::kSf10: {
      object_count = 3;
      scale_factor_infix = "10";
    } break;
    case ScaleFactor::kSf100: {
      object_count = 25;
      scale_factor_infix = "100";
    } break;
    case ScaleFactor::kSf1000: {
      object_count = 249;
      scale_factor_infix = "1000";
    }
  }

  const std::string pipeline_id = "pipeline";
  std::vector<ObjectReference> input_objects;
  std::vector<ObjectReference> output_objects;
  const std::string tpch_orders_prefix = "tpc-h/standard/parquet/sf" + scale_factor_infix + "/orders/";
  for (size_t i = 0; i < object_count; i++) {
    input_objects.emplace_back(kS3BenchmarkDatasetsBucket, tpch_orders_prefix + std::to_string(i) + ".parquet");
    output_objects.emplace_back(shuffle_storage_prefix_.bucket_name,
                                shuffle_storage_prefix_.identifier + "/" + std::to_string(i) + ".csv");
  }

  auto pipeline = std::make_shared<PqpPipeline>(pipeline_id, export_operator);
  std::string input_id;
  input_id.append(pipeline_id).append("-").append(import_id);
  for (size_t i = 0; i < input_objects.size(); ++i) {
    std::unordered_map<std::string, std::vector<ObjectReference>> fragment_inputs;
    fragment_inputs[input_id].emplace_back(input_objects[i]);
    const auto fragment = PipelineFragmentDefinition(fragment_inputs, output_objects[i], FileFormat::kCsv);
    pipeline->AddFragmentDefinition(fragment);
  }
  return {pipeline};
}

EtlPqpGeneratorConfig::EtlPqpGeneratorConfig(const CompilerName& compiler_name, const QueryId& query_id,
                                             const ScaleFactor& scale_factor,
                                             const ObjectReference& shuffle_storage_prefix)
    : AbstractCompilerConfig(compiler_name, query_id, scale_factor, shuffle_storage_prefix) {}

std::shared_ptr<AbstractCompiler> EtlPqpGeneratorConfig::GenerateCompiler() const {
  return std::make_shared<EtlPqpGenerator>(query_id_, scale_factor_, shuffle_storage_prefix_);
}

bool EtlPqpGeneratorConfig::operator==(const EtlPqpGeneratorConfig& other) const {
  return query_id_ == other.query_id_ && scale_factor_ == other.scale_factor_ &&
         shuffle_storage_prefix_ == other.shuffle_storage_prefix_;
}

}  // namespace skyrise
