#include "tpch_pqp_generator.hpp"

#include <magic_enum/magic_enum.hpp>

#include "compiler/abstract_compiler.hpp"
#include "compiler/physical_query_plan/operator_proxy/aggregate_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/alias_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/join_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/partition_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/projection_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/sort_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_pipeline.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "storage/backend/s3_utils.hpp"
#include "storage/formats/csv_reader.hpp"

namespace skyrise {

TpchPqpGenerator::TpchPqpGenerator(const QueryId& query_id, const ScaleFactor& scale_factor,
                                   const ObjectReference& shuffle_storage_prefix)
    : AbstractCompiler(query_id, scale_factor, shuffle_storage_prefix) {}

std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GeneratePqp() const {
  std::vector<std::shared_ptr<PqpPipeline>> result;
  switch (query_id_) {
    case QueryId::kTpchQ1:
      result = GenerateQ1();
      break;
    case QueryId::kTpchQ6:
      result = GenerateQ6();
      break;
    case QueryId::kTpchQ12:
      result = GenerateQ12();
      break;
    default:
      Fail("Unknown query.");
  }
  return result;
}

std::vector<ObjectReference> TpchPqpGenerator::ListTableObjects(const std::string& table_name,
                                                                const FileFormat& import_format) const {
  std::string scale_factor_infix;
  switch (scale_factor_) {
    case ScaleFactor::kSf1: {
      scale_factor_infix = "1";
    } break;
    case ScaleFactor::kSf10: {
      scale_factor_infix = "10";
    } break;
    case ScaleFactor::kSf100: {
      scale_factor_infix = "100";
    } break;
    case ScaleFactor::kSf1000: {
      scale_factor_infix = "1000";
    }
  }
  const std::string table_prefix = std::string("tpc-h/standard/") + GetFormatName(import_format) + "/sf" +
                                   scale_factor_infix + "/" + table_name + "/";

  // TODO(tobodner): Use base client for this.
  const auto s3_client = std::make_shared<Aws::S3::S3Client>();
  S3Storage s3_storage(s3_client, kS3BenchmarkDatasetsBucket);
  const auto outcome = s3_storage.List(table_prefix);
  Assert(!outcome.second.IsError(), outcome.second.GetMessage());

  std::vector<ObjectReference> objects;
  objects.reserve(outcome.first.size());
  for (const auto& object : outcome.first) {
    objects.emplace_back(kS3BenchmarkDatasetsBucket, object.GetIdentifier(), object.GetChecksum());
  }
  return objects;
}

std::vector<ObjectReference> TpchPqpGenerator::GenerateOutputObjectIds(size_t count, const std::string& prefix,
                                                                       const FileFormat export_format) const {
  std::vector<ObjectReference> object_ids;
  object_ids.reserve(count);
  while (count-- > 0) {
    const std::string object_id =
        shuffle_storage_prefix_.identifier + "/" + prefix + "/" + RandomString(8) + GetFormatExtension(export_format);
    object_ids.emplace_back(shuffle_storage_prefix_.bucket_name, object_id);
  }
  return object_ids;
}

std::shared_ptr<PqpPipeline> TpchPqpGenerator::GeneratePipeline(const std::string& pipeline_id,
                                                                const std::string& import_id,
                                                                const std::shared_ptr<AbstractOperatorProxy>& pqp,
                                                                const FileFormat& export_format,
                                                                std::vector<ObjectReference> input_objects,
                                                                std::vector<ObjectReference> output_objects) {
  std::vector<std::unordered_map<std::string, std::vector<ObjectReference>>> worker_input_mappings;
  worker_input_mappings.resize(output_objects.size());
  std::string input_id;
  input_id.append(pipeline_id).append("-").append(import_id);
  for (size_t i = 0; i < input_objects.size(); ++i) {
    worker_input_mappings[i % worker_input_mappings.size()][input_id].push_back(input_objects[i]);
  }

  auto pipeline = std::make_shared<PqpPipeline>(pipeline_id, pqp);
  for (size_t i = 0; i < output_objects.size(); ++i) {
    const PipelineFragmentDefinition fragment(worker_input_mappings[i], output_objects[i], export_format);
    pipeline->AddFragmentDefinition(fragment);
  }

  return pipeline;
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ1Pipeline1(
    const std::vector<ObjectReference>& /*input_objects*/) {
  return {std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>>()};
}
std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ1Pipeline2(
    const std::vector<ObjectReference>& /*input_objects*/) {
  return {std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>>()};
}
std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ1() {
  return {std::vector<std::shared_ptr<PqpPipeline>>()};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ6Pipeline1(
    const std::vector<ObjectReference>& input_objects) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  const std::string l_shipdate_start = "1994-01-01";
  const float l_discount = 0.06;
  const int32_t l_quantity = 24;

  const std::string import_id = "import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{4, 5, 6, 10});
  import_operator->SetIdentity(import_id);

  auto l_shipdate_end = l_shipdate_start;
  l_shipdate_end[3] += 1;
  const auto predicate1 = std::make_shared<BetweenExpression>(PredicateCondition::kBetweenUpperExclusive,
                                                              PqpColumn_(3, DataType::kString, false, "l_shipdate"),
                                                              Value_(l_shipdate_start), Value_(l_shipdate_end));
  const auto filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  filter_operator1->SetLeftInput(import_operator);

  const auto predicate2 = std::make_shared<BetweenExpression>(PredicateCondition::kBetweenInclusive,
                                                              PqpColumn_(2, DataType::kFloat, false, "l_discount"),
                                                              Value_(l_discount - 0.01f), Value_(l_discount + 0.01f));
  const auto filter_operator2 = std::make_shared<FilterOperatorProxy>(predicate2);
  filter_operator2->SetLeftInput(filter_operator1);

  const auto predicate3 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kLessThan, PqpColumn_(0, DataType::kFloat, false, "l_quantity"), Value_(l_quantity));
  const auto filter_operator3 = std::make_shared<FilterOperatorProxy>(predicate3);
  filter_operator3->SetLeftInput(filter_operator2);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{Mul_(
      PqpColumn_(1, DataType::kFloat, false, "l_extendedprice"), PqpColumn_(2, DataType::kFloat, false, "l_discount"))};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(filter_operator3);

  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(0, DataType::kFloat, false, "revenue"))};
  const std::vector<ColumnId> groupby_column_ids{};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(groupby_column_ids, aggregates);
  aggregate_operator->SetLeftInput(projection_operator);

  const auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(aggregate_operator);

  const std::string pipeline_id = "pipeline-1";
  const size_t worker_count = (input_objects.size() / 5) + (input_objects.size() % 5);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  return {output_objects, GeneratePipeline(pipeline_id, import_id, export_operator, kIntermediateResultsExportFormat,
                                           input_objects, output_objects)};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ6Pipeline2(
    const std::vector<ObjectReference>& input_objects) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  const std::string import_id = "import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0});
  import_operator->SetIdentity(import_id);

  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {std::make_shared<AggregateExpression>(
      AggregateFunction::kSum, PqpColumn_(0, DataType::kDouble, false, "revenue"))};
  const std::vector<ColumnId> groupby_column_ids{};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(groupby_column_ids, aggregates);
  aggregate_operator->SetLeftInput(import_operator);

  const std::vector<ColumnId>& column_ids_alias = {0};
  const std::vector<std::string>& aliases = {"revenue"};
  const auto alias_operator = std::make_shared<AliasOperatorProxy>(column_ids_alias, aliases);
  alias_operator->SetLeftInput(aggregate_operator);

  const auto export_operator = std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), FileFormat::kCsv);
  export_operator->SetLeftInput(alias_operator);

  const std::string pipeline_id = "pipeline-2";
  auto output_objects = GenerateOutputObjectIds(1, pipeline_id, FileFormat::kCsv);
  return {output_objects,
          GeneratePipeline(pipeline_id, import_id, export_operator, FileFormat::kCsv, input_objects, output_objects)};
}

std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ6() const {
  const auto pipeline1 = GenerateQ6Pipeline1(ListTableObjects("lineitem", FileFormat::kParquet));
  const auto pipeline2 = GenerateQ6Pipeline2(pipeline1.first);

  pipeline1.second->SetAsPredecessorOf(pipeline2.second);

  return {pipeline1.second, pipeline2.second};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ12Pipeline1(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  const std::vector<std::string> l_shipmode_in = {"MAIL", "SHIP"};
  const std::string l_receiptdate_start = "1994-01-01";  // inclusive
  const std::string l_receiptdate_end = "1995-01-01";    // exclusive

  const std::string left_import_id = "left_import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 10, 11, 12, 14});
  import_operator->SetIdentity(left_import_id);

  const auto predicate1 = std::make_shared<BetweenExpression>(PredicateCondition::kBetweenUpperExclusive,
                                                              PqpColumn_(3, DataType::kString, false, "l_receiptdate"),
                                                              Value_(l_receiptdate_start), Value_(l_receiptdate_end));
  const auto filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  filter_operator1->SetLeftInput(import_operator);

  const auto predicate2 =
      std::make_shared<InExpression>(PredicateCondition::kIn, PqpColumn_(4, DataType::kString, false, "l_shipmode"),
                                     List_(Value_(l_shipmode_in.front()), Value_(l_shipmode_in.back())));
  const auto filter_operator2 = std::make_shared<FilterOperatorProxy>(predicate2);
  filter_operator2->SetLeftInput(filter_operator1);

  const auto predicate3 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kLessThan, PqpColumn_(1, DataType::kString, false, "l_shipdate"),
      PqpColumn_(2, DataType::kString, false, "l_commitdate"));
  const auto filter_operator3 = std::make_shared<FilterOperatorProxy>(predicate3);
  filter_operator3->SetLeftInput(filter_operator2);

  const auto predicate4 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kLessThan, PqpColumn_(2, DataType::kString, false, "l_commitdate"),
      PqpColumn_(3, DataType::kString, false, "l_receiptdate"));
  const auto filter_operator4 = std::make_shared<FilterOperatorProxy>(predicate4);
  filter_operator4->SetLeftInput(filter_operator3);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(0, DataType::kInt, false, "l_orderkey"), PqpColumn_(4, DataType::kString, false, "l_shipmode")};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(filter_operator4);

  const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
      std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, partition_count));
  partition_operator->SetLeftInput(projection_operator);

  const auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(partition_operator);

  const std::string pipeline_id = "pipeline-1";
  const size_t worker_count = (input_objects.size() / 5 + (input_objects.size() % 5));
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  const auto pipeline1 = GeneratePipeline(pipeline_id, left_import_id, export_operator,
                                          kIntermediateResultsExportFormat, input_objects, output_objects);
  return {output_objects, pipeline1};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ12Pipeline2(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  const std::string left_import_id = "left_import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 5});
  import_operator->SetIdentity(left_import_id);

  const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
      std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, partition_count));
  partition_operator->SetLeftInput(import_operator);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(partition_operator);

  const std::string pipeline_id = "pipeline-2";
  const size_t worker_count = (input_objects.size() / 3) + (input_objects.size() % 3);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  const auto pipeline2 = GeneratePipeline(pipeline_id, left_import_id, export_operator,
                                          kIntermediateResultsExportFormat, input_objects, output_objects);
  return {output_objects, pipeline2};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ12Pipeline3(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects_left,
    const std::vector<ObjectReference>& input_objects_right) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  const std::string left_import_id = "left_import";
  auto import_operator_left =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1});
  import_operator_left->SetIdentity(left_import_id);

  const std::string right_import_id = "right_import";
  auto import_operator_right =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1});
  import_operator_right->SetIdentity(right_import_id);

  std::vector<std::shared_ptr<JoinOperatorPredicate>> empty_secondary_predicates;
  const auto predicate =
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{0, 0, PredicateCondition::kEquals});
  const auto join_operator =
      std::make_shared<JoinOperatorProxy>(JoinMode::kInner, predicate, empty_secondary_predicates);
  join_operator->SetLeftInput(import_operator_left);
  join_operator->SetRightInput(import_operator_right);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(1, DataType::kString, false, "l_shipmode"),
      Case_(Or_(Equals_(PqpColumn_(3, DataType::kString, false, "o_orderpriority"), Value_("1-URGENT")),
                Equals_(PqpColumn_(3, DataType::kString, false, "o_orderpriority"), Value_("2-HIGH"))),
            1, 0),
      Case_(And_(NotEquals_(PqpColumn_(3, DataType::kString, false, "o_orderpriority"), Value_("1-URGENT")),
                 NotEquals_(PqpColumn_(3, DataType::kString, false, "o_orderpriority"), Value_("2-HIGH"))),
            1, 0)};

  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(join_operator);

  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(1, DataType::kInt, false, "high_line_count")),
      Sum_(PqpColumn_(2, DataType::kInt, false, "low_line_count"))};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(std::vector<ColumnId>{0}, aggregates);
  aggregate_operator->SetLeftInput(projection_operator);

  auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(aggregate_operator);

  const std::string pipeline_id = "pipeline-3";
  const auto output_objects = GenerateOutputObjectIds(partition_count, pipeline_id, kIntermediateResultsExportFormat);

  auto pipeline3 = std::make_shared<PqpPipeline>(pipeline_id, export_operator);

  std::string left_input_id;
  left_input_id.append("pipeline-3").append("-").append(left_import_id);
  std::string right_input_id;
  right_input_id.append("pipeline-3").append("-").append(right_import_id);
  for (size_t i = 0; i < partition_count; ++i) {
    std::unordered_map<std::string, std::vector<ObjectReference>> map;
    map[left_input_id] = std::vector<ObjectReference>{};
    for (const auto& left_input : input_objects_left) {
      map[left_input_id].emplace_back(shuffle_storage_prefix_.bucket_name, left_input.identifier, "",
                                      std::vector<int32_t>{static_cast<int32_t>(i)});
    }
    map[right_input_id] = std::vector<ObjectReference>{};
    for (const auto& right_input : input_objects_right) {
      map[right_input_id].emplace_back(shuffle_storage_prefix_.bucket_name, right_input.identifier, "",
                                       std::vector<int32_t>{static_cast<int32_t>(i)});
    }
    const PipelineFragmentDefinition fragment(map, output_objects[i], FileFormat::kParquet);
    pipeline3->AddFragmentDefinition(fragment);
  }
  return {output_objects, pipeline3};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ12Pipeline4(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  const std::string left_import_id = "left_import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2});
  import_operator->SetIdentity(left_import_id);

  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(1, DataType::kLong, false, "high_line_count")),
      Sum_(PqpColumn_(2, DataType::kLong, false, "low_line_count"))};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(std::vector<ColumnId>{0}, aggregates);
  aggregate_operator->SetLeftInput(import_operator);

  const auto sort_operator =
      std::make_shared<SortOperatorProxy>(std::vector<SortColumnDefinition>{SortColumnDefinition(0)});
  sort_operator->SetLeftInput(aggregate_operator);

  const auto alias_operator = std::make_shared<AliasOperatorProxy>(
      std::vector<ColumnId>{0, 1, 2}, std::vector<std::string>{"l_shipmode", "high_line_count", "low_line_count"});
  alias_operator->SetLeftInput(sort_operator);

  auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(alias_operator);

  const std::string pipeline_id = "pipeline-4";
  auto output_objects = GenerateOutputObjectIds(partition_count, pipeline_id, FileFormat::kCsv);
  const auto pipeline4 =
      GeneratePipeline(pipeline_id, left_import_id, export_operator, FileFormat::kCsv, input_objects, output_objects);
  return {output_objects, pipeline4};
}

std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ12() const {
  size_t partition_count = 0;
  switch (scale_factor_) {
    case ScaleFactor::kSf1: {
      partition_count = 3;
    } break;
    case ScaleFactor::kSf10: {
      partition_count = 10;
    } break;
    case ScaleFactor::kSf100: {
      partition_count = 30;
    } break;
    case ScaleFactor::kSf1000: {
      partition_count = 100;
    }
  }

  const auto pipeline1 = GenerateQ12Pipeline1(partition_count, ListTableObjects("lineitem", FileFormat::kParquet));
  const auto pipeline2 = GenerateQ12Pipeline2(partition_count, ListTableObjects("orders", FileFormat::kParquet));
  const auto pipeline3 = GenerateQ12Pipeline3(partition_count, pipeline1.first, pipeline2.first);
  const auto pipeline4 = GenerateQ12Pipeline4(1, pipeline3.first);

  pipeline1.second->SetAsPredecessorOf(pipeline3.second);
  pipeline2.second->SetAsPredecessorOf(pipeline3.second);
  pipeline3.second->SetAsPredecessorOf(pipeline4.second);

  return {pipeline1.second, pipeline2.second, pipeline3.second, pipeline4.second};
}

TpchPqpGeneratorConfig::TpchPqpGeneratorConfig(const CompilerName& compiler_name, const QueryId& query_id,
                                               const ScaleFactor& scale_factor,
                                               const ObjectReference& shuffle_storage_prefix)
    : AbstractCompilerConfig(compiler_name, query_id, scale_factor, shuffle_storage_prefix) {}

std::shared_ptr<AbstractCompiler> TpchPqpGeneratorConfig::GenerateCompiler() const {
  return std::make_shared<TpchPqpGenerator>(query_id_, scale_factor_, shuffle_storage_prefix_);
}

bool TpchPqpGeneratorConfig::operator==(const TpchPqpGeneratorConfig& other) const {
  return query_id_ == other.query_id_ && scale_factor_ == other.scale_factor_ &&
         shuffle_storage_prefix_ == other.shuffle_storage_prefix_;
}

}  // namespace skyrise
