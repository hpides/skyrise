#include "client/base_client.hpp"
#include "compiler/physical_query_plan/operator_proxy/aggregate_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/alias_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/projection_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/sort_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_pipeline.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "storage/backend/s3_storage.hpp"
#include "testing/aws_test.hpp"

namespace skyrise {

using namespace expression_functional;

namespace {

// Taken from the TPC-H specification.
const std::string kTpchQ6StartDate = "1994-01-01";
const float kTpchQ6Discount = 0.06;
const int32_t kTpchQ6Quantity = 24;
const double kTpchQ6ExpectedResult = 123141078.2283;

}  // namespace

/**
 * This whole class/file becomes obsolet once the real compiler is merged.
 */
class MockCompiler {
 public:
  MockCompiler()
      : client_(std::make_shared<BaseClient>()), output_directory_(kSkyriseTestBucket, GetUniqueName("mockCompiler")) {}
  enum ScaleFactor { k1 = 1, k10 = 10, k100 = 100, k1000 = 1000 };

  std::pair<std::vector<std::shared_ptr<PqpPipeline>>, double> CompileTpchQuery6(const ScaleFactor sf) {
    const std::string intermediate_suffix = std::string(".") + GetFormatName(kIntermediateFormat);
    // ======================= STAGE 1 =======================
    // Import
    std::shared_ptr<ImportOperatorProxy> import_operator1 =
        std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{4, 5, 6, 10});
    import_operator1->SetIdentity(import_identity_);

    // Filter by l_shipdate.
    auto end_date = kTpchQ6StartDate;
    end_date[3] += 1;

    const auto predicate1 = std::make_shared<BetweenExpression>(PredicateCondition::kBetweenUpperExclusive,
                                                                PqpColumn_(3, DataType::kString, false, "l_shipdate"),
                                                                Value_(kTpchQ6StartDate), Value_(end_date));
    std::shared_ptr<FilterOperatorProxy> filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
    filter_operator1->SetLeftInput(import_operator1);

    // Filter by l_discount.
    const auto predicate2 = std::make_shared<BetweenExpression>(
        PredicateCondition::kBetweenInclusive, PqpColumn_(2, DataType::kFloat, false, "l_discount"),
        Value_(kTpchQ6Discount - 0.01f), Value_(kTpchQ6Discount + 0.01f));
    std::shared_ptr<FilterOperatorProxy> filter_operator2 = std::make_shared<FilterOperatorProxy>(predicate2);
    filter_operator2->SetLeftInput(filter_operator1);

    // Filter by l_quantity.
    const auto predicate3 = std::make_shared<BinaryPredicateExpression>(
        PredicateCondition::kLessThan, PqpColumn_(0, DataType::kFloat, false, "l_quantity"), Value_(kTpchQ6Quantity));
    std::shared_ptr<FilterOperatorProxy> filter_operator3 = std::make_shared<FilterOperatorProxy>(predicate3);
    filter_operator3->SetLeftInput(filter_operator2);

    // Projection
    const std::vector<std::shared_ptr<AbstractExpression>> expressions{
        Mul_(PqpColumn_(1, DataType::kFloat, false, "l_extendedprice"),
             PqpColumn_(2, DataType::kFloat, false, "l_discount"))};
    std::shared_ptr<ProjectionOperatorProxy> projection_operator =
        std::make_shared<ProjectionOperatorProxy>(expressions);
    projection_operator->SetLeftInput(filter_operator3);

    // Aggregate: SUM(l_extendedprice * l_discount)
    std::vector<std::shared_ptr<AbstractExpression>> aggregates1 = {
        Sum_(PqpColumn_(0, DataType::kFloat, false, "revenue"))};
    const std::vector<ColumnId> groupby_column_ids1{};
    std::shared_ptr<AggregateOperatorProxy> aggregate_operator1 =
        std::make_shared<AggregateOperatorProxy>(groupby_column_ids1, aggregates1);
    aggregate_operator1->SetLeftInput(projection_operator);

    // Export
    std::shared_ptr<ExportOperatorProxy> export_operator1 =
        std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateFormat);
    export_operator1->SetLeftInput(aggregate_operator1);

    // ======================= STAGE 2 =======================
    std::shared_ptr<ImportOperatorProxy> import_operator2 =
        std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0});
    import_operator2->SetIdentity(import_identity_);

    // Aggregate: SUM(l_extendedprice * l_discount)
    std::vector<std::shared_ptr<AbstractExpression>> aggregates2 = {std::make_shared<AggregateExpression>(
        AggregateFunction::kSum, PqpColumn_(0, DataType::kDouble, false, "revenue"))};
    const std::vector<ColumnId> groupby_column_ids2{};
    std::shared_ptr<AggregateOperatorProxy> aggregate_operator2 =
        std::make_shared<AggregateOperatorProxy>(groupby_column_ids2, aggregates2);
    aggregate_operator2->SetLeftInput(import_operator2);

    // Alias: SUM(l_extendedprice * l_discount) => revenue
    const std::vector<ColumnId>& column_ids_alias = {0};
    const std::vector<std::string>& aliases = {"revenue"};
    std::shared_ptr<AliasOperatorProxy> alias_operator2 =
        std::make_shared<AliasOperatorProxy>(column_ids_alias, aliases);
    alias_operator2->SetLeftInput(aggregate_operator2);

    // Export
    std::shared_ptr<ExportOperatorProxy> export_operator2 =
        std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), ExportFormat::kCsv);
    export_operator2->SetLeftInput(alias_operator2);

    // ======================= PQP Build =======================
    auto input_objects = ListTpchTableObjects(sf, kIntermediateFormat, "lineitem");
    auto target_objects = GetRandomFiles(input_objects.size(), intermediate_suffix);
    const auto pipeline_a = GeneratePipeline(export_operator1, input_objects, target_objects, kIntermediateFormat, "a");

    input_objects = target_objects;
    target_objects = GetRandomFiles(1, ".csv");
    const auto pipeline_b = GeneratePipeline(export_operator2, input_objects, target_objects, ExportFormat::kCsv, "b");
    pipeline_a->SetAsPredecessorOf(pipeline_b);

    return {{pipeline_a, pipeline_b}, kTpchQ6ExpectedResult};
  }

 private:
  std::vector<ObjectReference> ListTpchTableObjects(const ScaleFactor sf, const ExportFormat format,
                                                    const std::string table_name) {
    const std::string bucket = "benchmark-data-sets";

    const std::string format_path = GetFormatName(format);
    const std::string sf_path = std::to_string(sf);

    const std::string file_path =
        std::string("tpc-h/standard/") + format_path + "/sf" + sf_path + "/" + table_name + "/";

    S3Storage storage(client_->GetS3Client(), bucket);
    const auto outcome = storage.List(file_path);
    Assert(!outcome.second.IsError(), outcome.second.GetMessage());

    std::vector<ObjectReference> files;
    files.reserve(outcome.first.size());
    for (const auto& object : outcome.first) {
      files.emplace_back(bucket, object.GetIdentifier(), object.GetChecksum());
    }
    return files;
  }

  std::vector<ObjectReference> GetRandomFiles(size_t n, std::string format = ".orc") {
    const std::string directory = RandomString(8);
    std::vector<ObjectReference> files;
    files.reserve(n);
    while (n-- > 0) {
      const std::string identifier = output_directory_.identifier + RandomString(8) + format;
      files.emplace_back(output_directory_.bucket_name, identifier);
    }
    return files;
  }

  std::shared_ptr<PqpPipeline> GeneratePipeline(std::shared_ptr<AbstractOperatorProxy> pqp,
                                                std::vector<ObjectReference> input_objects,
                                                std::vector<ObjectReference> target_objects, ExportFormat export_format,
                                                const std::string& pipeline_id) {
    std::vector<std::unordered_map<std::string, std::vector<ObjectReference>>> worker_input_mappings;
    worker_input_mappings.resize(target_objects.size());

    for (size_t i = 0; i < input_objects.size(); ++i) {
      worker_input_mappings[i % worker_input_mappings.size()][pipeline_id + import_identity_].push_back(
          input_objects[i]);
    }

    auto pipeline = std::make_shared<PqpPipeline>(pipeline_id, pqp);
    for (size_t i = 0; i < target_objects.size(); ++i) {
      const PipelineFragmentDefinition fragment(worker_input_mappings[i], target_objects[i], export_format);
      pipeline->AddFragmentDefinition(fragment);
    }

    return pipeline;
  }

  const std::shared_ptr<BaseClient> client_;
  const ObjectReference output_directory_;
  const std::string import_identity_ = "import_operator";

 private:
  static constexpr ExportFormat kIntermediateFormat = ExportFormat::kParquet;
};

}  // namespace skyrise
