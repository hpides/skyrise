#include <aws/lambda/model/InvokeRequest.h>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>

#include "client/coordinator_client.hpp"
#include "compiler/pqp_generator/tpch_pqp_generator.hpp"
#include "function/function_utils.hpp"
#include "scheduler/coordinator/pqp_pipeline_scheduler.hpp"
#include "storage/backend/s3_utils.hpp"
#include "testing/aws_test.hpp"
#include "testing/load_table.hpp"

namespace skyrise {

class AwsSystemBenchmarkTest : public ::testing::Test {
 public:
  void SetUp() override {
    client_ = std::make_shared<CoordinatorClient>();
    coordinator_function_name_ = GetUniqueName(kCoordinatorFunctionName);
    worker_function_name_ = GetUniqueName(kWorkerFunctionName);
    const size_t vcpus_per_worker_function_count = 4;

    UploadFunctions(
        client_->GetIamClient(), client_->GetLambdaClient(),
        std::vector<FunctionConfig>{
            {GetFunctionZipFilePath(kCoordinatorBinaryName), coordinator_function_name_, kLambdaMaximumMemorySizeMb,
             true, false},
            {GetFunctionZipFilePath(kWorkerBinaryName), worker_function_name_,
             static_cast<size_t>(vcpus_per_worker_function_count * kLambdaVcpuEquivalentMemorySizeMb), true, false}},
        false);
  }

  void TearDown() override {
    DeleteFunction(client_->GetLambdaClient(), worker_function_name_);
    DeleteFunction(client_->GetLambdaClient(), coordinator_function_name_);
  }

  ObjectReference RunQuery(const CompilerName& compiler_name, const QueryId& query_id,
                           const ScaleFactor& scale_factor) const {
    auto invoke_request = Aws::Lambda::Model::InvokeRequest()
                              .WithFunctionName(coordinator_function_name_)
                              .WithInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
    Aws::Utils::Json::JsonValue payload =
        Aws::Utils::Json::JsonValue()
            .WithString(kCoordinatorRequestCompilerNameAttribute, std::string(magic_enum::enum_name(compiler_name)))
            .WithString(kCoordinatorRequestQueryPlanAttribute, std::string(magic_enum::enum_name(query_id)))
            .WithString(kCoordinatorRequestScaleFactorAttribute, std::string(magic_enum::enum_name(scale_factor)))
            .WithObject(kCoordinatorRequestShuffleStorageAttribute,
                        ObjectReference(kSkyriseTestBucket, GetUniqueName("SystemBenchmarkTest." +
                                                                          std::string(magic_enum::enum_name(query_id))))
                            .ToJson())
            .WithString(kCoordinatorRequestWorkerFunctionAttribute, worker_function_name_);
    const auto request_payload_stream = std::make_shared<std::stringstream>(payload.View().WriteCompact());
    invoke_request.SetBody(request_payload_stream);

    const auto response = client_->GetLambdaClient()->Invoke(invoke_request);
    EXPECT_TRUE(response.IsSuccess());
    auto& response_payload_stream = response.GetResult().GetPayload();
    auto response_payload = Aws::Utils::Json::JsonValue(response_payload_stream);
    return ObjectReference::FromJson(response_payload.View().GetObject(kCoordinatorResponseResultObjectAttribute));
  }

  bool VerifyResult(const QueryId& query_id, const ScaleFactor& scale_factor,
                    const ObjectReference& query_result) const {
    const auto storage = std::make_shared<S3Storage>(client_->GetS3Client(), query_result.bucket_name);
    const auto format_reader = BuildFormatReader<CsvFormatReader>(storage, query_result.identifier);
    Assert(!format_reader->HasError() && format_reader->HasNext(), "Cannot open query result.");
    const auto chunk = format_reader->Next();
    switch (query_id) {
      case QueryId::kEtlCopyTpchOrders: {
        size_t object_count = 0;
        switch (scale_factor) {
          case ScaleFactor::kSf1: {
            object_count = 1;
          } break;
          case ScaleFactor::kSf10: {
            object_count = 3;
          } break;
          case ScaleFactor::kSf100: {
            object_count = 25;
          } break;
          case ScaleFactor::kSf1000: {
            object_count = 249;
          }
        }
        bool success = true;
        for (size_t i = 0; i < object_count; i++) {
          size_t last_separator_position = query_result.identifier.find_last_of('/');
          std::string query_result_prefix = query_result.identifier.substr(0, last_separator_position);
          success =
              success && ObjectExists(client_->GetS3Client(), query_result_prefix + "/" + std::to_string(i) + ".csv",
                                      query_result.bucket_name);
        }
        return success;
      }
      case QueryId::kTpchQ1: {
        Assert(chunk->GetColumnCount() == 10 && chunk->Size() == 4, "Query result is malformed.");
        // TODO(tobodner): Check results.
        return true;
      } break;
      case QueryId::kTpchQ6: {
        Assert(chunk->GetColumnCount() == 1 && chunk->Size() == 1, "Query result is malformed.");
        const double revenue = std::stod(std::get<std::string>((*chunk->GetSegment(0))[0]));
        const double expected_revenue = 123141078.2283;
        return (revenue >= expected_revenue - 0.25 && revenue <= expected_revenue + 0.25);
      } break;
      case QueryId::kTpchQ12: {
        Assert(chunk->GetColumnCount() == 3 && chunk->Size() == 2, "Query result is malformed.");
        const auto l_shipmode = std::static_pointer_cast<ValueSegment<std::string>>(chunk->GetSegment(0));
        const auto high_line_count = std::static_pointer_cast<ValueSegment<std::string>>(chunk->GetSegment(1));
        const auto low_line_count = std::static_pointer_cast<ValueSegment<std::string>>(chunk->GetSegment(2));
        return l_shipmode->Get(0) == "MAIL" && std::stol(high_line_count->Get(0)) == 6202 &&
               std::stol(low_line_count->Get(0)) == 9324 && l_shipmode->Get(1) == "SHIP" &&
               std::stol(high_line_count->Get(1)) == 6200 && std::stol(low_line_count->Get(1)) == 9262;
      } break;
      default:
        Fail("Cannot verify results.");
    }
  }

 protected:
  const AwsApi aws_api_;
  std::shared_ptr<CoordinatorClient> client_;
  std::string coordinator_function_name_;
  std::string worker_function_name_;
};

TEST_F(AwsSystemBenchmarkTest, EtlCopyTpchOrders) {
  EXPECT_TRUE(VerifyResult(QueryId::kEtlCopyTpchOrders, ScaleFactor::kSf1,
                           RunQuery(CompilerName::kEtl, QueryId::kEtlCopyTpchOrders, ScaleFactor::kSf1)));
}

TEST_F(AwsSystemBenchmarkTest, TpchQ6) {
  EXPECT_TRUE(VerifyResult(QueryId::kTpchQ6, ScaleFactor::kSf1,
                           RunQuery(CompilerName::kTpch, QueryId::kTpchQ6, ScaleFactor::kSf1)));
}

TEST_F(AwsSystemBenchmarkTest, TpchQ12) {
  EXPECT_TRUE(VerifyResult(QueryId::kTpchQ12, ScaleFactor::kSf1,
                           RunQuery(CompilerName::kTpch, QueryId::kTpchQ12, ScaleFactor::kSf1)));
}

}  // namespace skyrise
