#include "benchmark_helper.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iterator>
#include <numeric>
#include <regex>

#include <aws/core/utils/base64/Base64.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "utils/assert.hpp"
#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

// TODO(anyone): Add Git commit hash to logging tag
const std::string kTag = "SKYRISE/BENCHMARK/BENCHMARK_HELPER";

// TODO(anyone): Split this into a) CreateJsonOutput, b) AddDoubleMetrics, and c) AddStringMetrics once it gets moved to
// the abstract Benchmark class
Aws::Utils::Json::JsonValue BenchmarkHelper::GenerateJsonOutput(
    const Aws::String& benchmark_name, const std::vector<std::tuple<Aws::String, double>>& aggregated_numeric_metrics,
    const std::vector<std::tuple<Aws::String, Aws::String>>& aggregated_alphabetic_metrics,
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
    const std::vector<std::function<std::tuple<Aws::String, double>(const BenchmarkItemResult&)>>&
        extract_numeric_metric_functions,
    const std::vector<std::function<std::tuple<Aws::String, Aws::String>(const BenchmarkItemResult&)>>&
        extract_alphabetic_metric_functions) {
  auto json_output = Aws::Utils::Json::JsonValue().WithString("name", benchmark_name);

  for (const auto& [metric_name, aggregated_numeric_metric] : aggregated_numeric_metrics) {
    json_output = json_output.WithDouble(metric_name, aggregated_numeric_metric);
  }

  for (const auto& [metric_name, aggregated_alphabetic_metric] : aggregated_alphabetic_metrics) {
    json_output = json_output.WithString(metric_name, aggregated_alphabetic_metric);
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_runs(benchmark_result->size());

  for (size_t i = 0; i < benchmark_result->size(); i++) {
    auto benchmark_run_value =
        Aws::Utils::Json::JsonValue().WithString("name", benchmark_name + "/" + std::to_string(i));

    for (const auto& extract_numeric_metric_function : extract_numeric_metric_functions) {
      const auto& [metric_name, numeric_metric] = extract_numeric_metric_function(benchmark_result->at(i));
      benchmark_run_value = benchmark_run_value.WithDouble(metric_name, numeric_metric);
    }

    for (const auto& extract_alphabetic_metric_function : extract_alphabetic_metric_functions) {
      const auto& [metric_name, alphabetic_metric] = extract_alphabetic_metric_function(benchmark_result->at(i));
      benchmark_run_value = benchmark_run_value.WithString(metric_name, alphabetic_metric);
    }

    benchmark_runs[i] = benchmark_run_value;
  }

  json_output = json_output.WithArray("runs", benchmark_runs);

  return json_output;
}

long double BenchmarkHelper::CreateS3BucketIfNotExists(const Aws::String& bucket_name) const {
  const auto& s3_client = client_aws_->GetS3Client();

  const auto list_buckets_outcome = s3_client.ListBuckets();

  const long double cost = cost_calculator_.CalculateCostS3Requests(1, 0);

  if (!list_buckets_outcome.IsSuccess()) {
    Fail(list_buckets_outcome.GetError().GetMessage());
  }

  const auto& buckets = list_buckets_outcome.GetResult().GetBuckets();

  const auto contains_bucket_iterator = std::find_if(
      buckets.cbegin(), buckets.cend(), [&](const auto& bucket) { return bucket.GetName() == bucket_name; });

  if (contains_bucket_iterator == buckets.cend()) {
    const auto create_bucket_outcome =
        s3_client.CreateBucket(Aws::S3::Model::CreateBucketRequest().WithBucket(bucket_name));

    if (!create_bucket_outcome.IsSuccess()) {
      Fail(create_bucket_outcome.GetError().GetMessage());
    }
  }

  return cost;
}

std::shared_ptr<Aws::IOStream> BenchmarkHelper::GenerateRandomObject(const size_t num_bytes) {
  return std::make_shared<Aws::StringStream>(RandomString(num_bytes));
}

long double BenchmarkHelper::UploadObjectToS3(const Aws::String& object_key,
                                              const std::shared_ptr<Aws::IOStream>& object_value,
                                              const size_t object_byte_size, const Aws::String& bucket_name) const {
  return UploadObjectsToS3Parallel({{object_key, object_value, object_byte_size}}, bucket_name);
}

long double BenchmarkHelper::UploadObjectsToS3Parallel(
    const std::vector<std::tuple<Aws::String, std::shared_ptr<Aws::IOStream>, size_t>>& objects,
    const Aws::String& bucket_name) const {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Uploading objects to S3...");

  const auto& s3_client = client_aws_->GetS3Client();

  std::vector<Aws::S3::Model::PutObjectOutcomeCallable> callables;
  callables.reserve(objects.size());

  size_t num_bytes_total = 0;

  // TODO(anyone): Introduce a client-side thread pool
  for (const auto& [object_key, object, num_bytes] : objects) {
    auto put_object_request = Aws::S3::Model::PutObjectRequest().WithBucket(bucket_name).WithKey(object_key);
    put_object_request.SetBody(object);
    callables.emplace_back(s3_client.PutObjectCallable(put_object_request));
    num_bytes_total += num_bytes;
  }

  size_t num_errors = 0;

  for (size_t i = 0; i < callables.size(); i++) {
    const auto& outcome = callables[i].get();

    if (!outcome.IsSuccess()) {
      AWS_LOGSTREAM_ERROR(kTag.c_str(), outcome.GetError().GetExceptionName()
                                            << ": " << outcome.GetError().GetMessage());
      num_errors++;
    } else {
      AWS_LOGSTREAM_INFO(kTag.c_str(), std::get<0>(objects[i]) << " was uploaded successfully to S3.");
    }
  }

  if (num_errors > 0) {
    Fail(std::to_string(num_errors) + " errors during multi-threaded upload to S3.");
  }

  // TODO(d-justen): Find a way to track actual hours. For now, we assume that S3 objects will be deleted within an
  // hour.
  const long double storage_cost = cost_calculator_.CalculateCostS3StorageMonthly(num_bytes_total, 1);
  const long double request_cost = cost_calculator_.CalculateCostS3Requests(callables.size(), 0);

  return storage_cost + request_cost;
}

long double BenchmarkHelper::EmptyS3Bucket(const Aws::String& bucket_name) const {
  const auto& s3_client = client_aws_->GetS3Client();

  const auto list_objects_outcome = s3_client.ListObjects(Aws::S3::Model::ListObjectsRequest().WithBucket(bucket_name));

  const auto cost = cost_calculator_.CalculateCostS3Requests(1, 0);

  if (!list_objects_outcome.IsSuccess()) {
    Fail(list_objects_outcome.GetError().GetMessage());
  }

  const auto& listed_objects = list_objects_outcome.GetResult().GetContents();

  if (listed_objects.empty()) {
    return cost;
  }

  Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects_to_delete;

  std::transform(listed_objects.cbegin(), listed_objects.cend(), std::back_inserter(objects_to_delete),
                 [](const auto& object) { return Aws::S3::Model::ObjectIdentifier().WithKey(object.GetKey()); });
  const auto delete_objects = Aws::S3::Model::Delete().WithObjects(objects_to_delete);

  const auto delete_objects_outcome = s3_client.DeleteObjects(
      Aws::S3::Model::DeleteObjectsRequest().WithBucket(bucket_name).WithDelete(delete_objects));

  if (!delete_objects_outcome.IsSuccess()) {
    Fail(list_objects_outcome.GetError().GetMessage());
  }

  return cost;
}

std::vector<double> BenchmarkHelper::ExtractMetrics(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
    const std::function<double(const BenchmarkItemResult&)>& extract_metric) {
  std::vector<double> metrics;
  metrics.reserve(benchmark_result->size());

  std::transform(benchmark_result->cbegin(), benchmark_result->cend(), std::back_inserter(metrics),
                 [&](const BenchmarkItemResult& result) { return extract_metric(result); });

  return metrics;
}

double BenchmarkHelper::ExtractMetric(const BenchmarkItemResult& result, const Aws::String& key) {
  const auto payload_value = Aws::Utils::Json::JsonValue(StreamToString(&result.invoke_result->GetPayload()));
  const auto payload_view = payload_value.View();

  return payload_view.GetDouble(key);
}

double BenchmarkHelper::ExtractBilledLambdaDuration(const BenchmarkItemResult& result) {
  Aws::Utils::Base64::Base64 base64;
  const Aws::Utils::ByteBuffer log_result_chars = base64.Decode(result.invoke_result->GetLogResult());

  const unsigned char* data = log_result_chars.GetUnderlyingData();
  std::string log_result(reinterpret_cast<char const*>(data), log_result_chars.GetLength());

  const std::regex billing_regex("REPORT.+Billed Duration: (\\d+)");
  std::smatch billing_match;
  std::regex_search(log_result, billing_match, billing_regex);

  return std::stod(billing_match[1]);
}

}  // namespace skyrise
