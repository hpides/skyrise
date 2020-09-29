#include "benchmark_helper.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iterator>
#include <numeric>
#include <random>
#include <regex>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/base64/Base64.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

BenchmarkHelper::BenchmarkHelper(const bool use_sdk) {
  if (use_sdk) {
    const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();

    // TODO(anyone): Improve error handling; below AWS SDK call just checks for presence of AWS_ACCESS_KEY_ID
    if (credentials_provider == nullptr || (*credentials_provider).GetAWSCredentials().IsEmpty()) {
      std::cout << "ERROR: AWS credentials are missing. Please export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.\n";
      exit(1);
    }

    Aws::Client::ClientConfiguration client_config;
    client_config.caFile = "/etc/pki/tls/certs/ca-bundle.crt";

    if (!std::ifstream(client_config.caFile).good()) {
      std::cout << "ERROR: AWS certificates are missing. Please provide caFile.\n";
      exit(1);
    }

    s3_client_ = Aws::S3::S3Client(credentials_provider, client_config);
    cost_calculator_ = std::make_shared<CostCalculator>(std::make_shared<Pricing>(client_config.region));
  }
}

BenchmarkAggregates BenchmarkHelper::CalculateAggregates(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
    const std::function<double(const BenchmarkItemResult&)>& extract_metric) {
  std::vector<double> metrics;
  std::transform(benchmark_result->cbegin(), benchmark_result->cend(), std::back_inserter(metrics),
                 [&](const BenchmarkItemResult& result) { return extract_metric(result); });

  return CalculateAggregates(metrics);
}

BenchmarkAggregates BenchmarkHelper::CalculateAggregates(std::vector<double>& metrics) {
  if (metrics.empty()) {
    return {};
  }

  std::sort(metrics.begin(), metrics.end());

  const double minimum = metrics.front();
  const double maximum = metrics.back();
  const double average = std::accumulate(metrics.cbegin(), metrics.cend(), 0.0) / metrics.size();

  const double median = metrics.size() % 2 == 0 ? (metrics[metrics.size() / 2 - 1] + metrics[metrics.size() / 2]) / 2
                                                : metrics[(metrics.size() / 2)];
  const double percentile_90 = metrics[static_cast<size_t>(metrics.size() * 0.9)];
  const double percentile_99 = metrics[static_cast<size_t>(metrics.size() * 0.99)];
  const double percentile_99_9 = metrics[static_cast<size_t>(metrics.size() * 0.999)];
  const double percentile_99_99 = metrics[static_cast<size_t>(metrics.size() * 0.9999)];

  const double variance = std::accumulate(metrics.cbegin(), metrics.cend(), 0.0,
                                          [&](double a, double b) { return a + std::pow(b - average, 2); }) /
                          static_cast<double>(metrics.size());
  const double standard_deviation = std::sqrt(variance);

  return {minimum,         maximum,          average,           median, percentile_90, percentile_99,
          percentile_99_9, percentile_99_99, standard_deviation};
}

Aws::Utils::Json::JsonValue BenchmarkHelper::GenerateJsonOutput(
    const Aws::String& benchmark_name, const std::vector<std::tuple<Aws::String, double>>& aggregated_metrics,
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
    const std::vector<std::function<std::tuple<Aws::String, double>(const BenchmarkItemResult&)>>& metrics) {
  auto json_output = Aws::Utils::Json::JsonValue().WithString("name", benchmark_name);

  for (const auto& [metric_name, metric] : aggregated_metrics) {
    json_output = json_output.WithDouble(metric_name, metric);
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_runs(benchmark_result->size());

  for (size_t i = 0; i < benchmark_result->size(); i++) {
    auto benchmark_run_value =
        Aws::Utils::Json::JsonValue().WithString("name", benchmark_name + "/" + std::to_string(i));

    for (const auto& extract_metric : metrics) {
      const auto& [metric_name, metric] = extract_metric(benchmark_result->at(i));
      benchmark_run_value = benchmark_run_value.WithDouble(metric_name, metric);
    }

    benchmark_runs[i] = benchmark_run_value;
  }

  json_output = json_output.WithArray("runs", benchmark_runs);

  return json_output;
}

double BenchmarkHelper::CreateS3BucketIfNotExists(const Aws::String& bucket_name) {
  const auto list_buckets_outcome = s3_client_.ListBuckets();

  const auto cost = cost_calculator_->CalculateCostS3Requests(1, 0);

  if (!list_buckets_outcome.IsSuccess()) {
    // TODO(anyone): Align with to-be-defined error handling convention
    std::cout << list_buckets_outcome.GetError().GetMessage() << "\n";
    return cost;
  }

  const auto& buckets = list_buckets_outcome.GetResult().GetBuckets();

  const auto contains_bucket_iterator = std::find_if(
      buckets.cbegin(), buckets.cend(), [&](const auto& bucket) { return bucket.GetName() == bucket_name; });

  if (contains_bucket_iterator == buckets.cend()) {
    const auto create_bucket_outcome =
        s3_client_.CreateBucket(Aws::S3::Model::CreateBucketRequest().WithBucket(bucket_name));

    if (!create_bucket_outcome.IsSuccess()) {
      // TODO(anyone): Align with to-be-defined error handling convention
      std::cout << create_bucket_outcome.GetError().GetMessage() << "\n";
    }
  }

  return cost;
}

// TODO(anyone): Introduce string utility function for random string generation and use it here
std::shared_ptr<Aws::IOStream> BenchmarkHelper::GenerateRandomObject(const size_t num_bytes) {
  const std::string charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  std::default_random_engine random_number_generator(std::random_device{}());
  std::uniform_int_distribution<> distribution(0, charset.size() - 1);

  std::string random_string(num_bytes, '0');
  std::generate(random_string.begin(), random_string.end(),
                [&]() { return charset[distribution(random_number_generator)]; });

  return std::make_shared<Aws::StringStream>(random_string);
}

long double BenchmarkHelper::UploadObjectToS3Bucket(const Aws::String& bucket_name, const Aws::String& object_key,
                                                    const std::shared_ptr<Aws::IOStream>& object,
                                                    const size_t num_bytes) {
  std::cout << "Uploading " << ByteToMb(num_bytes) << " MB file to S3...\n";

  auto put_object_request = Aws::S3::Model::PutObjectRequest().WithBucket(bucket_name).WithKey(object_key);
  put_object_request.SetBody(object);

  const auto put_object_outcome = s3_client_.PutObject(put_object_request);

  if (!put_object_outcome.IsSuccess()) {
    // TODO(anyone): Align with to-be-defined error handling convention
    std::cout << put_object_outcome.GetError().GetMessage() << "\n";
  }

  std::cout << "File uploaded.\n";

  const long double storage_cost = cost_calculator_->CalculateCostS3StorageMonthly(num_bytes);
  const long double request_cost = cost_calculator_->CalculateCostS3Requests(1, 0);

  return storage_cost + request_cost;
}

double BenchmarkHelper::EmptyS3Bucket(const Aws::String& bucket_name) {
  const auto list_objects_outcome =
      s3_client_.ListObjects(Aws::S3::Model::ListObjectsRequest().WithBucket(bucket_name));

  const auto cost = cost_calculator_->CalculateCostS3Requests(1, 0);

  if (!list_objects_outcome.IsSuccess()) {
    // TODO(anyone): Align with to-be-defined error handling convention
    std::cout << list_objects_outcome.GetError().GetMessage() << "\n";
    return cost;
  }

  const auto& listed_objects = list_objects_outcome.GetResult().GetContents();

  if (listed_objects.empty()) {
    return cost;
  }

  Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects_to_delete;

  std::transform(listed_objects.cbegin(), listed_objects.cend(), std::back_inserter(objects_to_delete),
                 [](const auto& object) { return Aws::S3::Model::ObjectIdentifier().WithKey(object.GetKey()); });
  const auto delete_objects = Aws::S3::Model::Delete().WithObjects(objects_to_delete);

  const auto delete_objects_outcome = s3_client_.DeleteObjects(
      Aws::S3::Model::DeleteObjectsRequest().WithBucket(bucket_name).WithDelete(delete_objects));

  if (!delete_objects_outcome.IsSuccess()) {
    // TODO(anyone): Align with to-be-defined error handling convention
    std::cout << list_objects_outcome.GetError().GetMessage() << "\n";
  }

  return cost;
}

double BenchmarkHelper::ExtractMetric(const BenchmarkItemResult& result, const Aws::String& key) {
  Aws::StringStream payload_stream;
  payload_stream << result.invoke_result->GetPayload().rdbuf();
  result.invoke_result->GetPayload().seekg(std::ios::beg);
  const auto payload_value = Aws::Utils::Json::JsonValue(StreamToString(payload_stream));
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
