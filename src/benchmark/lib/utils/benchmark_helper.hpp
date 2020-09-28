#pragma once

#include <functional>
#include <memory>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/s3/S3Client.h>

#include "benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

struct BenchmarkAggregates {
  double minimum;
  double maximum;
  double average;
  double median;
  double percentile_90;
  double percentile_99;
  double percentile_99_9;
  double percentile_99_99;
  double standard_deviation;
};

class BenchmarkHelper {
 public:
  // TODO(anyone): Centralize AWS client creation and configuration and reuse clients here
  BenchmarkHelper(const bool use_sdk = false);
  static BenchmarkAggregates CalculateAggregates(
      const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
      const std::function<double(const BenchmarkItemResult&)>& extract_metric);
  static BenchmarkAggregates CalculateAggregates(std::vector<double>& metrics);
  static Aws::Utils::Json::JsonValue GenerateJsonOutput(
      const Aws::String& benchmark_name, const std::vector<std::tuple<Aws::String, double>>& aggregated_metrics,
      const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
      const std::vector<std::function<std::tuple<Aws::String, double>(const BenchmarkItemResult&)>>& metrics);

  double CreateS3BucketIfNotExists(const Aws::String& bucket_name);
  static std::shared_ptr<Aws::IOStream> GenerateRandomObject(const size_t num_bytes);
  long double UploadObjectToS3Bucket(const Aws::String& bucket_name, const Aws::String& object_key,
                                     const std::shared_ptr<Aws::IOStream>& object, const size_t num_bytes);
  double EmptyS3Bucket(const Aws::String& bucket_name);

  static double ExtractMetric(const BenchmarkItemResult& result, const Aws::String& key);
  static double ExtractBilledLambdaDuration(const BenchmarkItemResult& result);

 private:
  std::shared_ptr<CostCalculator> cost_calculator_;
  Aws::S3::S3Client s3_client_;
};

}  // namespace skyrise
