#pragma once

#include <functional>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark_runner.hpp"
#include "client/client_aws.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class BenchmarkHelper {
 public:
  BenchmarkHelper(std::shared_ptr<ClientAws> client_aws) : client_aws_(client_aws), cost_calculator_(client_aws) {}

  static Aws::Utils::Json::JsonValue GenerateJsonOutput(
      const Aws::String& benchmark_name, const std::vector<std::tuple<Aws::String, double>>& aggregated_numeric_metrics,
      const std::vector<std::tuple<Aws::String, Aws::String>>& aggregated_alphabetic_metrics,
      const std::shared_ptr<BenchmarkResult>& benchmark_result,
      const std::vector<std::function<std::tuple<Aws::String, double>(const InvocationResult&)>>&
          extract_numeric_metric_functions,
      const std::vector<std::function<std::tuple<Aws::String, Aws::String>(const InvocationResult&)>>&
          extract_alphabetic_metric_functions);

  long double CreateS3BucketIfNotExists(const Aws::String& bucket_name) const;
  static std::shared_ptr<Aws::IOStream> GenerateRandomObject(const size_t num_bytes);
  long double UploadObjectToS3(const Aws::String& object_key, const std::shared_ptr<Aws::IOStream>& object_value,
                               const size_t object_byte_size, const Aws::String& bucket_name) const;
  long double UploadObjectsToS3Parallel(
      const std::vector<std::tuple<Aws::String, std::shared_ptr<Aws::IOStream>, size_t>>& objects,
      const Aws::String& bucket_name) const;
  long double EmptyS3Bucket(const Aws::String& bucket_name) const;

  static std::vector<double> ExtractMetrics(const std::shared_ptr<BenchmarkResult>& benchmark_result,
                                            const std::function<double(const InvocationResult&)>& extract_metric);
  static std::vector<double> ExtractMetrics(const std::map<Aws::String, InvocationResult>& repetition_results,
                                            const std::function<double(const InvocationResult&)>& extract_metric);
  static double ExtractMetric(const InvocationResult& result, const Aws::String& key);
  static double ExtractBilledLambdaDuration(const InvocationResult& result);

 private:
  const std::shared_ptr<ClientAws> client_aws_;
  const CostCalculator cost_calculator_;
};

}  // namespace skyrise
