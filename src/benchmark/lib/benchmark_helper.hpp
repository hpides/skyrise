#pragma once

#include <functional>
#include <optional>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark_runner.hpp"
#include "client/client.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class BenchmarkHelper {
 public:
  BenchmarkHelper(std::shared_ptr<Client> client) : client_(client), cost_calculator_(client) {}

  static Aws::Utils::Json::JsonValue GenerateJsonOutput(
      const Aws::String& benchmark_name, const std::vector<std::tuple<Aws::String, double>>& aggregated_numeric_metrics,
      const std::vector<std::tuple<Aws::String, Aws::String>>& aggregated_alphabetic_metrics,
      const std::shared_ptr<BenchmarkResult>& benchmark_result,
      const std::vector<std::function<std::tuple<Aws::String, double>(const InvokeResult&)>>&
          extract_numeric_metric_functions,
      const std::vector<std::function<std::tuple<Aws::String, Aws::String>(const InvokeResult&)>>&
          extract_alphabetic_metric_functions,
      const std::vector<std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const InvokeResult&)>>&
          extract_object_metric_functions);

  long double CreateS3BucketIfNotExists(const Aws::String& bucket_name) const;
  static std::shared_ptr<Aws::IOStream> GenerateRandomObject(const size_t num_bytes);
  long double UploadObjectToS3(const Aws::String& object_key, const std::shared_ptr<Aws::IOStream>& object_value,
                               const size_t object_byte_size, const Aws::String& bucket_name) const;
  long double UploadObjectsToS3Parallel(
      const std::vector<std::tuple<Aws::String, std::shared_ptr<Aws::IOStream>, size_t>>& objects,
      const Aws::String& bucket_name) const;
  long double EmptyS3Bucket(const Aws::String& bucket_name) const;
  // All objects in an S3 bucket must be deleted before the bucket can be deleted
  long double EmptyAndDeleteS3Bucket(const Aws::String& bucket_name) const;

 private:
  const std::shared_ptr<Client> client_;
  const CostCalculator cost_calculator_;
};

}  // namespace skyrise
