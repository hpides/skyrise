#include "utils/benchmark_helper.hpp"

#include <chrono>
#include <cmath>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/lambda/model/InvokeRequest.h>

#include "benchmark_config.hpp"
#include "gtest/gtest.h"

namespace skyrise {

class BenchmarkHelperTest : public ::testing::Test {};

TEST_F(BenchmarkHelperTest, CalculateAggregates) {
  Aws::SDKOptions options;

  Aws::InitAPI(options);
  {
    const auto begin = std::chrono::steady_clock::now();
    const auto benchmark_results = std::make_shared<std::vector<BenchmarkItemResult>>();
    const Aws::Lambda::Model::InvokeRequest invoke_request;
    const auto end = std::chrono::steady_clock::now();

    const size_t num_benchmark_item_results = 100'000;
    const double average = (num_benchmark_item_results - 1) / 2.0;

    for (size_t i = 0; i < num_benchmark_item_results; i++) {
      benchmark_results->emplace_back(
          // Instead of creating a shared_ptr<InvokeResult>, we set it to nullptr and use the SQS message instead
          BenchmarkItemResult{"", invoke_request, true, begin, end, nullptr, std::to_string(i)});
    }

    const auto aggregates = BenchmarkHelper::CalculateAggregates(
        benchmark_results, [](const BenchmarkItemResult& b) { return std::stod(b.sqs_message_body); });

    double variance = 0;
    for (size_t i = 0; i < num_benchmark_item_results; i++) {
      variance += std::pow(i - average, 2);
    }

    variance /= num_benchmark_item_results;
    const double std_dev = std::sqrt(variance);

    EXPECT_EQ(aggregates.minimum, 0.0);
    EXPECT_EQ(aggregates.maximum, 99'999.0);
    EXPECT_EQ(aggregates.average, 49'999.5);
    EXPECT_EQ(aggregates.median, 49'999.5);
    EXPECT_EQ(aggregates.percentile_90, 90'000.0);
    EXPECT_EQ(aggregates.percentile_99, 99'000.0);
    EXPECT_EQ(aggregates.percentile_99_9, 99'900.0);
    EXPECT_EQ(aggregates.percentile_99_99, 99'990.0);
    EXPECT_EQ(aggregates.standard_deviation, std_dev);
  }
  Aws::ShutdownAPI(options);
}

TEST_F(BenchmarkHelperTest, GenerateJsonOutput) {
  Aws::SDKOptions options;

  Aws::InitAPI(options);
  {
    const auto begin = std::chrono::steady_clock::now();
    const auto benchmark_results = std::make_shared<std::vector<BenchmarkItemResult>>();
    const Aws::Lambda::Model::InvokeRequest invoke_request;
    const auto end = std::chrono::steady_clock::now();

    for (size_t i = 0; i < 3; i++) {
      benchmark_results->emplace_back(
          BenchmarkItemResult{"", invoke_request, true, begin, end, nullptr, std::to_string(i)});
    }

    std::vector<std::tuple<Aws::String, double>> aggregated_numeric_metrics;
    aggregated_numeric_metrics.emplace_back(std::make_tuple("numeric_metric", 0.0));

    std::vector<std::tuple<Aws::String, Aws::String>> aggregated_alphabetic_metrics;
    aggregated_alphabetic_metrics.emplace_back(std::make_tuple("alphabetic_metric", "zero"));

    std::vector<std::function<std::tuple<Aws::String, double>(const BenchmarkItemResult&)>>
        extract_numeric_metric_functions{[](const BenchmarkItemResult& b) {
                                           return std::make_tuple("numeric_metric_1", std::stod(b.sqs_message_body));
                                         },
                                         [](const BenchmarkItemResult& b) {
                                           return std::make_tuple("numeric_metric_2", static_cast<double>(b.success));
                                         }};

    std::vector<std::function<std::tuple<Aws::String, Aws::String>(const BenchmarkItemResult&)>>
        extract_alphabetic_metric_functions{
            [](const BenchmarkItemResult& b) { return std::make_tuple("alphabetic_metric_1", b.sqs_message_body); },
            [](const BenchmarkItemResult& b) {
              return std::make_tuple("alphabetic_metric_2", b.success ? "true" : "false");
            }};

    const auto json_value = BenchmarkHelper::GenerateJsonOutput(
        "benchmark", aggregated_numeric_metrics, aggregated_alphabetic_metrics, benchmark_results,
        extract_numeric_metric_functions, extract_alphabetic_metric_functions);
    const auto json_view = json_value.View();
    EXPECT_EQ(
        json_view.WriteCompact(),
        "{\"name\":\"benchmark\",\"numeric_metric\":0,\"alphabetic_metric\":\"zero\",\"runs\":[{\"name\":\"benchmark/"
        "0\",\"numeric_metric_1\":0,\"numeric_metric_2\":1,\"alphabetic_metric_1\":\"0\",\"alphabetic_metric_2\":"
        "\"true\"},{"
        "\"name\":\"benchmark/"
        "1\",\"numeric_metric_1\":1,\"numeric_metric_2\":1,\"alphabetic_metric_1\":\"1\",\"alphabetic_metric_2\":"
        "\"true\"},{"
        "\"name\":\"benchmark/"
        "2\",\"numeric_metric_1\":2,\"numeric_metric_2\":1,\"alphabetic_metric_1\":\"2\",\"alphabetic_metric_2\":"
        "\"true\"}]}");
  }
  Aws::ShutdownAPI(options);
}

}  // namespace skyrise
