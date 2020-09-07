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
  const auto begin = std::chrono::steady_clock::now();
  const auto benchmark_results = std::make_shared<std::vector<BenchmarkItemResult>>();
  const Aws::Lambda::Model::InvokeRequest invoke_request;
  const auto end = std::chrono::steady_clock::now();

  const size_t num_benchmark_item_results = 100'000;
  const double average = (num_benchmark_item_results - 1) / 2.0;

  for (size_t i = 0; i < num_benchmark_item_results; i++) {
    benchmark_results->emplace_back(
        BenchmarkItemResult{"", invoke_request, true, begin, end, nullptr, std::to_string(i)});
  }

  BenchmarkHelper benchmark_helper;
  const auto aggregates = benchmark_helper.CalculateAggregates(
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

TEST_F(BenchmarkHelperTest, GenerateJsonOutput) {
  const auto begin = std::chrono::steady_clock::now();
  const auto benchmark_results = std::make_shared<std::vector<BenchmarkItemResult>>();
  const Aws::Lambda::Model::InvokeRequest invoke_request;
  const auto end = std::chrono::steady_clock::now();

  for (size_t i = 0; i < 3; i++) {
    benchmark_results->emplace_back(
        BenchmarkItemResult{"", invoke_request, true, begin, end, nullptr, std::to_string(i)});
  }

  std::vector<std::tuple<Aws::String, double>> aggregated_metrics;
  aggregated_metrics.emplace_back(std::make_tuple("metric", 0.0));

  std::vector<std::function<std::tuple<Aws::String, double>(const BenchmarkItemResult&)>> extract_metric_functions{
      [](const BenchmarkItemResult& b) { return std::make_tuple("metric_1", std::stod(b.sqs_message_body)); },
      [](const BenchmarkItemResult& b) { return std::make_tuple("metric_2", static_cast<double>(b.success)); }};

  BenchmarkHelper benchmark_helper;
  const auto json_value =
      benchmark_helper.GenerateJsonOutput("benchmark", aggregated_metrics, benchmark_results, extract_metric_functions);
  const auto json_view = json_value.View();
  EXPECT_EQ(json_view.WriteCompact(),
            "{\"name\":\"benchmark\",\"metric\":0,\"runs\":[{\"name\":\"benchmark/"
            "0\",\"metric_1\":0,\"metric_2\":1},{\"name\":\"benchmark/"
            "1\",\"metric_1\":1,\"metric_2\":1},{\"name\":\"benchmark/2\",\"metric_1\":2,\"metric_2\":1}]}");
}

}  // namespace skyrise
