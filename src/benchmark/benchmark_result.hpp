#pragma once

#include <string>
#include <vector>

namespace skyrise {

struct BenchmarkResult {
  std::string name;
  std::vector<double> latencies;
  double mean;
  double median;
  double p95;
  double p99;
};

}  // namespace skyrise 