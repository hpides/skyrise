/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 *  - See tpch_benchmark_item_runner.hpp/cpp
 */
#pragma once

#include <atomic>
#include <string>

#include <boost/date_time/gregorian/gregorian.hpp>

#include "types.hpp"

namespace skyrise {

using BenchmarkItemId = size_t;

class TpchQueryGenerator : public Noncopyable {
 public:
  explicit TpchQueryGenerator(float scale_factor = 1.0);

  /**
   * @returns a SQL query with random parameters for a given (zero-indexed) benchmark item (i.e., 0 -> TPC-H 1)
   */
  std::string BuildQuery(BenchmarkItemId item_id);

  /**
   * Same as build_query, but uses the same parameters every time. Good for tests.
   */
  static std::string BuildDeterministicQuery(BenchmarkItemId item_id);

 private:
  const float kScaleFactor;

  // Used for naming the views generated in query 15.
  std::atomic_size_t q15_view_id_{0};

  // We want deterministic seeds, but since the engine is thread-local, we need to make sure that each thread has its
  // own seed.
  std::atomic_uint32_t random_seed_{0};
};

}  // namespace skyrise
