#include <numeric>
#include <random>
#include <vector>

#include "benchmark/benchmark.h"
#include "operator/hash_join_operator.hpp"
#include "operator/table_wrapper.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"
#include "types.hpp"
#include "utils/random.hpp"

namespace skyrise {

/**
 * The hash join operator is evaluated for each of its join modes (i.e., Inner, LeftOuter, RightOuter, and FullOuter).
 * The steps to register the microbenchmark variants are repetitive and differ only in two aspects: To avoid code
 * duplication for defining the benchmark case and specifying repetitions, range multipliers and the range, this macro
 * can be used.
 */
#define REGISTER_HASH_JOIN_OPERATOR_MICROBENCHMARK(MicrobenchmarkName, JoinMode)                                     \
  BENCHMARK_DEFINE_F(HashJoinOperatorMicrobenchmarkFixture, MicrobenchmarkName)(benchmark::State & state) {          \
    const auto predicate =                                                                                           \
        std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{0, 0, PredicateCondition::kEquals});           \
    auto hash_join_operator =                                                                                        \
        std::make_shared<skyrise::HashJoinOperator>(table_wrapper_left_, table_wrapper_right_, predicate, JoinMode); \
    for (auto _ : state) {                                                                                           \
      hash_join_operator->Execute();                                                                                 \
    }                                                                                                                \
    state.SetBytesProcessed(table_data_byte_size_);                                                                  \
  }                                                                                                                  \
  BENCHMARK_REGISTER_F(HashJoinOperatorMicrobenchmarkFixture, MicrobenchmarkName)                                    \
      ->Repetitions(5)                                                                                               \
      ->RangeMultiplier(2)                                                                                           \
      ->Range(1 << 20, 1 << 24);  // NOLINT(hicpp-signed-bitwise)

class HashJoinOperatorMicrobenchmarkFixture : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State& state) override {
    const auto table_left = CreateTableWithRandomizedInts(state.range(0) / 2, 0, "a");
    const auto table_right = CreateTableWithRandomizedInts(state.range(0), state.range(0) / 4, "b");

    table_data_byte_size_ = table_left->MemoryUsageBytes() + table_right->MemoryUsageBytes();

    table_wrapper_left_ = std::make_shared<TableWrapper>(table_left);
    table_wrapper_left_->Execute();

    table_wrapper_right_ = std::make_shared<TableWrapper>(table_right);
    table_wrapper_right_->Execute();
  }

  static std::shared_ptr<Table> CreateTableWithRandomizedInts(const size_t row_count, const size_t start,
                                                              const std::string& column_name) {
    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    std::vector<int> values(row_count);
    std::iota(values.begin(), values.end(), start);
    auto random_generator = RandomGenerator<std::mt19937>();
    std::shuffle(values.begin(), values.end(), random_generator);

    const auto value_segment = std::make_shared<ValueSegment<int>>(std::move(values));
    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    std::vector<std::shared_ptr<Chunk>> chunk = {std::make_shared<Chunk>(Segments({value_segment}))};

    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    const TableColumnDefinitions definitions = {TableColumnDefinition(column_name, DataType::kInt, false)};
    return std::make_shared<Table>(definitions, std::move(chunk));
  }

 protected:
  std::shared_ptr<TableWrapper> table_wrapper_left_;
  std::shared_ptr<TableWrapper> table_wrapper_right_;
  size_t table_data_byte_size_ = 0;
};

REGISTER_HASH_JOIN_OPERATOR_MICROBENCHMARK(InnerJoin, JoinMode::kInner)          // NOLINT(misc-use-anonymous-namespace)
REGISTER_HASH_JOIN_OPERATOR_MICROBENCHMARK(LeftOuterJoin, JoinMode::kLeftOuter)  // NOLINT(misc-use-anonymous-namespace)
REGISTER_HASH_JOIN_OPERATOR_MICROBENCHMARK(RightOuterJoin,                       // NOLINT(misc-use-anonymous-namespace)
                                           JoinMode::kRightOuter)
REGISTER_HASH_JOIN_OPERATOR_MICROBENCHMARK(FullOuterJoin, JoinMode::kFullOuter)  // NOLINT(misc-use-anonymous-namespace)

}  // namespace skyrise
