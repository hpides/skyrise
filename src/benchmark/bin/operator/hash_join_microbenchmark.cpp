#include <algorithm>
#include <numeric>
#include <random>
#include <vector>

#include <benchmark/benchmark.h>

#include "operator/hash_join_operator.hpp"
#include "operator/table_wrapper.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"
#include "types.hpp"
#include "utils/random.hpp"

namespace skyrise {

class HashJoinMicrobenchmarkFixture : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State& state) override {
    const auto table_left = CreateTableWithRandomizedInts(state.range(0) / 4, 0, "a");
    const auto table_right = CreateTableWithRandomizedInts(state.range(0), state.range(0) / 8, "b");

    table_data_byte_size_ = table_left->MemoryUsageBytes() + table_right->MemoryUsageBytes();

    table_wrapper_left_ = std::make_shared<TableWrapper>(table_left);
    table_wrapper_left_->Execute();

    table_wrapper_right_ = std::make_shared<TableWrapper>(table_right);
    table_wrapper_right_->Execute();
  }

  static std::shared_ptr<Table> CreateTableWithRandomizedInts(const size_t row_count, const size_t start,
                                                              const std::string& column_name) {
    std::vector<int> values(row_count);
    std::iota(values.begin(), values.end(), start);
    auto random_generator = RandomGenerator<std::mt19937>();
    std::shuffle(values.begin(), values.end(), random_generator);

    const auto value_segment = std::make_shared<ValueSegment<int>>(std::move(values));
    std::vector<std::shared_ptr<Chunk>> chunk = {std::make_shared<Chunk>(Segments({value_segment}))};

    const TableColumnDefinitions definitions = {TableColumnDefinition(column_name, DataType::kInt, false)};
    return std::make_shared<Table>(definitions, std::move(chunk));
  }

 protected:
  std::shared_ptr<TableWrapper> table_wrapper_left_;
  std::shared_ptr<TableWrapper> table_wrapper_right_;
  size_t table_data_byte_size_ = 0;
};

// NOLINTNEXTLINE(readability-redundant-member-init)
BENCHMARK_DEFINE_F(HashJoinMicrobenchmarkFixture, InnerJoin)(benchmark::State& state) {
  const auto predicate =
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{0, 0, PredicateCondition::kEquals});

  auto hash_join_operator =
      std::make_shared<HashJoinOperator>(table_wrapper_left_, table_wrapper_right_, predicate, JoinMode::kInner);

  for (auto _ : state) {  // NOLINT(clang-analyzer-deadcode.DeadStores)
    hash_join_operator->Execute();
  }

  state.SetBytesProcessed(table_data_byte_size_);
}

BENCHMARK_REGISTER_F(HashJoinMicrobenchmarkFixture, InnerJoin)
    ->Repetitions(5)
    ->RangeMultiplier(2)
    ->Range(1 << 20, 1 << 24);  // NOLINT(hicpp-signed-bitwise)

// NOLINTNEXTLINE(readability-redundant-member-init)
BENCHMARK_DEFINE_F(HashJoinMicrobenchmarkFixture, LeftOuterJoin)(benchmark::State& state) {
  const auto predicate =
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{0, 0, PredicateCondition::kEquals});

  auto hash_join_operator =
      std::make_shared<HashJoinOperator>(table_wrapper_left_, table_wrapper_right_, predicate, JoinMode::kLeftOuter);

  for (auto _ : state) {  // NOLINT(clang-analyzer-deadcode.DeadStores)
    hash_join_operator->Execute();
  }

  state.SetBytesProcessed(table_data_byte_size_);
}

BENCHMARK_REGISTER_F(HashJoinMicrobenchmarkFixture, LeftOuterJoin)
    ->Repetitions(5)
    ->RangeMultiplier(2)
    ->Range(1 << 20, 1 << 24);  // NOLINT(hicpp-signed-bitwise)

}  // namespace skyrise
