#include <vector>

#include "benchmark/benchmark.h"
#include "operator/partition_operator.hpp"
#include "operator/partitioning_function.hpp"
#include "operator/table_wrapper.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"
#include "types.hpp"

namespace skyrise {

class PartitionMicrobenchmarkFixture : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State& state) override {
    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    std::vector<int> ascending_numbers(state.range(0));
    std::iota(ascending_numbers.begin(), ascending_numbers.end(), 0);

    const auto value_segment = std::make_shared<ValueSegment<int>>(std::move(ascending_numbers));
    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    std::vector<std::shared_ptr<Chunk>> chunk = {std::make_shared<Chunk>(Segments({value_segment}))};

    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    const TableColumnDefinitions definitions = {TableColumnDefinition("a", DataType::kInt, false)};
    const auto table = std::make_shared<Table>(definitions, std::move(chunk));
    table_byte_size_ = table->MemoryUsageBytes();

    table_wrapper_ = std::make_shared<TableWrapper>(table);
    table_wrapper_->Execute();
  }

 protected:
  std::shared_ptr<TableWrapper> table_wrapper_;
  size_t table_byte_size_ = 0;
};

// NOLINTNEXTLINE(readability-redundant-member-init)
BENCHMARK_DEFINE_F(PartitionMicrobenchmarkFixture, PartitionOnOneColumn)(benchmark::State& state) {
  const auto partitioning_function = std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, state.range(1));
  auto partition_operator = std::make_shared<PartitionOperator>(table_wrapper_, partitioning_function);

  for (auto _ : state) {  // NOLINT(clang-analyzer-deadcode.DeadStores)
    partition_operator->Execute();
  }

  state.SetBytesProcessed(table_byte_size_);
}

BENCHMARK_REGISTER_F(PartitionMicrobenchmarkFixture, PartitionOnOneColumn)  // NOLINT(misc-use-anonymous-namespace)
    ->RangeMultiplier(2)
    ->Ranges({{1 << 20, 1 << 24}, {1 << 10, 1 << 13}});  // NOLINT(hicpp-signed-bitwise)

}  // namespace skyrise
