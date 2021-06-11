/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <algorithm>
#include <fstream>
#include <functional>
#include <memory>
#include <tuple>
#include <vector>

#include "storage/storage_types.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/chunk_writer.hpp"
#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {
namespace detail {

template <size_t I, typename T1, typename T2>
using zip_at_index_t = std::tuple<std::tuple_element_t<I, T1>&, std::tuple_element_t<I, T2>&>;

template <size_t I, typename T1, typename T2>
zip_at_index_t<I, T1, T2> ZipTupleWithIndex(T1& t1, T2& t2) {
  return std::tie(std::get<I>(t1), std::get<I>(t2));
}

template <typename T1, typename T2, size_t... I>
std::tuple<zip_at_index_t<I, T1, T2>...> ZipTupleWithSequence(T1& t1, T2& t2, std::index_sequence<I...>) {
  return {ZipTupleWithIndex<I>(t1, t2)...};
}

// Aggregates two tuples with the same length by tying together elements with the same index from both tuples. The
// resulting tuple has as many elements as the first tuple with each element being a 2-tuple. For example with
//  std::tuple<int, double> a;
//  std::tuple<std::string, size_t> b;
// ZipTuples(a, b) will return a tuple of type
//  std::tuple<std::tuple<int, std::string>, std::tuple<double, size_t>>.
template <typename Head, typename Tail>
auto ZipTuples(Head& head, Tail& tail) {
  constexpr size_t size = std::tuple_size_v<Head>;

  static_assert(std::tuple_size_v<Tail> == size, "Tuple size mismatch.");

  return ZipTupleWithSequence<Head, Tail>(head, tail, std::make_index_sequence<size>());
}

}  // namespace detail

template <typename... DataTypes, template <typename...> class T, typename Names>
TableColumnDefinitions GetSchemaFromTypesAndNames(const T<DataTypes...>& types, const Names& names) {
  static_assert(sizeof...(DataTypes) == std::tuple_size_v<Names>);

  TableColumnDefinitions schema;

  // We want to have something like ( (int, "id"), (string, "name"), ...) given (int, string) and ("id", "name").
  const auto types_names = detail::ZipTuples(types, names);

  // Generate schema from this, assuming that no column is nullable.
  auto add_to_schema = [&schema](auto& type_name_tuple) {
    using I = typename std::decay_t<decltype(std::get<0>(type_name_tuple))>;
    schema.emplace_back(std::get<1>(type_name_tuple), DataTypeFromType<I>(), false);
  };
  std::apply([add_to_schema](auto&... type_name_tuple) { ((add_to_schema(type_name_tuple)), ...); }, types_names);

  return schema;
}

template <typename... DataTypes>
class TableBuilder {
 public:
  template <typename Names>
  TableBuilder(const PartitionedChunkWriterFactory& writer_factory, const std::string& table_name,
               const std::tuple<DataTypes...>& types, const Names& names) {
    writer_ = writer_factory(table_name, GetSchemaFromTypesAndNames(types, names));

    if (!writer_) {
      has_error_ = true;
    }
  }

  ~TableBuilder() { FinishTable(); }

  size_t NumRows() const { return row_count_; }

  template <typename... Types>
  void AppendRow(Types&&... new_values) {
    static_assert(sizeof...(DataTypes) == sizeof...(Types));

    if (has_error_) {
      return;
    }

    auto value_tuple = std::forward_as_tuple(new_values...);
    auto value_vector_pairs = detail::ZipTuples(value_tuple, value_vectors_);

    std::apply(
        [](auto&... value_vector_pair) {
          ((std::get<1>(value_vector_pair).push_back(std::get<0>(value_vector_pair))), ...);
        },
        value_vector_pairs);
    row_count_++;

    if (CurrentChunkCount() >= kChunkDefaultSize) {
      EmitChunk();
    }
  }

 private:
  size_t CurrentChunkCount() const { return std::get<0>(value_vectors_).size(); }
  void EmitChunk() {
    Segments segments;
    auto add_column_to_segment = [&segments](auto& v) {
      using T = typename std::decay_t<decltype(v)>::value_type;
      segments.emplace_back(std::make_shared<ValueSegment<T>>(std::move(v)));
      // Reset the vector
      v = std::decay_t<decltype(v)>{};
    };
    std::apply([add_column_to_segment](auto&... vectors) { ((add_column_to_segment(vectors)), ...); }, value_vectors_);

    auto chunk = std::make_shared<Chunk>(segments);
    writer_->ProcessChunk(chunk);

    if (writer_->HasError()) {
      has_error_ = true;
    }
  }

  void FinishTable() {
    if (CurrentChunkCount() > 0 && !has_error_) {
      EmitChunk();
    }
  }

  std::tuple<std::vector<DataTypes>...> value_vectors_;
  std::shared_ptr<AbstractChunkWriter> writer_;
  size_t row_count_ = 0;
  bool has_error_ = false;
};

}  // namespace skyrise
