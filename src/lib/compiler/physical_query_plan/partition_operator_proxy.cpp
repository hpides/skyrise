#include "partition_operator_proxy.hpp"

#include "operator/partition_operator.hpp"

namespace skyrise {

static const std::string kName{"Partition"};

PartitionOperatorProxy::PartitionOperatorProxy(const size_t partition_count,
                                               const std::set<ColumnId>& partition_column_ids,
                                               std::shared_ptr<AbstractOperatorProxy> input)
    : AbstractOperatorProxy(OperatorType::kPartition, std::move(input)),
      partition_count_(partition_count),
      partition_column_ids_(partition_column_ids) {}

const std::string& PartitionOperatorProxy::Name() const { return kName; }

std::shared_ptr<AbstractOperatorProxy> PartitionOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  const size_t partition_count = json.GetInteger("partition_count");
  const auto partition_column_id_array = json.GetArray("partition_column_ids");

  // Store ColumnIds in a set to provide a deterministic order.
  std::set<ColumnId> partition_column_id_set;
  for (size_t i = 0; i < partition_column_id_array.GetLength(); i++) {
    partition_column_id_set.emplace(partition_column_id_array[i].AsInteger());
  }

  return std::make_shared<PartitionOperatorProxy>(partition_count, partition_column_id_set);
}

Aws::Utils::Json::JsonValue PartitionOperatorProxy::ToJson() const {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> partition_column_id_array(partition_column_ids_.size());

  size_t i = 0;
  for (const auto& partition_column_id : partition_column_ids_) {
    partition_column_id_array[i++] = Aws::Utils::Json::JsonValue().AsInteger(partition_column_id);
  }

  return AbstractOperatorProxy::ToJson()
      .WithInteger("partition_count", partition_count_)
      .WithArray("partition_column_ids", partition_column_id_array);
}

std::shared_ptr<AbstractOperator> PartitionOperatorProxy::CreateOperatorInstance() const {
  const auto input_operator = GetLeftInput() ? GetLeftInput()->GetOperatorInstance() : nullptr;
  return std::make_shared<PartitionOperator>(input_operator, partition_count_, partition_column_ids_);
}

}  // namespace skyrise
