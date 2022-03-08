#include "partition_operator_proxy.hpp"

#include "operator/partition_operator.hpp"

namespace {
const std::string kJsonKeyPartitionCount{"partition_count"};
const std::string kJsonKeyPartitionColumnIds{"partition_column_ids"};
}  // namespace

namespace skyrise {

PartitionOperatorProxy::PartitionOperatorProxy(const size_t partition_count,
                                               const std::set<ColumnId>& partition_column_ids)
    : AbstractOperatorProxy(OperatorType::kPartition),
      partition_count_(partition_count),
      partition_column_ids_(partition_column_ids) {}

const std::string& PartitionOperatorProxy::Name() const {
  static const std::string kName = "Partition";
  return kName;
}

std::string PartitionOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << partition_count_ << " bucket(s)" << separator;
  stream << "ColumnIds{";
  auto column_ids_iter = partition_column_ids_.cbegin();
  while (column_ids_iter != partition_column_ids_.cend()) {
    stream << *column_ids_iter++;
    if (column_ids_iter != partition_column_ids_.cend()) {
      stream << ", ";
    }
  }
  stream << "}";

  return stream.str();
}

size_t PartitionOperatorProxy::PartitionCount() const { return partition_count_; }

const std::set<ColumnId>& PartitionOperatorProxy::PartitionColumnIds() const { return partition_column_ids_; }

bool PartitionOperatorProxy::IsPipelineBreaker() const { return false; }

Aws::Utils::Json::JsonValue PartitionOperatorProxy::ToJson() const {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> partition_column_id_array(partition_column_ids_.size());

  size_t i = 0;
  for (const auto& partition_column_id : partition_column_ids_) {
    partition_column_id_array[i++] = Aws::Utils::Json::JsonValue().AsInteger(partition_column_id);
  }

  return AbstractOperatorProxy::ToJson()
      .WithInteger(kJsonKeyPartitionCount, partition_count_)
      .WithArray(kJsonKeyPartitionColumnIds, partition_column_id_array);
}

std::shared_ptr<AbstractOperatorProxy> PartitionOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  const size_t partition_count = json.GetInteger(kJsonKeyPartitionCount);
  const auto partition_column_id_array = json.GetArray(kJsonKeyPartitionColumnIds);

  // Store ColumnIds in a set to provide a deterministic order.
  std::set<ColumnId> partition_column_id_set;
  for (size_t i = 0; i < partition_column_id_array.GetLength(); i++) {
    partition_column_id_set.emplace(partition_column_id_array[i].AsInteger());
  }

  auto partition_proxy = PartitionOperatorProxy::Make(partition_count, partition_column_id_set);
  partition_proxy->SetAttributesFromJson(json);

  return partition_proxy;
}

std::shared_ptr<AbstractOperatorProxy> PartitionOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return PartitionOperatorProxy::Make(partition_count_, partition_column_ids_, copied_left_input);
}

std::shared_ptr<AbstractOperator> PartitionOperatorProxy::CreateOperatorInstanceRecursively() {
  Assert(LeftInput(), "Missing input operator proxy.");
  return std::make_shared<PartitionOperator>(LeftInput()->GetOrCreateOperatorInstance(), partition_count_,
                                             partition_column_ids_);
}

}  // namespace skyrise
