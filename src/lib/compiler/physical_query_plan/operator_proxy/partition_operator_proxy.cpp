#include "partition_operator_proxy.hpp"

#include <boost/container_hash/hash.hpp>
#include <magic_enum/magic_enum.hpp>

#include "operator/partition_operator.hpp"
#include "utils/assert.hpp"

namespace {

const std::string kJsonKeyPartitionCount = "partition_count";
const std::string kJsonKeyPartitionColumnIds = "partition_column_ids";
const std::string kName = "Partition";

}  // namespace

namespace skyrise {

PartitionOperatorProxy::PartitionOperatorProxy(std::shared_ptr<AbstractPartitioningFunction> partitioning_function)
    : AbstractOperatorProxy(OperatorType::kPartition), partitioning_function_(std::move(partitioning_function)) {}

const std::string& PartitionOperatorProxy::Name() const { return kName; }

std::string PartitionOperatorProxy::Description(const DescriptionMode mode) const {
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';

  std::stringstream stream;
  stream << AbstractOperatorProxy::Description(mode) << separator;
  // TODO(tobodner): Add PartitioningFunctionType
  stream << partitioning_function_->PartitionCount() << " partition(s)" << separator << "ColumnIds{";

  const auto& partition_column_ids = partitioning_function_->PartitionColumnIds();

  for (auto column_id_iterator = partition_column_ids.cbegin(); column_id_iterator != partition_column_ids.cend();
       ++column_id_iterator) {
    stream << *column_id_iterator;
    if (column_id_iterator != std::prev(partition_column_ids.cend(), 1)) {
      stream << ", ";
    }
  }

  stream << "}";

  return stream.str();
}

size_t PartitionOperatorProxy::PartitionCount() const { return partitioning_function_->PartitionCount(); }

const std::set<ColumnId>& PartitionOperatorProxy::PartitionColumnIds() const {
  return partitioning_function_->PartitionColumnIds();
}

bool PartitionOperatorProxy::IsPipelineBreaker() const { return false; }

Aws::Utils::Json::JsonValue PartitionOperatorProxy::ToJson() const {
  return AbstractOperatorProxy::ToJson().WithObject("partitioning_function", partitioning_function_->ToJson());
}

std::shared_ptr<AbstractOperatorProxy> PartitionOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  const auto partitioning_function = AbstractPartitioningFunction::FromJson(json.GetObject("partitioning_function"));

  auto partition_proxy = PartitionOperatorProxy::Make(partitioning_function);
  partition_proxy->SetAttributesFromJson(json);

  return partition_proxy;
}

std::shared_ptr<AbstractOperatorProxy> PartitionOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return PartitionOperatorProxy::Make(
      std::make_shared<HashPartitioningFunction>(partitioning_function_->PartitionColumnIds(),
                                                 partitioning_function_->PartitionCount()),
      copied_left_input);
}

size_t PartitionOperatorProxy::ShallowHash() const {
  size_t hash = boost::hash_value(partitioning_function_->PartitionCount());
  for (const auto partition_column_id : partitioning_function_->PartitionColumnIds()) {
    boost::hash_combine(hash, partition_column_id);
  }

  return hash;
}

std::shared_ptr<AbstractOperator> PartitionOperatorProxy::CreateOperatorInstanceRecursively() {
  Assert(LeftInput(), "Missing input operator proxy.");
  return std::make_shared<PartitionOperator>(LeftInput()->GetOrCreateOperatorInstance(), partitioning_function_);
}

}  // namespace skyrise
