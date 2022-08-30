#include "exchange_operator_proxy.hpp"

#include <sstream>
#include <string>

#include <boost/container_hash/hash.hpp>

#include "types.hpp"

namespace {

const std::string kName = "Exchange";

}  // namespace

namespace skyrise {

ExchangeOperatorProxy::ExchangeOperatorProxy(
    ExchangeType exchange_type, size_t target_bucket_count,
    std::optional<std::shared_ptr<const AbstractPartitioningFunction>> target_partitioning_function)
    : AbstractOperatorProxy(OperatorType::kExchange),
      exchange_type_(exchange_type),
      target_bucket_count_(target_bucket_count),
      target_partitioning_function_(target_partitioning_function) {
  output_data_traits_.bucket_count = target_bucket_count;

  switch (exchange_type_) {
    case ExchangeType::kBroadcast:
      Assert(target_bucket_count_ == 1, "Invalid target bucket count for Broadcast.");
      Assert(target_partitioning_function_ == std::nullopt, "Broadcast does not involve partitioning.");
      output_data_traits_.partition_count = 0;
      break;
    case ExchangeType::kCombine:
      Assert(target_bucket_count_ > 0, "Invalid target bucket count.");
      Assert(target_partitioning_function_ == std::nullopt, "Combining buckets does not involve partitioning.");
      output_data_traits_.partition_count = 0;
      break;
    case ExchangeType::kShuffle:
      Assert(target_bucket_count_ > 1, "Invalid target bucket count for Shuffle.");
      Assert(target_partitioning_function_ != std::nullopt, "Shuffle requires partitioning function.");
      output_data_traits_.partition_count = *target_partitioning_function_->PartitionCount();
      break;
  }
}

const std::string& ExchangeOperatorProxy::Name() const { return kName; }

std::string ExchangeOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << exchange_type_;
  if (exchange_type_ == ExchangeType::kBroadcast) {
    return stream.str();
  }

  stream << separator << target_bucket_count_ << " bucket(s)";
  if (exchange_type_ == ExchangeType::kCombine) {
    return stream.str();
  }

  stream << ',' << separator << target_partitioning_function_->PartitionCount() << " partition(s)";
  return stream.str();
}

ExchangeType ExchangeOperatorProxy::GetExchangeType() const { return exchange_type_; }

size_t ExchangeOperatorProxy::TargetBucketCount() const { return target_bucket_count_; }

const std::optional<std::shared_ptr<const AbstractPartitioningFunction>>&
ExchangeOperatorProxy::TargetPartitioningFunction() const {
  return target_partitioning_function_;
}

const DataTraits& ExchangeOperatorProxy::OutputDataTraits() const {
  const auto& input_traits = InputDataTraits();
  Assert(input_traits.bucket_count >= output_data_traits_.bucket_count,
         "Cannot increase the number of buckets via Exchange.");
  // InputDataTraits might change as a result of optimizations. Therefore, we must update the column_count on each
  // invocation of OutputDataTraits.
  output_data_traits_.column_count = input_traits.column_count;
  return output_data_traits_;
}

bool ExchangeOperatorProxy::IsPipelineBreaker() const {
  // This operator proxy does not specify data manipulation. Instead, it only specifies the mechanics of data exchange
  // between different pipelines in PQPs. Therefore, it is not considered as pipeline-breaking during optimization.
  return false;
}

Aws::Utils::Json::JsonValue ExchangeOperatorProxy::ToJson() const {
  Fail(Name() + " does not support (de)serialization.");
}

std::shared_ptr<AbstractOperatorProxy> ExchangeOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return ExchangeOperatorProxy::Make(exchange_type_, target_bucket_count_, target_partitioning_function_,
                                     copied_left_input);
}

size_t ExchangeOperatorProxy::ShallowHash() const {
  size_t hash = boost::hash_value(exchange_type_);
  boost::hash_combine(hash, target_bucket_count_);
  boost::hash_combine(hash, target_partitioning_function_);
  return hash;
}

std::shared_ptr<AbstractOperator> ExchangeOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail(Name() + " does not have an operator equivalent since it is pure a logical entity.");
}

}  // namespace skyrise
