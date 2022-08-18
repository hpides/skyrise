#include "exchange_operator_proxy.hpp"

#include <sstream>
#include <string>

#include <boost/container_hash/hash.hpp>

#include "types.hpp"

namespace {

const std::string kName = "Exchange";

}  // namespace

namespace skyrise {

ExchangeOperatorProxy::ExchangeOperatorProxy(std::shared_ptr<const AbstractExchangeStrategy> strategy)
    : AbstractOperatorProxy(OperatorType::kExchange), strategy_(std::move(strategy)) {}

const std::string& ExchangeOperatorProxy::Name() const { return kName; }

std::string ExchangeOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << strategy_->Type() << separator;
  stream << "Target: " << OutputObjectsCount() << " object(s),";
  stream << separator << OutputPartitionsCount() << " partition(s)";
  return stream.str();
}

const std::shared_ptr<const AbstractExchangeStrategy>& ExchangeOperatorProxy::Strategy() const { return strategy_; }

void ExchangeOperatorProxy::SetStrategy(std::shared_ptr<const AbstractExchangeStrategy> strategy) {
  strategy_ = std::move(strategy);
}

const DataTraits& ExchangeOperatorProxy::OutputDataTraits() const override {
  // Since InputDataTraits might change as a result of optimizations, update the current OutputDataTraits accordingly.
  output_data_traits_.column_count = InputDataTraits().column_count;
  output_data_traits_.partition_count = strategy_->TargetPartitionCount();
  output_data_traits_.object_count = strategy_->TargetObjectCount(InputDataTraits().object_count);
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
  return ExchangeOperatorProxy::Make(strategy_, copied_left_input);
}

size_t ExchangeOperatorProxy::ShallowHash() const { return boost::hash_value(strategy_->Hash()); }

std::shared_ptr<AbstractOperator> ExchangeOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail(Name() + " does not have an operator equivalent since it is pure a logical entity.");
}

}  // namespace skyrise
