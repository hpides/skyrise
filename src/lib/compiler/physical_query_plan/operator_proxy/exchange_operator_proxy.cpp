#include "exchange_operator_proxy.hpp"

#include <sstream>
#include <string>

#include <boost/container_hash/hash.hpp>

#include "types.hpp"

namespace {

const std::string kName = "DataExchange";

}  // namespace

namespace skyrise {

ExchangeOperatorProxy::ExchangeOperatorProxy() : AbstractOperatorProxy(OperatorType::kExchange) {}

const std::string& ExchangeOperatorProxy::Name() const { return kName; }

std::string ExchangeOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << mode_;
  if (mode_ == ExchangeMode::kPartialMerge) {
    if (mode == DescriptionMode::kSingleLine) {
      stream << ",";
    }
    stream << separator << output_objects_count_ << " objects";
  }
  return stream.str();
}

ExchangeMode ExchangeOperatorProxy::GetExchangeMode() const { return mode_; }

void ExchangeOperatorProxy::SetToFullMerge() {
  output_objects_count_ = 1;
  mode_ = ExchangeMode::kFullMerge;
}

void ExchangeOperatorProxy::SetToPartialMerge(size_t output_objects_count) {
  Assert(output_objects_count > 1, "A partial merge should have a higher number of output partitions.");
  output_objects_count_ = output_objects_count;
  mode_ = ExchangeMode::kPartialMerge;
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
void ExchangeOperatorProxy::SetToFullyMeshedExchange() {
  // TODO(tobodner): Currently, only FullMerge and PartialMerge are implemented, which cover staged aggregations.
  //               For joins, we need to specify fully meshed data exchanges.
  Fail("The fully meshed data exchange is not implemented yet.");
}

bool ExchangeOperatorProxy::IsPipelineBreaker() const {
  // This operator proxy does not specify data manipulation. Instead, it only specifies the mechanics of data exchange
  // between different pipelines in PQPs. Therefore, it is not considered as pipeline-breaking during optimization.
  return false;
}

size_t ExchangeOperatorProxy::OutputObjectsCount() const { return output_objects_count_; }

Aws::Utils::Json::JsonValue ExchangeOperatorProxy::ToJson() const {
  Fail(Name() + " does not support (de)serialization.");
}

std::shared_ptr<AbstractOperatorProxy> ExchangeOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  auto exchange_proxy = ExchangeOperatorProxy::Make(copied_left_input);
  switch (mode_) {
    case ExchangeMode::kFullMerge:
      break;
    case ExchangeMode::kPartialMerge:
      exchange_proxy->SetToPartialMerge(output_objects_count_);
      break;
    default:
      Fail("Unexpected ExchangeMode.");
  }
  return exchange_proxy;
}

size_t ExchangeOperatorProxy::ShallowHash() const {
  size_t hash = boost::hash_value(mode_);
  boost::hash_combine(hash, output_objects_count_);

  return hash;
}

std::shared_ptr<AbstractOperator> ExchangeOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail(Name() + " does not have an operator equivalent since it is pure a logical entity.");
}

}  // namespace skyrise
