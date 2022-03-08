#include "data_exchange_operator_proxy.hpp"

#include <sstream>
#include <string>

#include "types.hpp"

namespace skyrise {

DataExchangeOperatorProxy::DataExchangeOperatorProxy()
    : AbstractOperatorProxy(OperatorType::kDataExchange),
      mode_(DataExchangeMode::kFullMerge),
      output_objects_count_(1) {}

const std::string& DataExchangeOperatorProxy::Name() const {
  static const std::string kName = "DataExchange";
  return kName;
}

std::string DataExchangeOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << mode_;
  if (mode_ == DataExchangeMode::kPartialMerge) {
    if (mode == DescriptionMode::kSingleLine) {
      stream << ",";
    }
    stream << separator << output_objects_count_ << " objects";
  }
  return stream.str();
}

DataExchangeMode DataExchangeOperatorProxy::GetDataExchangeMode() const { return mode_; }

void DataExchangeOperatorProxy::SetToFullMerge() {
  output_objects_count_ = 1;
  mode_ = DataExchangeMode::kFullMerge;
}

void DataExchangeOperatorProxy::SetToPartialMerge(size_t output_objects_count) {
  Assert(output_objects_count > 1, "A partial merge should have a higher number of output partitions.");
  output_objects_count_ = output_objects_count;
  mode_ = DataExchangeMode::kPartialMerge;
}

bool DataExchangeOperatorProxy::IsPipelineBreaker() const {
  // This operator proxy does not specify data manipulation. Instead, it only specifies the mechanics of data exchange
  // between different pipelines in PQPs. Therefore, it is not considered as pipeline-breaking during optimization.
  return false;
}

size_t DataExchangeOperatorProxy::OutputObjectsCount() const { return output_objects_count_; }

Aws::Utils::Json::JsonValue DataExchangeOperatorProxy::ToJson() const {
  Fail(Name() + " does not support (de)serialization.");
}

std::shared_ptr<AbstractOperatorProxy> DataExchangeOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  auto data_exchange_proxy = DataExchangeOperatorProxy::Make(copied_left_input);
  switch (mode_) {
    case DataExchangeMode::kFullMerge:
      break;
    case DataExchangeMode::kPartialMerge:
      data_exchange_proxy->SetToPartialMerge(output_objects_count_);
      break;
    default:
      Fail("Unexpected DataExchangeMode.");
  }
  return data_exchange_proxy;
}

std::shared_ptr<AbstractOperator> DataExchangeOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail(Name() + " does not have an operator equivalent since it is pure a logical entity.");
}

}  // namespace skyrise
