#include "aggregate_operator_proxy.hpp"

#include <sstream>

#include "expression/expression_utils.hpp"
#include "expression/serialization/expression_deserializer.hpp"
#include "expression/serialization/expression_serializer.hpp"
#include "utils/json.hpp"

namespace {

const std::string kJsonKeyAggregates = "aggregates";
const std::string kJsonKeyGroupByColumnIds = "group_by_column_ids";
const std::string kJsonKeyIsPipelineBreaker = "is_pipeline_breaker";
const std::string kName = "Aggregate";

}  // namespace

namespace skyrise {

AggregateOperatorProxy::AggregateOperatorProxy(std::vector<ColumnId> groupby_column_ids,
                                               std::vector<std::shared_ptr<AbstractExpression>> aggregates)
    : AbstractOperatorProxy(OperatorType::kAggregate),
      groupby_column_ids_(std::move(groupby_column_ids)),
      aggregates_(std::move(aggregates)),
      is_pipeline_breaker_(true) {}

const std::string& AggregateOperatorProxy::Name() const { return kName; }

std::string AggregateOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << "GroupByColumnIds{" << groupby_column_ids_ << "}" << separator;

  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const auto& aggregate = aggregates_[i];
    stream << aggregate->AsColumnName();

    if (i + 1 < aggregates_.size()) {
      stream << "," << separator;
    }
  }

  return stream.str();
}

const std::vector<ColumnId>& AggregateOperatorProxy::GroupByColumnIds() const { return groupby_column_ids_; }

const std::vector<std::shared_ptr<AbstractExpression>>& AggregateOperatorProxy::Aggregates() const {
  return aggregates_;
}

bool AggregateOperatorProxy::IsPipelineBreaker() const { return is_pipeline_breaker_; }

void AggregateOperatorProxy::SetIsPipelineBreaker(bool is_pipeline_breaker) {
  is_pipeline_breaker_ = is_pipeline_breaker;
}

size_t AggregateOperatorProxy::OutputColumnsCount() const { return groupby_column_ids_.size() + aggregates_.size(); }

Aws::Utils::Json::JsonValue AggregateOperatorProxy::ToJson() const {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> aggregates_json(aggregates_.size());

  for (size_t i = 0; i < aggregates_.size(); ++i) {
    aggregates_json[i] = ExpressionSerializer::Serialize(*aggregates_[i]);
  }

  return AbstractOperatorProxy::ToJson()
      .WithArray(kJsonKeyAggregates, aggregates_json)
      .WithArray(kJsonKeyGroupByColumnIds, VectorToJsonArray(groupby_column_ids_))
      .WithBool(kJsonKeyIsPipelineBreaker, is_pipeline_breaker_);
}

std::shared_ptr<AbstractOperatorProxy> AggregateOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  const auto json_aggregates = json.GetArray(kJsonKeyAggregates);
  std::vector<std::shared_ptr<AbstractExpression>> aggregates;
  aggregates.reserve(json_aggregates.GetLength());

  for (size_t i = 0; i < json_aggregates.GetLength(); ++i) {
    auto deserialized_expression = ExpressionDeserializer::Deserialize(json_aggregates.GetItem(i));
    Assert(deserialized_expression->type_ == ExpressionType::kAggregate, "Expected type AggregateExpression.");
    aggregates.emplace_back(deserialized_expression);
  }

  auto group_by_column_ids = JsonArrayToVector<ColumnId>(json.GetArray(kJsonKeyGroupByColumnIds));

  auto aggregate_proxy = AggregateOperatorProxy::Make(group_by_column_ids, aggregates);
  aggregate_proxy->SetAttributesFromJson(json);

  // Because of an AWS SDK bug, GetBool() always returns false.
  // Workaround: Retrieve bool fields as objects and cast to bool.
  aggregate_proxy->SetIsPipelineBreaker(json.GetObject(kJsonKeyIsPipelineBreaker).AsBool());

  return aggregate_proxy;
}

std::shared_ptr<AbstractOperatorProxy> AggregateOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  auto aggregate_proxy =
      AggregateOperatorProxy::Make(groupby_column_ids_, ExpressionsDeepCopy(aggregates_), copied_left_input);
  aggregate_proxy->is_pipeline_breaker_ = is_pipeline_breaker_;

  return aggregate_proxy;
}

std::shared_ptr<AbstractOperator> AggregateOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail("CreateOperatorInstanceRecursively() is not yet implemented.");
  return nullptr;
}

}  // namespace skyrise
