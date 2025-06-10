#include "sort_operator_proxy.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>
#include <magic_enum/magic_enum.hpp>

#include "operator/sort_operator.hpp"

namespace {

const std::string kJsonKeySortDefinitions = "sort_definitions";
const std::string kJsonKeySortColumnId = "sort_column_id";
const std::string kJsonKeySortMode = "sort_mode";
const std::string kName = "Sort";

}  // namespace

namespace skyrise {

SortOperatorProxy::SortOperatorProxy(std::vector<SortColumnDefinition> sort_definitions)
    : AbstractOperatorProxy(OperatorType::kSort), sort_definitions_(std::move(sort_definitions)) {
  Assert(!sort_definitions_.empty(), "Expected at least one sort definition.");
}

const std::string& SortOperatorProxy::Name() const { return kName; }

std::vector<SortColumnDefinition> SortOperatorProxy::SortDefinitions() const { return sort_definitions_; }

bool SortOperatorProxy::IsPipelineBreaker() const { return true; }

Aws::Utils::Json::JsonValue SortOperatorProxy::ToJson() const {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> json_sort_definitions(sort_definitions_.size());

  for (size_t i = 0; i < sort_definitions_.size(); ++i) {
    const auto& sort_column_definition = sort_definitions_[i];
    json_sort_definitions[i] =
        Aws::Utils::Json::JsonValue()
            .WithInteger(kJsonKeySortColumnId, sort_column_definition.column_id)
            .WithString(kJsonKeySortMode, std::string(magic_enum::enum_name(sort_column_definition.sort_mode)));
  }

  return AbstractOperatorProxy::ToJson().WithArray(kJsonKeySortDefinitions, json_sort_definitions);
}

std::shared_ptr<AbstractOperatorProxy> SortOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  const Aws::Utils::Array<Aws::Utils::Json::JsonView> json_sort_definitions = json.GetArray(kJsonKeySortDefinitions);
  std::vector<SortColumnDefinition> sort_definitions;
  sort_definitions.reserve(json_sort_definitions.GetLength());

  for (size_t i = 0; i < json_sort_definitions.GetLength(); ++i) {
    sort_definitions.emplace_back(
        json_sort_definitions[i].GetInteger(kJsonKeySortColumnId),
        magic_enum::enum_cast<SortMode>(json_sort_definitions[i].GetString(kJsonKeySortMode)).value());
  }

  auto sort_proxy = SortOperatorProxy::Make(sort_definitions);
  sort_proxy->SetAttributesFromJson(json);

  return sort_proxy;
}

std::shared_ptr<AbstractOperatorProxy> SortOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return SortOperatorProxy::Make(sort_definitions_, copied_left_input);
}

size_t SortOperatorProxy::ShallowHash() const {
  size_t hash = 0;
  for (const auto& sort_definition : sort_definitions_) {
    boost::hash_combine(hash, sort_definition.Hash());
  }

  return hash;
}

std::shared_ptr<AbstractOperator> SortOperatorProxy::CreateOperatorInstanceRecursively() {
  Assert(LeftInput(), "Missing input operator proxy.");
  Assert(!sort_definitions_.empty(), "SortOperatorProxy must specify at least one sort definition.");
  return std::make_shared<SortOperator>(LeftInput()->GetOrCreateOperatorInstance(), sort_definitions_);
}

}  // namespace skyrise
