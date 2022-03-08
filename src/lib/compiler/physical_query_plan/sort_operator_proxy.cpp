#include "sort_operator_proxy.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>
#include <magic_enum.hpp>

namespace {

const std::string kJsonKeySortDefinitions = "sort_definitions";
const std::string kJsonKeySortColumnId = "sort_column_id";
const std::string kJsonKeySortMode = "sort_mode";

}  // namespace

namespace skyrise {

SortOperatorProxy::SortOperatorProxy(std::vector<SortColumnDefinition> sort_definitions)
    : AbstractOperatorProxy(OperatorType::kSort), sort_definitions_(std::move(sort_definitions)) {
  Assert(!sort_definitions_.empty(), "Expected at least one sort definition.");
}

const std::string& SortOperatorProxy::Name() const {
  static const std::string kName = "Sort";
  return kName;
}

const std::vector<SortColumnDefinition> SortOperatorProxy::SortDefinitions() const { return sort_definitions_; }

bool SortOperatorProxy::IsPipelineBreaker() const { return true; }

Aws::Utils::Json::JsonValue SortOperatorProxy::ToJson() const {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> sort_definitions_json_array(sort_definitions_.size());

  for (size_t i = 0; i < sort_definitions_.size(); ++i) {
    const auto& sort_column_definition = sort_definitions_[i];
    sort_definitions_json_array[i] =
        Aws::Utils::Json::JsonValue()
            .WithInteger(kJsonKeySortColumnId, sort_column_definition.column)
            .WithString(kJsonKeySortMode, std::string(magic_enum::enum_name(sort_column_definition.sort_mode)));
  }

  return AbstractOperatorProxy::ToJson().WithArray(kJsonKeySortDefinitions, sort_definitions_json_array);
}

std::shared_ptr<AbstractOperatorProxy> SortOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  const Aws::Utils::Array<Aws::Utils::Json::JsonView> sort_definitions_json_array =
      json.GetArray(kJsonKeySortDefinitions);
  std::vector<SortColumnDefinition> sort_definitions;
  sort_definitions.reserve(sort_definitions_json_array.GetLength());
  for (size_t i = 0; i < sort_definitions_json_array.GetLength(); i++) {
    sort_definitions.emplace_back(
        sort_definitions_json_array[i].GetInteger(kJsonKeySortColumnId),
        magic_enum::enum_cast<SortMode>(sort_definitions_json_array[i].GetString(kJsonKeySortMode)).value());
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

std::shared_ptr<AbstractOperator> SortOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail("CreateOperatorInstanceRecursively() is not yet implemented.");
  return nullptr;
}

}  // namespace skyrise
