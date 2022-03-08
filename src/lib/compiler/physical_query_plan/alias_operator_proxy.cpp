#include "alias_operator_proxy.hpp"

#include <sstream>
#include <string>

#include <boost/algorithm/string/join.hpp>

#include "types.hpp"
#include "utils/json.hpp"

namespace {
const std::string kJsonKeyColumnIds{"column_ids"};
const std::string kJsonKeyAliases = "aliases";
}  // namespace

namespace skyrise {

AliasOperatorProxy::AliasOperatorProxy(std::vector<ColumnId> column_ids, std::vector<std::string> aliases)
    : AbstractOperatorProxy(OperatorType::kAlias), column_ids_(std::move(column_ids)), aliases_(std::move(aliases)) {
  Assert(column_ids_.size() == aliases_.size(), "Expected as many aliases as columns.");
}

const std::string& AliasOperatorProxy::Name() const {
  static const std::string kName = "Alias";
  return kName;
}

std::string AliasOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << AbstractOperatorProxy::Description(mode) << separator;
  std::string delimiter = ",";
  delimiter += separator;
  stream << boost::algorithm::join(aliases_, delimiter);

  return stream.str();
}

const std::vector<ColumnId>& AliasOperatorProxy::ColumnIds() const { return column_ids_; }

const std::vector<std::string>& AliasOperatorProxy::Aliases() const { return aliases_; }

bool AliasOperatorProxy::IsPipelineBreaker() const { return false; }

size_t AliasOperatorProxy::OutputColumnsCount() const {
  DebugAssert(!LeftInput() || LeftInput()->OutputColumnsCount() == column_ids_.size(),
              "Unexpected number of ColumnIds.");
  return column_ids_.size();
}

Aws::Utils::Json::JsonValue AliasOperatorProxy::ToJson() const {
  return AbstractOperatorProxy::ToJson()
      .WithArray(kJsonKeyColumnIds, VectorToJsonArray(column_ids_))
      .WithArray(kJsonKeyAliases, VectorToJsonArray(aliases_));
}

std::shared_ptr<AbstractOperatorProxy> AliasOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  std::vector<ColumnId> column_ids = JsonArrayToVector<ColumnId>(json.GetArray(kJsonKeyColumnIds));
  std::vector<std::string> aliases = JsonArrayToVector<std::string>(json.GetArray(kJsonKeyAliases));

  auto alias_proxy = AliasOperatorProxy::Make(column_ids, aliases);
  alias_proxy->SetAttributesFromJson(json);

  return alias_proxy;
}

std::shared_ptr<AbstractOperatorProxy> AliasOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return AliasOperatorProxy::Make(column_ids_, aliases_, copied_left_input);
}

std::shared_ptr<AbstractOperator> AliasOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail("CreateOperatorInstanceRecursively() is not yet implemented.");
  return nullptr;
}

}  // namespace skyrise
