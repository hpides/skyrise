#include "union_operator_proxy.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>
#include <magic_enum/magic_enum.hpp>

#include "types.hpp"

namespace {

const std::string kJsonKeySetOperationMode = "set_operation_mode";
const std::string kName = "Union";

}  // namespace

namespace skyrise {

UnionOperatorProxy::UnionOperatorProxy(const SetOperationMode mode)
    : AbstractOperatorProxy(OperatorType::kUnion), mode_(mode) {}

const std::string& UnionOperatorProxy::Name() const { return kName; }

std::string UnionOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << mode_;

  return stream.str();
}

bool UnionOperatorProxy::RequiresRightInput() const { return true; }

bool UnionOperatorProxy::IsPipelineBreaker() const {
  // TODO(tobodner): True, if the UNION operation is part of the original SQL query.
  //               However, when the node was inserted during optimization, for parallel filtering, for example, the
  //               union might no longer be a pipeline-breaker.
  return true;
}

size_t UnionOperatorProxy::OutputObjectsCount() const {
  // TODO(tobodner): The function result depends on the Union operator implementation, which is currently missing.
  //               Therefore, the function logic must be adjusted when the operator gets implemented. For now, the
  //               following temporary logic should suffice.
  return LeftInput()->OutputObjectsCount();
}

size_t UnionOperatorProxy::OutputColumnsCount() const {
  Assert(LeftInput()->OutputColumnsCount() == RightInput()->OutputColumnsCount(),
         "Union inputs should output the same number of ColumnIds.");
  return LeftInput()->OutputColumnsCount();
}

SetOperationMode UnionOperatorProxy::GetSetOperationMode() const { return mode_; }

Aws::Utils::Json::JsonValue UnionOperatorProxy::ToJson() const {
  return AbstractOperatorProxy::ToJson().WithString(kJsonKeySetOperationMode,
                                                    std::string(magic_enum::enum_name(mode_)));
}

std::shared_ptr<AbstractOperatorProxy> UnionOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  auto mode = magic_enum::enum_cast<SetOperationMode>(json.GetString(kJsonKeySetOperationMode)).value();

  auto union_proxy = UnionOperatorProxy::Make(mode);
  union_proxy->SetAttributesFromJson(json);

  return union_proxy;
}

std::shared_ptr<AbstractOperatorProxy> UnionOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const {
  return UnionOperatorProxy::Make(mode_, copied_left_input, copied_right_input);
}

size_t UnionOperatorProxy::ShallowHash() const { return boost::hash_value(mode_); }

std::shared_ptr<AbstractOperator> UnionOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail("CreateOperatorInstanceRecursively() is not yet implemented.");
  return nullptr;
}

}  // namespace skyrise
