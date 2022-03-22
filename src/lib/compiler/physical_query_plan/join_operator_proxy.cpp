#include "join_operator_proxy.hpp"

#include <sstream>

#include "expression/expression_utils.hpp"

namespace {

const std::string kNameHash = "HashJoin";
const std::string kNameNestedLoop = "NestedLoopJoin";

}  // namespace

namespace skyrise {

JoinOperatorProxy::JoinOperatorProxy(const JoinMode mode, std::shared_ptr<AbstractExpression> primary_predicate,
                                     std::vector<std::shared_ptr<AbstractExpression>> secondary_predicates)
    : AbstractOperatorProxy(OperatorType::kNestedLoopJoin),
      mode_(mode),
      primary_predicate_(std::move(primary_predicate)),
      secondary_predicates_(std::move(secondary_predicates)) {}

const std::string& JoinOperatorProxy::Name() const {
  switch (type_) {
    case OperatorType::kHashJoin:
      return kNameHash;
    case OperatorType::kNestedLoopJoin:
      return kNameNestedLoop;
    default:
      Fail("Undefined join implementation name.");
  }
}

std::string JoinOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;

  stream << mode_;
  if (mode_ == JoinMode::kCross) {
    // Cross joins do not have any predicates.
    return stream.str();
  }

  stream << separator << "where " << primary_predicate_->AsColumnName();

  // Join predicates
  if (!secondary_predicates_.empty()) {
    stream << separator << "and ";

    for (size_t i = 0; i < secondary_predicates_.size(); ++i) {
      stream << secondary_predicates_.at(i)->AsColumnName();
      if (i < secondary_predicates_.size() - 1) {
        stream << separator << "and ";
      }
    }
  }

  return stream.str();
}

bool JoinOperatorProxy::RequiresRightInput() const { return true; }

JoinMode JoinOperatorProxy::GetJoinMode() const { return mode_; }

const std::shared_ptr<AbstractExpression>& JoinOperatorProxy::PrimaryPredicate() const { return primary_predicate_; }

const std::vector<std::shared_ptr<AbstractExpression>>& JoinOperatorProxy::SecondaryPredicates() const {
  return secondary_predicates_;
}

bool JoinOperatorProxy::IsPipelineBreaker() const { return true; }

size_t JoinOperatorProxy::OutputObjectsCount() const {
  // TODO(anyone): Currently, we do not have a join implementation. But, since we aim for a distributed join, we assume
  //               multiple output partitions. The following partition output count, however, is arbitrary and just for
  //               testing purposes.
  //               Replace with some proper logic, if possible.
  return std::max(LeftInput()->OutputObjectsCount(), RightInput()->OutputObjectsCount());
}

size_t JoinOperatorProxy::OutputColumnsCount() const {
  return LeftInput()->OutputColumnsCount() + RightInput()->OutputColumnsCount();
}

void JoinOperatorProxy::SetImplementation(OperatorType operator_type) {
  Assert(operator_type == OperatorType::kNestedLoopJoin || operator_type == OperatorType::kHashJoin,
         "The given operator type is not a valid join implementation.");
  type_ = operator_type;
}

Aws::Utils::Json::JsonValue JoinOperatorProxy::ToJson() const {
  Fail("ToJson() is not yet implemented.");
  return AbstractOperatorProxy::ToJson();
}

std::shared_ptr<AbstractOperatorProxy> JoinOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  const auto operator_type = magic_enum::enum_cast<OperatorType>(json.GetString(kJsonKeyOperatorType)).value();
  switch (operator_type) {
    case OperatorType::kHashJoin:
      // TODO(anyone): Create JoinHash instance
    case OperatorType::kNestedLoopJoin:
      // TODO(anyone): Create JoinNestedLoop instance
    default:
      Fail("Cannot create join implementation from given operator type.");
  }
}

std::shared_ptr<AbstractOperatorProxy> JoinOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const {
  std::shared_ptr<AbstractExpression> primary_predicate_copy = nullptr;
  std::vector<std::shared_ptr<AbstractExpression>> secondary_join_predicates_copy = {};
  if (mode_ != JoinMode::kCross) {
    primary_predicate_copy = primary_predicate_->DeepCopy();
    secondary_join_predicates_copy = ExpressionsDeepCopy(secondary_predicates_);
  }
  return JoinOperatorProxy::Make(mode_, primary_predicate_copy, secondary_join_predicates_copy, copied_left_input,
                                 copied_right_input);
}

std::shared_ptr<AbstractOperator> JoinOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail("CreateOperatorInstanceRecursively() is not yet implemented.");
}

}  // namespace skyrise
