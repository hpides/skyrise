#include "join_operator_proxy.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "operator/hash_join_operator.hpp"
#include "operator/join_operator_predicate.hpp"

namespace {

const std::string kJsonKeyJoinMode = "join_mode";
const std::string kJsonKeyPrimaryPredicate = "primary_predicate";
const std::string kJsonKeySecondaryPredicates = "secondary_predicates";
const std::string kJsonKeyColumnIdLeft = "column_id_left";
const std::string kJsonKeyColumnIdRight = "column_id_right";
const std::string kJsonKeyPredicateCondition = "predicate_condition";

const std::string kNameHash = "HashJoin";

}  // namespace

namespace skyrise {

JoinOperatorProxy::JoinOperatorProxy(const JoinMode mode, std::shared_ptr<JoinOperatorPredicate> primary_predicate,
                                     std::vector<std::shared_ptr<JoinOperatorPredicate>> secondary_predicates)
    : AbstractOperatorProxy(OperatorType::kHashJoin),
      mode_(mode),
      primary_predicate_(std::move(primary_predicate)),
      secondary_predicates_(std::move(secondary_predicates)) {}

const std::string& JoinOperatorProxy::Name() const {
  switch (type_) {
    case OperatorType::kHashJoin:
      return kNameHash;
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

  // TODO(tobodner): resolve table names to display where clause
  stream << separator << "where column #" << primary_predicate_->column_id_left << " ";
  stream << magic_enum::enum_name(primary_predicate_->predicate_condition) << " ";
  stream << "column #" << primary_predicate_->column_id_right;

  // Join predicates
  if (!secondary_predicates_.empty()) {
    stream << separator << "and ";

    for (size_t i = 0; i < secondary_predicates_.size(); ++i) {
      stream << "column #" << secondary_predicates_[i]->column_id_left << " ";
      stream << magic_enum::enum_name(secondary_predicates_[i]->predicate_condition) << " ";
      stream << "column #" << secondary_predicates_[i]->column_id_right;

      if (i < secondary_predicates_.size() - 1) {
        stream << separator << "and ";
      }
    }
  }

  return stream.str();
}

bool JoinOperatorProxy::RequiresRightInput() const { return true; }

JoinMode JoinOperatorProxy::GetJoinMode() const { return mode_; }

const std::shared_ptr<JoinOperatorPredicate>& JoinOperatorProxy::PrimaryPredicate() const { return primary_predicate_; }

const std::vector<std::shared_ptr<JoinOperatorPredicate>>& JoinOperatorProxy::SecondaryPredicates() const {
  return secondary_predicates_;
}

bool JoinOperatorProxy::IsPipelineBreaker() const { return true; }

size_t JoinOperatorProxy::OutputObjectsCount() const {
  // TODO(tobodner): Currently, we do not have a join implementation. But, since we aim for a distributed join, we
  // assume
  //               multiple output partitions. The following partition output count, however, is arbitrary and just for
  //               testing purposes.
  //               Replace with some proper logic, if possible.
  return std::max(LeftInput()->OutputObjectsCount(), RightInput()->OutputObjectsCount());
}

size_t JoinOperatorProxy::OutputColumnsCount() const {
  return LeftInput()->OutputColumnsCount() + RightInput()->OutputColumnsCount();
}

void JoinOperatorProxy::SetImplementation(OperatorType operator_type) {
  Assert(operator_type == OperatorType::kHashJoin, "The given operator type is not a valid join implementation.");
  type_ = operator_type;
}

Aws::Utils::Json::JsonValue JoinOperatorProxy::ToJson() const {
  auto json = AbstractOperatorProxy::ToJson();
  json.WithString(kJsonKeyJoinMode, std::string(magic_enum::enum_name(mode_)));

  if (primary_predicate_) {
    json.WithObject(kJsonKeyPrimaryPredicate, SerializePredicate(primary_predicate_));
  }

  if (!secondary_predicates_.empty()) {
    Aws::Utils::Array<Aws::Utils::Json::JsonValue> json_secondary_predicates(secondary_predicates_.size());

    for (size_t i = 0; i < secondary_predicates_.size(); ++i) {
      json_secondary_predicates[i] = SerializePredicate(secondary_predicates_[i]);
    }

    json.WithArray(kJsonKeySecondaryPredicates, json_secondary_predicates);
  }

  return json;
}

std::shared_ptr<AbstractOperatorProxy> JoinOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  Assert(json.ValueExists(kJsonKeyJoinMode), "JoinOperatorProxy must have a join mode.");
  const JoinMode join_mode = magic_enum::enum_cast<JoinMode>(json.GetString(kJsonKeyJoinMode)).value();

  std::shared_ptr<JoinOperatorPredicate> primary_predicate = nullptr;

  if (json.ValueExists(kJsonKeyPrimaryPredicate)) {
    primary_predicate = DeserializePredicate(json.GetObject(kJsonKeyPrimaryPredicate));
  }

  std::vector<std::shared_ptr<JoinOperatorPredicate>> secondary_predicates;

  if (json.ValueExists(kJsonKeySecondaryPredicates)) {
    const auto json_secondary_predicates = json.GetArray(kJsonKeySecondaryPredicates);
    secondary_predicates.reserve(json_secondary_predicates.GetLength());

    for (size_t i = 0; i < json_secondary_predicates.GetLength(); ++i) {
      secondary_predicates.push_back(DeserializePredicate(json_secondary_predicates[i]));
    }
  }

  const auto operator_type = magic_enum::enum_cast<OperatorType>(json.GetString(kJsonKeyOperatorType)).value();
  // NOLINTNEXTLINE(hicpp-multiway-paths-covered)
  switch (operator_type) {
    case OperatorType::kHashJoin: {
      auto join_proxy = JoinOperatorProxy::Make(join_mode, primary_predicate, secondary_predicates);
      join_proxy->SetImplementation(operator_type);
      join_proxy->SetAttributesFromJson(json);
      return join_proxy;
    }
    default:
      Fail("Cannot create join implementation from given operator type.");
  }
}

std::shared_ptr<AbstractOperatorProxy> JoinOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const {
  std::shared_ptr<JoinOperatorPredicate> primary_predicate_copy = nullptr;

  if (primary_predicate_) {
    primary_predicate_copy = std::make_shared<JoinOperatorPredicate>(
        JoinOperatorPredicate{.column_id_left = primary_predicate_->column_id_left,
                              .column_id_right = primary_predicate_->column_id_right,
                              .predicate_condition = primary_predicate_->predicate_condition});
  }

  std::vector<std::shared_ptr<JoinOperatorPredicate>> secondary_join_predicates_copy;

  if (!secondary_predicates_.empty()) {
    secondary_join_predicates_copy.reserve(secondary_predicates_.size());

    for (const auto& predicate : secondary_predicates_) {
      secondary_join_predicates_copy.push_back(std::make_shared<JoinOperatorPredicate>(
          JoinOperatorPredicate{.column_id_left = predicate->column_id_left,
                                .column_id_right = predicate->column_id_right,
                                .predicate_condition = predicate->predicate_condition}));
    }
  }

  return JoinOperatorProxy::Make(mode_, primary_predicate_copy, secondary_join_predicates_copy, copied_left_input,
                                 copied_right_input);
}

size_t JoinOperatorProxy::ShallowHash() const {
  size_t hash = boost::hash_value(mode_);
  boost::hash_combine(hash, primary_predicate_->Hash());
  for (const auto& secondary_predicate : secondary_predicates_) {
    boost::hash_combine(hash, secondary_predicate->Hash());
  }

  return hash;
}

std::shared_ptr<AbstractOperator> JoinOperatorProxy::CreateOperatorInstanceRecursively() {
  Assert(type_ == OperatorType::kHashJoin, "OperatorType must be HashJoin.");
  Assert(secondary_predicates_.empty(), "Join does not support secondary predicates yet.");
  Assert(LeftInput(), "Join needs a left input.");
  Assert(RightInput(), "Join needs a right input.");

  return std::make_shared<HashJoinOperator>(LeftInput()->GetOrCreateOperatorInstance(),
                                            RightInput()->GetOrCreateOperatorInstance(), primary_predicate_, mode_);
}

Aws::Utils::Json::JsonValue JoinOperatorProxy::SerializePredicate(
    const std::shared_ptr<JoinOperatorPredicate>& predicate) {
  return Aws::Utils::Json::JsonValue()
      .WithInteger(kJsonKeyColumnIdLeft, predicate->column_id_left)
      .WithInteger(kJsonKeyColumnIdRight, predicate->column_id_right)
      .WithString(kJsonKeyPredicateCondition, std::string(magic_enum::enum_name(predicate->predicate_condition)));
}

std::shared_ptr<JoinOperatorPredicate> JoinOperatorProxy::DeserializePredicate(
    const Aws::Utils::Json::JsonView& predicate) {
  Assert(predicate.ValueExists(kJsonKeyColumnIdLeft), "Predicate must contain a left column id.");
  Assert(predicate.ValueExists(kJsonKeyColumnIdRight), "Predicate must contain a right column id.");
  Assert(predicate.ValueExists(kJsonKeyPredicateCondition), "Predicate must contain a predicate condition.");

  return std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
      .column_id_left = static_cast<ColumnId>(predicate.GetInteger(kJsonKeyColumnIdLeft)),
      .column_id_right = static_cast<ColumnId>(predicate.GetInteger(kJsonKeyColumnIdRight)),
      .predicate_condition =
          magic_enum::enum_cast<PredicateCondition>(predicate.GetString(kJsonKeyPredicateCondition)).value()});
}

std::shared_ptr<JoinOperatorPredicate> JoinOperatorPredicate_(
    const std::shared_ptr<BinaryPredicateExpression>& binary_predicate_expression) {
  Assert(binary_predicate_expression->LeftOperand()->GetExpressionType() == ExpressionType::kPqpColumn &&
             binary_predicate_expression->RightOperand()->GetExpressionType() == ExpressionType::kPqpColumn,
         "Binary predicate must have PQP columns as operands.");
  const auto left_pqp_column =
      std::static_pointer_cast<PqpColumnExpression>(binary_predicate_expression->LeftOperand());
  const auto right_pqp_column =
      std::static_pointer_cast<PqpColumnExpression>(binary_predicate_expression->RightOperand());

  auto join_operator_predicate = std::make_shared<JoinOperatorPredicate>(
      JoinOperatorPredicate{.column_id_left = left_pqp_column->GetColumnId(),
                            .column_id_right = right_pqp_column->GetColumnId(),
                            .predicate_condition = binary_predicate_expression->GetPredicateCondition()});
  return join_operator_predicate;
}

}  // namespace skyrise
