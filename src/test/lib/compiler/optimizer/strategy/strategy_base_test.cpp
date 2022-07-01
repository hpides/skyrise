/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "strategy_base_test.hpp"

#include <memory>
#include <string>
#include <utility>

#include "compiler/logical_query_plan/abstract_lqp_node.hpp"
#include "compiler/logical_query_plan/logical_plan_root_node.hpp"
#include "compiler/optimizer/abstract_rule.hpp"
#include "compiler/physical_query_plan/operator_proxy/abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"

namespace {
using namespace skyrise;  // NOLINT(google-build-using-namespace)
const std::string kDummyBucketName = "dummy-export-bucket";
const std::string kDummyTargetObjectKey = "dummy_key";
const auto kDummyExportFormat = ExportFormat::kCsv;
}  // namespace

namespace skyrise {

std::shared_ptr<AbstractLqpNode> StrategyBaseTest::ApplyRule(const std::shared_ptr<AbstractRule>& rule,
                                                             const std::shared_ptr<AbstractLqpNode>& input) {
  // Add explicit root node
  const auto root_node = LogicalPlanRootNode::Make();
  root_node->SetLeftInput(input);

  rule->ApplyTo(root_node);

  // Remove LogicalPlanRootNode
  auto optimized_lqp = root_node->LeftInput();
  root_node->SetLeftInput(nullptr);

  return optimized_lqp;
}

std::shared_ptr<AbstractOperatorProxy> StrategyBaseTest::ApplyRule(
    const std::shared_ptr<AbstractRule>& rule, const std::shared_ptr<AbstractOperatorProxy>& input) {
  Assert(input->Type() != OperatorType::kExport, "Did not expect input root to have OperatorType::kExport.");

  // Add an ExportOperatorProxy as a root node
  const auto root_node = ExportOperatorProxy::Make(kDummyBucketName, kDummyTargetObjectKey, kDummyExportFormat);
  root_node->SetLeftInput(input);

  rule->ApplyTo(input);

  // Remove root node
  auto optimized_pqp = root_node->LeftInput();
  root_node->SetLeftInput(nullptr);

  return optimized_pqp;
}

}  // namespace skyrise
