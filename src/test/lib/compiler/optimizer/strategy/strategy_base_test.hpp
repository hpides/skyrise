/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>

#include <gtest/gtest.h>

namespace skyrise {

class AbstractLqpNode;
class AbstractOperatorProxy;
class AbstractRule;

class StrategyBaseTest : public ::testing::Test {
 protected:
  /**
   * Helper method for applying a single rule to an LQP. Creates the temporary LogicalPlanRootNode and returns its input
   * after applying the rule.
   */
  static std::shared_ptr<AbstractLqpNode> ApplyRule(const std::shared_ptr<AbstractRule>& rule,
                                                    const std::shared_ptr<AbstractLqpNode>& input);

  static std::shared_ptr<AbstractOperatorProxy> ApplyRule(const std::shared_ptr<AbstractRule>& rule,
                                                          const std::shared_ptr<AbstractOperatorProxy>& input);
};

}  // namespace skyrise
