/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace skyrise {

/**
 * With this class, plan nodes inherit a static <PlanNode>::Make construction function that allows for convenience and a
 * clean notation in tests. The function creates a plan node by passing its arguments to the plan node's constructor.
 * In addition, it sets the plan node's left and right inputs, if provided as the last argument(s).
 *
 * Usage examples for the Make function:
 *
 *    auto lqp =
 *    JoinNode::Make(JoinMode::kSemi, Equals_(b_y, literal),
 *      JoinNode::Make(JoinMode::kInner, Equals_(a_a, b_x),
 *        UnionNode::Make(SetOperationMode::kAll,
 *          PredicateNode::Make(GreaterThan_(a_a, 700),
 *            node_a),
 *          PredicateNode::Make(LessThan_(a_b, 123),
 *            node_a)),
 *        node_b),
 *      ProjectionNode::Make(ExpressionVector_(literal),
 *        dummy_table_node));
 *
 *    auto pqp =
 *    JoinOperatorProxy::Make(JoinMode::kInner, JoinOperatorPredicate_(Equals_(a_a_, b_x_)), secondary_predicates,
 *      UnionOperatorProxy::Make(SetOperationMode::kAll,
 *        FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
 *          import_proxy_a_),
 *        FilterOperatorProxy::Make(LessThan_(a_b_, 123),
 *          import_proxy_a_)),
 *      import_proxy_b_);
 */
template <typename DerivedPlanNodeType, typename PlanNodeType>
class EnableMakeForPlanNode {
 public:
  // The following declaration is a variadic function template taking any number of arguments of arbitrary types.
  template <typename... ArgumentTypes>
  static std::shared_ptr<DerivedPlanNodeType> Make(ArgumentTypes&&... arguments) {
    if constexpr (sizeof...(ArgumentTypes) > 0) {
      auto arguments_tuple = std::forward_as_tuple(arguments...);

      // Check if the last function argument represents a plan node (that can be set as an input).
      if constexpr (IsPlanNodeArgument<sizeof...(ArgumentTypes) - 1, ArgumentTypes...>::value) {
        if constexpr (sizeof...(ArgumentTypes) > 1) {
          // Check if the second to last function argument represents a plan node as well.
          if constexpr (IsPlanNodeArgument<sizeof...(ArgumentTypes) - 2, ArgumentTypes...>::value) {
            // Use function arguments, except for the last two, to construct the plan node.
            auto node = CreatePlanNode(arguments_tuple, std::make_index_sequence<sizeof...(ArgumentTypes) - 2>());
            node->SetLeftInput(std::get<sizeof...(ArgumentTypes) - 2>(arguments_tuple));
            node->SetRightInput(std::get<sizeof...(ArgumentTypes) - 1>(arguments_tuple));
            return node;
          } else {
            // Use function arguments, except for the last, to construct the plan node.
            auto node = CreatePlanNode(arguments_tuple, std::make_index_sequence<sizeof...(ArgumentTypes) - 1>());
            node->SetLeftInput(std::get<sizeof...(ArgumentTypes) - 1>(arguments_tuple));
            return node;
          }
        } else {
          // Only one input plan node was provided as an argument.
          auto node = std::make_shared<DerivedPlanNodeType>();
          node->SetLeftInput(std::get<0>(arguments_tuple));
          return node;
        }
      } else {
        // Additional input plan nodes were not passed as last arguments.
        return CreatePlanNode(arguments_tuple, std::make_index_sequence<sizeof...(ArgumentTypes)>());
      }
    } else {
      // No arguments were passed to this function.
      return std::make_shared<DerivedPlanNodeType>();
    }
  }

 private:
  template <class ArgumentsTupleType, size_t... ConstructorIndices>
  static std::shared_ptr<DerivedPlanNodeType> CreatePlanNode(const ArgumentsTupleType& arguments_tuple,
                                                             std::index_sequence<ConstructorIndices...> /* indices */) {
    return std::make_shared<DerivedPlanNodeType>(std::get<ConstructorIndices>(arguments_tuple)...);
  }

  template <size_t ArgumentIndex, typename... ArgumentTypes>
  using IsPlanNodeArgument = std::is_convertible<std::tuple_element_t<ArgumentIndex, std::tuple<ArgumentTypes...>>,
                                                 std::shared_ptr<PlanNodeType>>;
};

}  // namespace skyrise
