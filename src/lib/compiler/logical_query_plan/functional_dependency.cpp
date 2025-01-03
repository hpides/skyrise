/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "functional_dependency.hpp"

#include <algorithm>

#include <boost/container_hash/hash.hpp>

namespace skyrise {

FunctionalDependency::FunctionalDependency(ExpressionUnorderedSet init_determinant_expressions,
                                           ExpressionUnorderedSet init_dependent_expressions)
    : determinant_expressions(std::move(init_determinant_expressions)),
      dependent_expressions(std::move(init_dependent_expressions)) {
  DebugAssert(!determinant_expressions.empty() && !dependent_expressions.empty(),
              "FunctionalDependency cannot be empty.");
}

bool FunctionalDependency::operator==(const FunctionalDependency& other) const {
  // Cannot use unordered_set::operator== because it ignores the custom equality function.
  // https://stackoverflow.com/questions/36167764/can-not-compare-stdunorded-set-with-custom-keyequal

  // Quick check for cardinality
  if (determinant_expressions.size() != other.determinant_expressions.size() ||
      dependent_expressions.size() != other.dependent_expressions.size()) {
    return false;
  }

  // Compare determinant_expressions
  for (const auto& determinant_expression : other.determinant_expressions) {
    if (!determinant_expressions.contains(determinant_expression)) {
      return false;
    }
  }
  // Compare dependants
  return std::all_of(other.dependent_expressions.cbegin(), other.dependent_expressions.cend(),
                     [this](const std::shared_ptr<AbstractExpression>& dependent_expression) {
                       return dependent_expressions.find(dependent_expression) == dependent_expressions.cend();
                     });
}

bool FunctionalDependency::operator!=(const FunctionalDependency& other) const { return !(other == *this); }

size_t FunctionalDependency::Hash() const {
  size_t hash = 0;
  for (const auto& determinant_expression : determinant_expressions) {
    // To make the hash independent of the expressions' order, we have to use a commutative operator like XOR.
    hash = hash ^ determinant_expression->Hash();
  }

  return boost::hash_value(hash - determinant_expressions.size());
}

std::ostream& operator<<(std::ostream& stream, const FunctionalDependency& functional_dependency) {
  stream << "{";

  std::vector<std::shared_ptr<AbstractExpression>> determinant_expressions(
      functional_dependency.determinant_expressions.cbegin(), functional_dependency.determinant_expressions.cend());
  stream << determinant_expressions.front()->AsColumnName();
  for (size_t i = 1; i < determinant_expressions.size(); ++i) {
    stream << ", " << determinant_expressions[i]->AsColumnName();
  }

  stream << "} => {";

  std::vector<std::shared_ptr<AbstractExpression>> dependent_expressions(
      functional_dependency.dependent_expressions.cbegin(), functional_dependency.dependent_expressions.cend());
  stream << dependent_expressions.front()->AsColumnName();
  for (size_t i = 1; i < dependent_expressions.size(); ++i) {
    stream << ", " << dependent_expressions[i]->AsColumnName();
  }

  stream << "}";

  return stream;
}

std::unordered_set<FunctionalDependency> InflateFunctionalDependencies(
    const std::vector<FunctionalDependency>& functional_dependencies) {
  if (functional_dependencies.empty()) {
    return {};
  }

  std::unordered_set<FunctionalDependency> inflated_functional_dependencies;
  inflated_functional_dependencies.reserve(functional_dependencies.size());

  for (const auto& functional_dependency : functional_dependencies) {
    if (functional_dependency.dependent_expressions.size() == 1) {
      inflated_functional_dependencies.insert(functional_dependency);
    } else {
      for (const auto& dependent_expression : functional_dependency.dependent_expressions) {
        inflated_functional_dependencies.emplace(functional_dependency.determinant_expressions,
                                                 ExpressionUnorderedSet{dependent_expression});
      }
    }
  }

  return inflated_functional_dependencies;
}

std::vector<FunctionalDependency> DeflateFunctionalDependencies(
    const std::vector<FunctionalDependency>& functional_dependencies) {
  if (functional_dependencies.empty()) {
    return {};
  }

  std::vector<FunctionalDependency> deflated_functional_dependencies;
  deflated_functional_dependencies.reserve(functional_dependencies.size());

  for (const auto& functional_dependency_to_add : functional_dependencies) {
    auto existing_functional_dependency_iterator =
        std::find_if(deflated_functional_dependencies.begin(), deflated_functional_dependencies.end(),
                     [&functional_dependency_to_add](auto& functional_dependency) {
                       // Cannot use unordered_set::operator== because it ignores the custom equality function.
                       // https://stackoverflow.com/questions/36167764/can-not-compare-stdunorded-set-with-custom-keyequal

                       // Check for cardinality
                       if (functional_dependency.determinant_expressions.size() !=
                           functional_dependency_to_add.determinant_expressions.size()) {
                         return false;
                       }

                       // Compare determinant_expressions
                       // NOLINTNEXTLINE(readability-use-anyofallof)
                       for (const auto& determinant_expression : functional_dependency_to_add.determinant_expressions) {
                         if (!functional_dependency.determinant_expressions.contains(determinant_expression)) {
                           return false;
                         }
                       }

                       return true;
                     });
    if (existing_functional_dependency_iterator == deflated_functional_dependencies.cend()) {
      deflated_functional_dependencies.push_back(functional_dependency_to_add);
    } else {
      // An FD with the same determinant expressions already exists. Therefore, we only have to add to the dependent
      // expressions set
      existing_functional_dependency_iterator->dependent_expressions.insert(
          functional_dependency_to_add.dependent_expressions.cbegin(),
          functional_dependency_to_add.dependent_expressions.cend());
    }
  }

  return deflated_functional_dependencies;
}

std::vector<FunctionalDependency> UnionFunctionalDependencies(
    const std::vector<FunctionalDependency>& functional_dependencies_a,
    const std::vector<FunctionalDependency>& functional_dependencies_b) {
  if constexpr (SKYRISE_DEBUG) {
    const std::unordered_set<FunctionalDependency> functional_dependencies_a_set(functional_dependencies_a.cbegin(),
                                                                                 functional_dependencies_a.cend());
    const std::unordered_set<FunctionalDependency> functional_dependencies_b_set(functional_dependencies_b.cbegin(),
                                                                                 functional_dependencies_b.cend());
    Assert(functional_dependencies_a.size() == functional_dependencies_a_set.size() &&
               functional_dependencies_b.size() == functional_dependencies_b_set.size(),
           "Did not expect input vector to contain multiple FDs with the same determinant expressions.");
  }
  if (functional_dependencies_a.empty()) {
    return functional_dependencies_b;
  }
  if (functional_dependencies_b.empty()) {
    return functional_dependencies_a;
  }

  std::vector<FunctionalDependency> unified_functional_dependencies;
  unified_functional_dependencies.reserve(functional_dependencies_a.size() + functional_dependencies_b.size());
  unified_functional_dependencies.insert(unified_functional_dependencies.end(), functional_dependencies_a.cbegin(),
                                         functional_dependencies_a.cend());
  unified_functional_dependencies.insert(unified_functional_dependencies.end(), functional_dependencies_b.cbegin(),
                                         functional_dependencies_b.cend());

  // To get rid of potential duplicates, we call deflate before returning.
  return DeflateFunctionalDependencies(unified_functional_dependencies);
}

std::vector<FunctionalDependency> IntersectFunctionalDependencies(
    const std::vector<FunctionalDependency>& functional_dependencies_a,
    const std::vector<FunctionalDependency>& functional_dependencies_b) {
  if (functional_dependencies_a.empty() || functional_dependencies_b.empty()) {
    return {};
  }

  const auto inflated_functional_dependencies_a = InflateFunctionalDependencies(functional_dependencies_a);
  const auto inflated_functional_dependencies_b = InflateFunctionalDependencies(functional_dependencies_b);

  std::vector<FunctionalDependency> intersected_functional_dependencies;
  intersected_functional_dependencies.reserve(functional_dependencies_a.size());

  for (const auto& functional_dependency : inflated_functional_dependencies_a) {
    if (inflated_functional_dependencies_b.contains(functional_dependency)) {
      intersected_functional_dependencies.push_back(functional_dependency);
    }
  }

  return DeflateFunctionalDependencies(intersected_functional_dependencies);
}

}  // namespace skyrise

namespace std {

size_t hash<skyrise::FunctionalDependency>::operator()(
    const skyrise::FunctionalDependency& functional_dependency) const {
  return functional_dependency.Hash();
}

}  // namespace std
