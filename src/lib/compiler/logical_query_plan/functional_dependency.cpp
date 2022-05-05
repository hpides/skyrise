/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "functional_dependency.hpp"

#include <boost/container_hash/hash.hpp>

namespace skyrise {

FunctionalDependency::FunctionalDependency(ExpressionUnorderedSet init_determinant_expressions,
                                           ExpressionUnorderedSet init_dependent_expressions)
    : determinant_expressions(std::move(init_determinant_expressions)),
      dependent_expressions(std::move(init_dependent_expressions)) {
  DebugAssert(!determinant_expressions.empty() && !dependent_expressions.empty(),
              "FunctionalDependency cannot be empty");
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
    // TODO(julianmenzler): C++20: Replace with .contains
    if (determinant_expressions.find(determinant_expression) == determinant_expressions.cend()) {
      return false;
    }
  }
  // Compare dependants
  for (const auto& dependent_expression : other.dependent_expressions) {
    // TODO(julianmenzler): C++20: Replace with .contains
    if (dependent_expressions.find(dependent_expression) == dependent_expressions.cend()) {
      return false;
    }
  }

  return true;
}

bool FunctionalDependency::operator!=(const FunctionalDependency& other) const { return !(other == *this); }

size_t FunctionalDependency::Hash() const {
  size_t hash = 0;
  for (const auto& expression : determinant_expressions) {
    // To make the hash independent of the expressions' order, we have to use a commutative operator like XOR.
    hash = hash ^ expression->Hash();
  }

  return boost::hash_value(hash - determinant_expressions.size());
}

std::ostream& operator<<(std::ostream& stream, const FunctionalDependency& expression) {
  stream << "{";
  std::vector<std::shared_ptr<AbstractExpression>> determinant_expressions_vector(
      expression.determinant_expressions.cbegin(), expression.determinant_expressions.cend());
  stream << determinant_expressions_vector.at(0)->AsColumnName();
  for (size_t i = 1; i < determinant_expressions_vector.size(); ++i) {
    stream << ", " << determinant_expressions_vector[i]->AsColumnName();
  }

  stream << "} => {";
  std::vector<std::shared_ptr<AbstractExpression>> dependent_expressions_vector(
      expression.dependent_expressions.cbegin(), expression.dependent_expressions.cend());
  stream << dependent_expressions_vector.at(0)->AsColumnName();
  for (size_t i = 1; i < dependent_expressions_vector.size(); ++i) {
    stream << ", " << dependent_expressions_vector[i]->AsColumnName();
  }
  stream << "}";

  return stream;
}

std::unordered_set<FunctionalDependency> InflateFds(const std::vector<FunctionalDependency>& fds) {
  if (fds.empty()) {
    return {};
  }

  auto inflated_fds = std::unordered_set<FunctionalDependency>();
  inflated_fds.reserve(fds.size());

  for (const auto& fd : fds) {
    if (fd.dependent_expressions.size() == 1) {
      inflated_fds.insert(fd);
    } else {
      for (const auto& dependent : fd.dependent_expressions) {
        inflated_fds.emplace(fd.determinant_expressions, ExpressionUnorderedSet{dependent});
      }
    }
  }

  return inflated_fds;
}

std::vector<FunctionalDependency> DeflateFds(const std::vector<FunctionalDependency>& fds) {
  if (fds.empty()) {
    return {};
  }

  std::vector<FunctionalDependency> deflated_fds;
  deflated_fds.reserve(fds.size());

  for (const auto& fd_to_add : fds) {
    auto existing_fd_iterator = std::find_if(deflated_fds.begin(), deflated_fds.end(), [&fd_to_add](auto& fd) {
      // Cannot use unordered_set::operator== because it ignores the custom equality function.
      // https://stackoverflow.com/questions/36167764/can-not-compare-stdunorded-set-with-custom-keyequal

      // Quick check for cardinality
      if (fd.determinant_expressions.size() != fd_to_add.determinant_expressions.size()) {
        return false;
      }

      // Compare determinant_expressions
      for (const auto& expression : fd_to_add.determinant_expressions) {
        // TODO(julianmenzler): C++20: Replace with .contains
        if (fd.determinant_expressions.find(expression) == fd.determinant_expressions.cend()) {
          return false;
        }
      }

      return true;
    });
    if (existing_fd_iterator == deflated_fds.cend()) {
      deflated_fds.push_back(fd_to_add);
    } else {
      // An FD with the same determinant expressions already exists. Therefore, we only have to add to the dependent
      // expressions set
      existing_fd_iterator->dependent_expressions.insert(fd_to_add.dependent_expressions.cbegin(),
                                                         fd_to_add.dependent_expressions.cend());
    }
  }

  return deflated_fds;
}

std::vector<FunctionalDependency> UnionFds(const std::vector<FunctionalDependency>& fds_a,
                                           const std::vector<FunctionalDependency>& fds_b) {
  if constexpr (SKYRISE_DEBUG) {
    auto fds_a_set = std::unordered_set<FunctionalDependency>(fds_a.cbegin(), fds_a.cend());
    auto fds_b_set = std::unordered_set<FunctionalDependency>(fds_b.cbegin(), fds_b.cend());
    Assert(fds_a.size() == fds_a_set.size() && fds_b.size() == fds_b_set.size(),
           "Did not expect input vector to contain multiple FDs with the same determinant expressions");
  }
  if (fds_a.empty()) {
    return fds_b;
  }
  if (fds_b.empty()) {
    return fds_a;
  }

  auto fds_unified = std::vector<FunctionalDependency>();
  fds_unified.reserve(fds_a.size() + fds_b.size());
  fds_unified.insert(fds_unified.end(), fds_a.cbegin(), fds_a.cend());
  fds_unified.insert(fds_unified.end(), fds_b.cbegin(), fds_b.cend());

  // To get rid of potential duplicates, we call deflate before returning.
  return DeflateFds(fds_unified);
}

std::vector<FunctionalDependency> IntersectFds(const std::vector<FunctionalDependency>& fds_a,
                                               const std::vector<FunctionalDependency>& fds_b) {
  if (fds_a.empty() || fds_b.empty()) {
    return {};
  }

  const auto& inflated_fds_a = InflateFds(fds_a);
  const auto& inflated_fds_b = InflateFds(fds_b);

  auto intersected_fds = std::vector<FunctionalDependency>();
  intersected_fds.reserve(fds_a.size());

  for (const auto& fd : inflated_fds_a) {
    // TODO(julianmenzler): C++20: Replace with .contains
    if (inflated_fds_b.find(fd) != inflated_fds_b.cend()) {
      intersected_fds.push_back(fd);
    }
  }

  return DeflateFds(intersected_fds);
}

}  // namespace skyrise

namespace std {

size_t hash<skyrise::FunctionalDependency>::operator()(const skyrise::FunctionalDependency& fd) const {
  return fd.Hash();
}

}  // namespace std
