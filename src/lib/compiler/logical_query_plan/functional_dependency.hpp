/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "expression/abstract_expression.hpp"

namespace skyrise {

/**
 * Models a functional dependency (FD), which consists out of two sets of expressions.
 * The left set of expressions (determinants) unambigiously identifies the right set (dependents):
 * {Left} => {Right}
 *
 * Example A:
 * Think of a table with three columns: (Semester, CourseID, Lecturer)
 * The primary key is defined across the first two columns, which leads to the following FD:
 * {Semester, CourseID} => {Lecturer}
 *
 * Example B:
 * Think of a table with four columns: (ISBN, Genre, Author, Author-Nationality)
 * The primary key {ISBN} identifies all other columns. Therefore, we get the following FDs:
 * {ISBN} => {Author}
 * {ISBN} => {Genre}
 * {ISBN} => {Author}
 * {ISBN} => {Author-Nationality}
 * Furthermore, knowing an author, we also know his nationality. Hence, we can specify another FD that applies:
 * {Author} => {Author-Nationality}
 *
 * Currently, the determinant expressions are required to be non-nullable to be involved in FDs.
 * Combining null values and FDs is not trivial. For more reference, see https://arxiv.org/abs/1404.4963.
 */
struct FunctionalDependency {
  FunctionalDependency(ExpressionUnorderedSet init_determinant_expressions,
                       ExpressionUnorderedSet init_dependent_expressions);

  bool operator==(const FunctionalDependency& other) const;
  bool operator!=(const FunctionalDependency& other) const;
  size_t Hash() const;

  ExpressionUnorderedSet determinant_expressions;
  ExpressionUnorderedSet dependent_expressions;
};

std::ostream& operator<<(std::ostream& stream, const FunctionalDependency& functional_dependency);

/**
 * @return The given FDs as an unordered set in an inflated form.
 *         We consider FDs as inflated when they have a single dependent expression only. Therefore, inflating an FD
 *         works as follows:
 *                                                      {a} => {b}
 *                             {a} => {b, c, d}   -->   {a} => {c}
 *                                                      {a} => {d}
 */
std::unordered_set<FunctionalDependency> InflateFunctionalDependencies(
    const std::vector<FunctionalDependency>& functional_dependencies);

/**
 * @return Reduces the given vector of FDs, so that there are no more FD objects with the same determinant expressions.
 *         As a result, FDs become deflated as follows:
 *
 *                             {a} => {b}
 *                             {a} => {c}         -->   {a} => {b, c, d}
 *                             {a} => {d}
 */
std::vector<FunctionalDependency> DeflateFunctionalDependencies(
    const std::vector<FunctionalDependency>& functional_dependencies);

/**
 * @return Unified FDs from the given @param functional_dependencies_a and @param functional_dependencies_b vectors. FDs
 * with the same determinant expressions are merged into single objects by merging their dependent expressions.
 */
std::vector<FunctionalDependency> UnionFunctionalDependencies(
    const std::vector<FunctionalDependency>& functional_dependencies_a,
    const std::vector<FunctionalDependency>& functional_dependencies_b);

/**
 * @return Returns FDs that are included in both of the given vectors.
 */
std::vector<FunctionalDependency> IntersectFunctionalDependencies(
    const std::vector<FunctionalDependency>& functional_dependencies_a,
    const std::vector<FunctionalDependency>& functional_dependencies_b);

/**
 * Future Work: Transitive FDs
 * Given two or more FDs, it might become possible to derive transitive FDs from them.
 * For example: {a} => {b} and
 *              {b} => {c} lead to the following transitive FD: {a} => {c}
 * To check for transitive FDs, we could provide a function called
 * `functional_dependencies_apply(functional_dependencies, dependent, dependee)` that takes a set of FDs and two
 * expressions to see if dependee is dependent on dependent.
 */

}  // namespace skyrise

namespace std {

/**
 * Please note: FDs with the same determinant expressions are expected to be merged into single FD objects (e.g. for
 * unordered sets). Therefore, we hash the determinant expressions only.
 */
template <>
struct hash<skyrise::FunctionalDependency> {
  size_t operator()(const skyrise::FunctionalDependency& functional_dependency) const;
};

}  // namespace std
