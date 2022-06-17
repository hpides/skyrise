/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "sql_identifier.hpp"

namespace skyrise {

class AbstractExpression;

struct SqlIdentifierContextEntry final {
  std::shared_ptr<AbstractExpression> expression;
  std::optional<std::string> table_name;
  std::vector<std::string> column_names;
};

/**
 * Used during SQL translation to obtain the expression an identifier refers to.
 * Manages column/table aliases.
 */
class SqlIdentifierResolver final {
 public:
  /**
   * @{
   * Set/Update/Delete the column/table names of an expression. There can be multiple column names referring to a single
   * expression because a new alias does not replace a former column name or alias.
   */
  void AddColumnName(const std::shared_ptr<AbstractExpression>& expression, const std::string& column_name);
  void ResetColumnNames(const std::shared_ptr<AbstractExpression>& expression);
  void SetTableName(const std::shared_ptr<AbstractExpression>& expression, const std::string& table_name);
  /** @} */

  /**
   * Resolve the expression that an SqlIdentifier refers to.
   * @return    The expression referenced to by @param identifier.
   *            nullptr, if no or multiple such expressions exist
   */
  std::shared_ptr<AbstractExpression> ResolveIdentifierRelaxed(const SqlIdentifier& identifier) const;

  /**
   * Resolve the identifiers of an @param expression
   * @return    The SqlIdentifiers
   */
  std::vector<SqlIdentifier> GetExpressionIdentifiers(const std::shared_ptr<AbstractExpression>& expression) const;

  /**
   * @return   The column expressions of a table/subquery identified by @param table_name.
   */
  std::vector<std::shared_ptr<AbstractExpression>> ResolveTableName(const std::string& table_name) const;

  /**
   * Move all entries from another resolver @param rhs into this resolver
   */
  void Append(SqlIdentifierResolver&& rhs);

 private:
  SqlIdentifierContextEntry& FindOrCreateExpressionEntry(const std::shared_ptr<AbstractExpression>& expression);

  std::vector<SqlIdentifierContextEntry> entries_;
};

}  // namespace skyrise
