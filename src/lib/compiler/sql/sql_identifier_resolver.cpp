/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "sql_identifier_resolver.hpp"

#include "sql_identifier_resolver_proxy.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT(google-build-using-namespace)

namespace skyrise {

void SqlIdentifierResolver::AddColumnName(const std::shared_ptr<AbstractExpression>& expression,
                                          const std::string& column_name) {
  auto& entry = FindOrCreateExpressionEntry(expression);
  if (std::find(entry.column_names.begin(), entry.column_names.end(), column_name) == entry.column_names.end()) {
    // This cannot be implemented as a set because the column_names's order would get lost.
    entry.column_names.emplace_back(column_name);
  }
}

void SqlIdentifierResolver::ResetColumnNames(const std::shared_ptr<skyrise::AbstractExpression>& expression) {
  auto entry_iter = std::find_if(entries_.begin(), entries_.end(),
                                 [&](const auto& entry) { return *entry.expression == *expression; });
  if (entry_iter == entries_.end()) return;
  entry_iter->column_names.clear();
}

void SqlIdentifierResolver::SetTableName(const std::shared_ptr<AbstractExpression>& expression,
                                         const std::string& table_name) {
  auto& entry = FindOrCreateExpressionEntry(expression);
  entry.table_name = table_name;
}

std::shared_ptr<AbstractExpression> SqlIdentifierResolver::ResolveIdentifierRelaxed(
    const SqlIdentifier& identifier) const {
  std::vector<std::shared_ptr<AbstractExpression>> matching_expressions;
  for (const auto& entry : entries_) {
    if (identifier.table_name && entry.table_name != identifier.table_name) {
      continue;
    }
    for (const auto& column_name : entry.column_names) {
      if (identifier.column_name == column_name) {
        matching_expressions.emplace_back(entry.expression);
        break;
      }
    }
  }

  if (matching_expressions.size() != 1) return nullptr;  // Identifier is ambiguous/not existing

  return matching_expressions[0];
}

std::vector<SqlIdentifier> SqlIdentifierResolver::GetExpressionIdentifiers(
    const std::shared_ptr<AbstractExpression>& expression) const {
  auto entry_iter = std::find_if(entries_.begin(), entries_.end(),
                                 [&](const auto& entry) { return *entry.expression == *expression; });

  std::vector<SqlIdentifier> identifiers;
  if (entry_iter == entries_.end()) return identifiers;
  for (const auto& column_name : entry_iter->column_names) {
    identifiers.emplace_back(SqlIdentifier(column_name, entry_iter->table_name));
  }
  return identifiers;
}

std::vector<std::shared_ptr<AbstractExpression>> SqlIdentifierResolver::ResolveTableName(
    const std::string& table_name) const {
  std::vector<std::shared_ptr<AbstractExpression>> expressions;
  for (const auto& entry : entries_) {
    if (entry.table_name == table_name) {
      expressions.emplace_back(entry.expression);
    }
  }
  return expressions;
}

void SqlIdentifierResolver::Append(SqlIdentifierResolver&& rhs) {
  entries_.insert(entries_.end(), rhs.entries_.begin(), rhs.entries_.end());
}

SqlIdentifierContextEntry& SqlIdentifierResolver::FindOrCreateExpressionEntry(
    const std::shared_ptr<AbstractExpression>& expression) {
  auto entry_iter = std::find_if(entries_.begin(), entries_.end(),
                                 [&](const auto& entry) { return *entry.expression == *expression; });

  // If there is no entry for this Expression, just add one
  if (entry_iter == entries_.end()) {
    SqlIdentifierContextEntry entry{expression, std::nullopt, {}};
    entry_iter = entries_.emplace(entries_.end(), entry);
  }

  return *entry_iter;
}

}  // namespace skyrise
