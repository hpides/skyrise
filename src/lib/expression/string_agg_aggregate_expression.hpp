#pragma once

#include <optional>

#include "aggregate_expression.hpp"

namespace skyrise {

class StringAggAggregateExpression : public AggregateExpression {
 public:
  explicit StringAggAggregateExpression(
      const std::shared_ptr<AbstractExpression>& argument,
      const std::optional<std::vector<SortColumnDefinition>>& sort_column_definition = std::nullopt);

  bool OrderWithinGroup() const;

  const std::vector<SortColumnDefinition>& SortColumnDefinitions() const;

 private:
  const std::optional<std::vector<SortColumnDefinition>> sort_column_definitions_;
};

}  // namespace skyrise
