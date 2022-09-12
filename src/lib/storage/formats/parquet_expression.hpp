#pragma once

#include <arrow/dataset/scanner.h>

#include "expression/abstract_expression.hpp"

namespace skyrise {

/**
 * Convert an expression from Skyrise into an Arrow expression. The function supports expressions that are suitable for
 * pushdown of filter and join conditions into the ParquetFormatReader such as binary predicates (>, < etc.),
 * in-between predicates, null checks and logical operator. They can be checked against columns or value literals.
 * It throws an exception if the conversion was not successful.
 *
 * @param expression AbstractExpression that should be converted into an arrow expression
 */
arrow::compute::Expression CreateArrowExpression(const std::shared_ptr<AbstractExpression>& expression);

}  // namespace skyrise
