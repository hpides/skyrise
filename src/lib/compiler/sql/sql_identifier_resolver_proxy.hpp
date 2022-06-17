/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <vector>

#include "expression/abstract_expression.hpp"

namespace skyrise {

class AbstractExpression;
struct SqlIdentifier;
class SqlIdentifierResolver;
class ParameterIDAllocator;

/**
 * Used during SqlTranslation to resolve identifiers from outer SELECTs in sub-SELECTs.
 * The SqlIdentifierResolverProxy provides and tracks access to a SELECT statement's identifiers from ANY inner
 *   query, no matter how deeply nested.
 *
 * Each nested SELECT is translated by a separate instance of the SqlTranslator. Each SqlTranslator has a
 *   SqlIdentifierResolver to resolve identifiers from its own SELECT-statement and an optional
 *   SqlIdentifierResolverProxy to resolve expressions from any parent SELECT.
 *
 * Consider "SELECT (SELECT t1.a + b FROM t2) FROM t1". The SqlTranslator uses the SqlIdentifierResolverProxy to resolve
 *   "t1.a", since "t1.a" cannot be resolved using its own SqlIdentifierResolver.
 *
 * The ParameterIDAllocator is global to the entire statement translation and makes sure ParameterIDs are unique
 *   across sub queries
 *
 * To be able to access expressions from SELECT-statements above the direct parent statement, the outer_context_proxy
 *   is used. If such a parent-parent query exists, `outer_context_proxy` points to its context. This nesting continues
 *   to any expression from any outer query can be accessed from any inner query and accesses to expressions from outer
 *   queries are tracked by the correct SqlIdentifierResolverProxy.
 */
class SqlIdentifierResolverProxy final {
 public:
  SqlIdentifierResolverProxy(const std::shared_ptr<SqlIdentifierResolver>& wrapped_resolver,
                             const std::shared_ptr<ParameterIDAllocator>& parameter_id_allocator,
                             const std::shared_ptr<SqlIdentifierResolverProxy>& outer_context_proxy = {});

  std::shared_ptr<AbstractExpression> ResolveIdentifierRelaxed(const SqlIdentifier& identifier);

  const ExpressionUnorderedMap<ParameterID>& accessed_expressions() const;

 private:
  std::shared_ptr<SqlIdentifierResolver> wrapped_resolver_;
  std::shared_ptr<ParameterIDAllocator> parameter_id_allocator_;
  std::shared_ptr<SqlIdentifierResolverProxy> outer_context_proxy_;

  // Previously accessed expressions that were already assigned a ParameterID
  ExpressionUnorderedMap<ParameterID> accessed_expressions_;
};

}  // namespace skyrise
