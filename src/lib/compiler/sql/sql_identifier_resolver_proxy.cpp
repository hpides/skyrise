#include "sql_identifier_resolver_proxy.hpp"

#include "sql_identifier_resolver.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT(google-build-using-namespace)

namespace skyrise {

SqlIdentifierResolverProxy::SqlIdentifierResolverProxy(
    const std::shared_ptr<SqlIdentifierResolver>& wrapped_resolver,
    const std::shared_ptr<ParameterIDAllocator>& parameter_id_allocator,
    const std::shared_ptr<SqlIdentifierResolverProxy>& outer_context_proxy)
    : wrapped_resolver_(wrapped_resolver),
      parameter_id_allocator_(parameter_id_allocator),
      outer_context_proxy_(outer_context_proxy) {}

std::shared_ptr<AbstractExpression> SqlIdentifierResolverProxy::ResolveIdentifierRelaxed(
    const SqlIdentifier& identifier) {
  auto expression = wrapped_resolver_->ResolveIdentifierRelaxed(identifier);
  if (expression) {
    Fail("CorrelatedParameterExpression is currently unsupported. For code examples, see Hyrise codebase.");
  } else {
    if (outer_context_proxy_) return outer_context_proxy_->ResolveIdentifierRelaxed(identifier);
  }

  return nullptr;
}

const ExpressionUnorderedMap<ParameterID>& SqlIdentifierResolverProxy::accessed_expressions() const {
  return accessed_expressions_;
}

}  // namespace skyrise
