#pragma once
#include <string>

#include <aws/core/utils/json/JsonSerializer.h>

#include "types.hpp"

namespace skyrise {

struct SubqueryFragmentIdentifier {
  Aws::Utils::Json::JsonValue ToJson() const {
    Aws::Utils::Json::JsonValue identifier;

    identifier.WithString("subquery_fragment_id", subquery_fragment_id)
        .WithString("timestamp", timestamp)
        .WithString("query_hash", query_hash)
        .WithInt64("subquery_id", subquery_id)
        .WithInt64("retry_attempt", retry_attempt);

    return identifier;
  }

  std::string subquery_fragment_id;
  std::string timestamp;
  std::string query_hash;
  size_t subquery_id;
  size_t retry_attempt;
};

}  // namespace skyrise
