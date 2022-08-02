#include "abstract_exchange_strategy.hpp"

#include <boost/container_hash/hash.hpp>

namespace skyrise {

AbstractExchangeStrategy::AbstractExchangeStrategy(const ExchangeStrategyType type)
    : type_(type) {}

ExchangeStrategyType AbstractExchangeStrategy::Type() const { return type_; }

size_t AbstractExchangeStrategy::Hash() const {
  size_t hash = boost::hash_value(type_);
  boost::hash_combine(hash, ShallowHash());
  return hash;
}

}  // namespace skyrise
