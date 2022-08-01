#include "abstract_exchange_strategy.hpp"

namespace skyrise {

AbstractExchangeStrategy::AbstractExchangeStrategy(const ExchangeStrategyType type)
    : type_(type) {}

ExchangeStrategyType AbstractExchangeStrategy::Type() const { return type_; }

}  // namespace skyrise
