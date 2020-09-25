#pragma once

#include <functional>

#include "utils/costs/pricing.hpp"

namespace skyrise {

void InitAndShutDownAPI(const std::function<void()>& func);

}  // namespace skyrise
