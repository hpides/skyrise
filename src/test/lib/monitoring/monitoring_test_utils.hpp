#pragma once

#include <functional>

#include "client/client.hpp"

namespace skyrise {

std::pair<std::chrono::time_point<std::chrono::system_clock>, std::chrono::time_point<std::chrono::system_clock>>
InvokeFunction(const std::shared_ptr<Client>& client, const std::string& function_name);

void DeleteFunction(const std::shared_ptr<Client>& client, const std::string& function_name);

}  // namespace skyrise
