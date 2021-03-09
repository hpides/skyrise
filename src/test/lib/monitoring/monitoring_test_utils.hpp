#pragma once

#include <functional>

#include "client/client.hpp"

namespace skyrise {

void ExecuteInsideAPI(const std::function<void()>& function);

void UploadFunction(const std::shared_ptr<Client>& client, const std::string& package_name,
                    const std::string& function_name, const std::string& role_name, bool enable_tracing = false);

std::pair<std::chrono::time_point<std::chrono::system_clock>, std::chrono::time_point<std::chrono::system_clock>>
InvokeFunction(const std::shared_ptr<Client>& client, const std::string& function_name);

void DeleteFunction(const std::shared_ptr<Client>& client, const std::string& function_name);

}  // namespace skyrise
