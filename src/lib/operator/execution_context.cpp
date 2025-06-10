#include "execution_context.hpp"

namespace skyrise {

OperatorExecutionContext::OperatorExecutionContext(
    std::function<std::shared_ptr<BaseClient>(void)>&& get_client_callback,
    std::function<std::shared_ptr<Storage>(const std::string& storage_name)>&& get_storage_callback,
    std::function<std::shared_ptr<FragmentScheduler>(void)>&& get_scheduler_callback)
    : get_client_callback_(std::move(get_client_callback)),
      get_storage_callback_(std::move(get_storage_callback)),
      get_scheduler_callback_(std::move(get_scheduler_callback)) {}

std::shared_ptr<BaseClient> OperatorExecutionContext::GetClient() const { return get_client_callback_(); }
std::shared_ptr<Storage> OperatorExecutionContext::GetStorage(const std::string& storage_name) const {
  return get_storage_callback_(storage_name);
}
std::shared_ptr<FragmentScheduler> OperatorExecutionContext::GetScheduler() const { return get_scheduler_callback_(); }

}  // namespace skyrise
