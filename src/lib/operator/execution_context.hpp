#pragma once

#include "client/base_client.hpp"
#include "scheduler/worker/fragment_scheduler.hpp"
#include "storage/backend/abstract_storage.hpp"

namespace skyrise {

class OperatorExecutionContext {
 public:
  OperatorExecutionContext(
      std::function<std::shared_ptr<BaseClient>(void)>&& get_client_callback,
      std::function<std::shared_ptr<Storage>(const std::string& storage_name)>&& get_storage_callback,
      std::function<std::shared_ptr<FragmentScheduler>(void)>&& get_scheduler_callback);
  std::shared_ptr<BaseClient> GetClient() const;
  std::shared_ptr<Storage> GetStorage(const std::string& storage_name) const;
  std::shared_ptr<FragmentScheduler> GetScheduler() const;

 private:
  std::function<std::shared_ptr<BaseClient>(void)> get_client_callback_;
  std::function<std::shared_ptr<Storage>(const std::string& storage_name)> get_storage_callback_;
  std::function<std::shared_ptr<FragmentScheduler>(void)> get_scheduler_callback_;
};

}  // namespace skyrise
