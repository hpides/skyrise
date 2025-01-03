#include <map>
#include <mutex>
#include <vector>

#include "scheduler/coordinator/lambda_executor.hpp"

namespace skyrise {

class MockExecutor : public AbstractPqpPipelineFragmentExecutor {
 public:
  std::map<std::string, size_t> pipeline_sleep_ms;

  std::vector<std::string> GetExecutionOrder() {
    std::vector<std::string> tmp = pipeline_execution_order_;
    pipeline_execution_order_.clear();
    return tmp;
  }

  void Execute(
      const std::shared_ptr<PqpPipeline>& pqp_pipeline,
      const std::function<void(std::shared_ptr<PqpPipelineFragmentExecutionResult>)>& on_finished_callback) override;

 private:
  std::vector<std::string> pipeline_execution_order_;
  std::mutex mutex_;
};

}  // namespace skyrise
