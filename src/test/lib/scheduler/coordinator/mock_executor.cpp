#include "mock_executor.hpp"

#include <mutex>
#include <thread>
#include <vector>

namespace skyrise {

void MockExecutor::Execute(
    const std::shared_ptr<PqpPipeline>& pqp_pipeline,
    const std::function<void(std::shared_ptr<PqpPipelineFragmentExecutionResult>)>& on_finished_callback) {
  std::thread t([pqp_pipeline, on_finished_callback, this]() {
    if (pipeline_sleep_ms[pqp_pipeline->Identity()]) {
      std::this_thread::sleep_for(std::chrono::milliseconds(pipeline_sleep_ms[pqp_pipeline->Identity()]));
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      pipeline_execution_order_.push_back(pqp_pipeline->Identity());
    }
    Aws::Utils::Json::JsonValue response;
    response.WithString("message", "Successful: All Operators executed successfully.").WithInteger("is_success", 1);
    for (size_t i = 0; i < pqp_pipeline->FragmentDefinitions().size(); ++i) {
      const std::string worker_id = pqp_pipeline->Identity() + "_" + std::to_string(i);
      const auto fragment_result = std::make_shared<PqpPipelineFragmentExecutionResult>(
          PqpPipelineFragmentExecutionResult{pqp_pipeline->Identity(), worker_id, true, "", 1, 1, 1, response});
      on_finished_callback(fragment_result);
    }
  });
  t.detach();
};
}  // namespace skyrise
