#pragma once

#include "client/base_client.hpp"
#include "compiler/physical_query_plan/pqp_pipeline.hpp"
#include "constants.hpp"

namespace skyrise {

struct PqpPipelineFragmentExecutionResult {
  const std::string pipeline_id;
  const std::string worker_id;
  const bool is_success;
  const std::string message;
  const size_t runtime_ms;
  const size_t function_instance_size_mb;
  const size_t export_data_size_bytes;
  const Aws::Utils::Json::JsonValue metering;
};

class AbstractPqpPipelineFragmentExecutor {
 public:
  virtual ~AbstractPqpPipelineFragmentExecutor() = default;

  virtual void Execute(
      const std::shared_ptr<PqpPipeline>& pqp_pipeline,
      const std::function<void(std::shared_ptr<PqpPipelineFragmentExecutionResult>)>& on_finished_callback) = 0;
};

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class LambdaExecutor : public AbstractPqpPipelineFragmentExecutor {
 public:
  explicit LambdaExecutor(const std::shared_ptr<const BaseClient>& client,
                          const std::string& worker_function_name = kWorkerFunctionName);
  ~LambdaExecutor() override;

  void Execute(
      const std::shared_ptr<PqpPipeline>& pqp_pipeline,
      const std::function<void(std::shared_ptr<PqpPipelineFragmentExecutionResult>)>& on_finished_callback) override;

 private:
  void SetupSqsResponseQueue();
  static void CollectSqsMessages(
      const std::shared_ptr<const Aws::SQS::SQSClient> sqs_client, const std::string sqs_response_queue_url,
      const std::string pipeline_id, const size_t invocation_count,
      const std::function<void(std::shared_ptr<PqpPipelineFragmentExecutionResult>)> on_finished_callback);

  const std::shared_ptr<const BaseClient> client_;
  const std::string worker_function_name_;
  std::string sqs_response_queue_url_;
  std::vector<std::thread> collector_threads_;
};

}  // namespace skyrise
