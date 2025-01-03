#include "scheduler/coordinator/pqp_pipeline_scheduler.hpp"

#include <random>

#include "gtest/gtest.h"
#include "scheduler/coordinator/lambda_executor.hpp"
#include "scheduler/coordinator/mock_executor.hpp"
#include "utils/time.hpp"

namespace skyrise {

class PqpPipelineSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override { executor_ = std::make_shared<MockExecutor>(); }

 protected:
  static std::shared_ptr<PqpPipeline> GetPipeline(const std::string& pipeline_id) {
    std::string bucket_name = "bucket";
    std::string target_key = "target_key_for_pipeline_" + pipeline_id;
    std::vector<std::string> object_keys = {"a", "b", "c"};
    const ObjectReference object_reference("bucket", "id");
    std::vector<ColumnId> column_ids = {ColumnId{2}, ColumnId{3}};

    auto import_proxy =
        std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{object_reference}, column_ids);
    import_proxy->SetIdentity(pipeline_id);

    const auto pqp = std::make_shared<ExportOperatorProxy>(object_reference, FileFormat::kOrc);
    pqp->SetLeftInput(import_proxy);
    pqp->SetIdentity(pipeline_id);

    std::unordered_map<std::string, std::vector<ObjectReference>> inputs;
    inputs[pipeline_id + "A"].push_back(object_reference);
    auto default_fragment = PipelineFragmentDefinition(inputs, object_reference, FileFormat::kOrc);

    auto pipeline = std::make_shared<PqpPipeline>(pipeline_id, pqp);
    pipeline->AddFragmentDefinition(default_fragment);
    return pipeline;
  }

  void ExecuteAndTest(const std::vector<std::shared_ptr<PqpPipeline>>& pipelines,
                      const std::vector<std::string>& expected_execution_order) {
    const auto scheduler = std::make_shared<PqpPipelineScheduler>(executor_, pipelines);
    scheduler->Execute();
    const auto& execution_order = executor_->GetExecutionOrder();

    EXPECT_EQ(execution_order, expected_execution_order);
  }

  void ExecuteAndTest(const std::vector<std::shared_ptr<PqpPipeline>>& pipelines,
                      const std::set<std::vector<std::string>>& expected_execution_orders) {
    const auto scheduler = std::make_shared<PqpPipelineScheduler>(executor_, pipelines);
    scheduler->Execute();
    const auto& execution_order = executor_->GetExecutionOrder();

    EXPECT_NE(expected_execution_orders.find(execution_order), expected_execution_orders.end());
  }

  std::shared_ptr<MockExecutor> executor_;
};

TEST_F(PqpPipelineSchedulerTest, BasicLinearTest) {
  const auto pipeline_a = GetPipeline("A");
  const auto pipeline_b = GetPipeline("B");
  const auto pipeline_c = GetPipeline("C");
  const auto pipeline_d = GetPipeline("D");

  pipeline_a->SetAsPredecessorOf(pipeline_b);
  pipeline_b->SetAsPredecessorOf(pipeline_c);
  pipeline_c->SetAsPredecessorOf(pipeline_d);

  ExecuteAndTest({pipeline_b, pipeline_d, pipeline_c, pipeline_a}, {"A", "B", "C", "D"});
}

TEST_F(PqpPipelineSchedulerTest, DiamondTest) {
  const auto pipeline_a = GetPipeline("A");
  const auto pipeline_b = GetPipeline("B");
  const auto pipeline_c = GetPipeline("C");
  const auto pipeline_d = GetPipeline("D");

  pipeline_a->SetAsPredecessorOf(pipeline_b);
  pipeline_a->SetAsPredecessorOf(pipeline_c);
  pipeline_b->SetAsPredecessorOf(pipeline_d);
  pipeline_c->SetAsPredecessorOf(pipeline_d);

  ExecuteAndTest({pipeline_b, pipeline_d, pipeline_c, pipeline_a}, {{"A", "B", "C", "D"}, {"A", "C", "B", "D"}});
}

TEST_F(PqpPipelineSchedulerTest, IndependentDelayTest) {
  const auto pipeline_a = GetPipeline("A");
  const auto pipeline_b = GetPipeline("B");
  const auto pipeline_c = GetPipeline("C");
  const auto pipeline_d = GetPipeline("D");

  executor_->pipeline_sleep_ms["A"] = 500;
  executor_->pipeline_sleep_ms["D"] = 2000;

  pipeline_a->SetAsPredecessorOf(pipeline_b);

  // Note: This does not HAVE to be true, since with a lot of bad luck,
  // a could be finished before C etc.
  ExecuteAndTest({pipeline_b, pipeline_d, pipeline_c, pipeline_a}, {"C", "A", "B", "D"});
}

}  // namespace skyrise
