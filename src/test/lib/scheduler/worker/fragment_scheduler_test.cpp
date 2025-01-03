#include "scheduler/worker/fragment_scheduler.hpp"

#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "operator/table_wrapper.hpp"
#include "scheduler/worker/generic_task.hpp"
#include "scheduler/worker/operator_task.hpp"

namespace skyrise {

class MockOperator : public AbstractOperator {
 public:
  using AbstractOperator::AbstractOperator;

  const std::string& Name() const override {
    static const std::string kName = "MockOperator";
    return kName;
  };

  std::shared_ptr<const Table> OnExecute(const std::shared_ptr<OperatorExecutionContext>& /*context*/) override {
    EXPECT_FALSE(was_executed_);
    was_executed_ = true;
    return std::make_shared<const Table>(TableColumnDefinitions());
  }

  bool WasExecuted() const { return was_executed_; }

 private:
  bool was_executed_ = false;
};

class FragmentSchedulerTest : public ::testing::Test {
 protected:
  static void StressLinearDependencies(std::atomic_uint32_t* counter,
                                       const std::shared_ptr<FragmentScheduler>& scheduler) {
    auto task_1 = std::make_shared<GenericTask>([counter]() {
      uint32_t current_value = 0;
      const bool successful = counter->compare_exchange_strong(current_value, 1);
      ASSERT_TRUE(successful);
    });
    auto task_2 = std::make_shared<GenericTask>([counter]() {
      uint32_t current_value = 1;
      const bool successful = counter->compare_exchange_strong(current_value, 2);
      ASSERT_TRUE(successful);
    });
    auto task_3 = std::make_shared<GenericTask>([counter]() {
      uint32_t current_value = 2;
      const bool successful = counter->compare_exchange_strong(current_value, 3);
      ASSERT_TRUE(successful);
    });

    task_1->SetAsPredecessorOf(task_2);
    task_2->SetAsPredecessorOf(task_3);

    scheduler->Schedule(task_3);
    scheduler->Schedule(task_1);
    scheduler->Schedule(task_2);
  }

  static void StressMultipleDependencies(std::atomic_uint32_t* counter,
                                         const std::shared_ptr<FragmentScheduler>& scheduler) {
    auto task_1 = std::make_shared<GenericTask>([counter]() { (*counter) += 1; });
    auto task_2 = std::make_shared<GenericTask>([counter]() { (*counter) += 2; });
    auto task_3 = std::make_shared<GenericTask>([counter]() {
      uint32_t current_value = 3;
      const bool successful = counter->compare_exchange_strong(current_value, 4);
      ASSERT_TRUE(successful);
    });

    task_1->SetAsPredecessorOf(task_3);
    task_2->SetAsPredecessorOf(task_3);

    scheduler->Schedule(task_3);
    scheduler->Schedule(task_1);
    scheduler->Schedule(task_2);
  }

  static void StressDiamondDependencies(std::atomic_uint32_t* counter,
                                        const std::shared_ptr<FragmentScheduler>& scheduler) {
    auto task_1 = std::make_shared<GenericTask>([counter]() {
      uint32_t current_value = 0;
      const bool successful = counter->compare_exchange_strong(current_value, 1);
      ASSERT_TRUE(successful);
    });
    auto task_2 = std::make_shared<GenericTask>([counter]() { (*counter) += 2; });
    auto task_3 = std::make_shared<GenericTask>([counter]() { (*counter) += 3; });
    auto task_4 = std::make_shared<GenericTask>([counter]() {
      uint32_t current_value = 6;
      const bool successful = counter->compare_exchange_strong(current_value, 7);
      ASSERT_TRUE(successful);
    });

    task_1->SetAsPredecessorOf(task_2);
    task_1->SetAsPredecessorOf(task_3);
    task_2->SetAsPredecessorOf(task_4);
    task_3->SetAsPredecessorOf(task_4);

    scheduler->Schedule(task_4);
    scheduler->Schedule(task_3);
    scheduler->Schedule(task_1);
    scheduler->Schedule(task_2);
  }

  static void IncrementCounterInSubtasks(std::atomic_uint32_t* counter,
                                         const std::shared_ptr<FragmentScheduler>& scheduler) {
    std::vector<std::shared_ptr<AbstractTask>> tasks;
    tasks.reserve(10);

    for (size_t i = 0; i < 10; ++i) {
      auto task = std::make_shared<GenericTask>([&, counter]() {
        std::vector<std::shared_ptr<AbstractTask>> jobs;
        for (size_t j = 0; j < 3; ++j) {
          auto job = std::make_shared<GenericTask>([&]() { (*counter)++; });

          scheduler->Schedule(job);
          jobs.emplace_back(job);
        }

        scheduler->WaitForTasks(jobs);
      });
      scheduler->Schedule(task);
      tasks.emplace_back(task);
    }
  }
};

/**
 * Schedule some tasks with subtasks, make sure all of them finish
 */
TEST_F(FragmentSchedulerTest, BasicTest) {
  auto scheduler = std::make_shared<FragmentScheduler>();

  std::atomic_uint32_t counter = 0;

  IncrementCounterInSubtasks(&counter, scheduler);
  scheduler->WaitForAllTasks();

  ASSERT_EQ(counter, 30u);
}

TEST_F(FragmentSchedulerTest, LinearDependenciesWithScheduler) {
  auto scheduler = std::make_shared<FragmentScheduler>();

  std::atomic_uint32_t counter = 0;

  StressLinearDependencies(&counter, scheduler);

  scheduler->WaitForAllTasks();

  ASSERT_EQ(counter, 3);
}

TEST_F(FragmentSchedulerTest, MultipleDependenciesWithScheduler) {
  auto scheduler = std::make_shared<FragmentScheduler>();

  std::atomic_uint32_t counter = 0;

  StressMultipleDependencies(&counter, scheduler);

  scheduler->WaitForAllTasks();

  ASSERT_EQ(counter, 4);
}

TEST_F(FragmentSchedulerTest, DiamondDependenciesWithScheduler) {
  auto scheduler = std::make_shared<FragmentScheduler>();

  std::atomic_uint32_t counter = 0;

  StressDiamondDependencies(&counter, scheduler);

  scheduler->WaitForAllTasks();

  ASSERT_EQ(counter, 7);
}

TEST_F(FragmentSchedulerTest, MultipleOperators) {
  auto op1 = std::make_shared<MockOperator>(OperatorType::kImport);
  auto op2 = std::make_shared<MockOperator>(OperatorType::kFilter, op1);
  auto op3 = std::make_shared<MockOperator>(OperatorType::kFilter, op1);
  auto root_operator = std::make_shared<MockOperator>(OperatorType::kExport, op2, op3);

  auto tasks = OperatorTask::GenerateTasksFromOperator(root_operator, nullptr);

  const std::vector<std::shared_ptr<AbstractTask>>& all_operator_tasks = tasks.first;
  EXPECT_EQ(all_operator_tasks.size(), 4);

  auto scheduler = std::make_shared<FragmentScheduler>();
  scheduler->ScheduleAndWaitForTasks(all_operator_tasks);

  EXPECT_TRUE(op1->WasExecuted());
  EXPECT_TRUE(op2->WasExecuted());
  EXPECT_TRUE(op3->WasExecuted());
  EXPECT_TRUE(root_operator->WasExecuted());
}

TEST_F(FragmentSchedulerTest, SingleWorkerGuaranteeProgress) {
  auto scheduler = std::make_shared<FragmentScheduler>(1);

  auto task_done = false;
  auto task = std::make_shared<GenericTask>([&]() {
    auto subtask = std::make_shared<GenericTask>([&]() { task_done = true; });

    scheduler->Schedule(subtask);
    scheduler->WaitForTasks(std::vector<std::shared_ptr<AbstractTask>>{subtask});
  });

  scheduler->Schedule(task);
  scheduler->WaitForAllTasks();
  EXPECT_TRUE(task_done);
}

TEST_F(FragmentSchedulerTest, WaitForTasks) {
  bool task_1_is_finished = false;
  auto task_1 = std::make_shared<GenericTask>([&task_1_is_finished]() { task_1_is_finished = true; });

  bool is_running = true;
  bool task_2_is_finished = false;
  auto task_2 = std::make_shared<GenericTask>([&is_running, &task_2_is_finished]() {
    // NOLINTNEXTLINE(bugprone-infinite-loop)
    while (is_running) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    };

    task_2_is_finished = true;
  });

  auto scheduler = std::make_shared<FragmentScheduler>();
  scheduler->Schedule(task_2);
  scheduler->ScheduleAndWaitForTasks({task_1});

  EXPECT_TRUE(task_1_is_finished);
  EXPECT_FALSE(task_2_is_finished);
  is_running = false;
}

}  // namespace skyrise
