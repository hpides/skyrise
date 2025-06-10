#include "compiler/physical_query_plan/pqp_pipeline.hpp"

#include <string>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_utils.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class PqpPipelineTest : public ::testing::Test {
 public:
  void SetUp() override {
    import_objects_.emplace_back("bucket_a", "obj.orc");
    column_ids_ = {ColumnId{0}, ColumnId{1}};
    const auto a_a = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a_a");
    const auto a_b = PqpColumn_(ColumnId{1}, DataType::kLong, false, "a_b");
    import_proxy_ = ImportOperatorProxy::Make(import_objects_, column_ids_);
    import_proxy_->SetIdentity(kImportIdentity);

    // clang-format off
    pipeline_plan_ =
    ExportOperatorProxy::Dummy(
      FilterOperatorProxy::Make(GreaterThan_(a_a, a_b),
        import_proxy_));
    // clang-format on

    pipeline_ = std::make_shared<PqpPipeline>(kPipelineIdentity, pipeline_plan_);
  }

 protected:
  inline static const std::string kPipelineIdentity = "p_1";
  inline static const std::string kImportIdentity = "custom_import_identity";
  std::shared_ptr<AbstractOperatorProxy> pipeline_plan_;
  std::shared_ptr<ImportOperatorProxy> import_proxy_;
  std::shared_ptr<PqpPipeline> pipeline_;
  std::vector<ObjectReference> import_objects_;
  std::vector<ColumnId> column_ids_;
};

TEST_F(PqpPipelineTest, PrefixOperatorProxyIdentities) {
  EXPECT_EQ(pipeline_->Identity(), kPipelineIdentity);

  const auto& template_plan = pipeline_->PqpPipelineFragmentTemplate()->TemplatedPlan();
  EXPECT_NE(template_plan, pipeline_plan_);

  VisitPqp(template_plan, [&](const auto& operator_proxy) {
    EXPECT_TRUE(boost::starts_with(operator_proxy->Identity(), kPipelineIdentity));
    return PqpVisitation::kVisitInputs;
  });
}

TEST_F(PqpPipelineTest, ValidateFragmentDefinitions) {
  ASSERT_TRUE(pipeline_->FragmentDefinitions().empty());
  std::unordered_map<std::string, std::vector<ObjectReference>> identity_to_objects;

  // Exception is expected when the import proxy identity is not prefixed with the pipeline's identity.
  identity_to_objects.emplace(kImportIdentity, import_objects_);
  const PipelineFragmentDefinition fragment_definition(identity_to_objects, ObjectReference("bucket", "target.csv"),
                                                       FileFormat::kCsv);
  EXPECT_THROW(pipeline_->AddFragmentDefinition(fragment_definition), std::logic_error);
}

TEST_F(PqpPipelineTest, AddFragmentDefinitions) {
  ASSERT_TRUE(pipeline_->FragmentDefinitions().empty());
  const std::string import_identity = kPipelineIdentity + import_proxy_->Identity();
  std::unordered_map<std::string, std::vector<ObjectReference>> identity_to_objects;
  identity_to_objects.emplace(import_identity, import_objects_);
  const PipelineFragmentDefinition fragment_definition_1(identity_to_objects, ObjectReference("bucket", "target.csv"),
                                                         FileFormat::kCsv);
  const PipelineFragmentDefinition fragment_definition_2(identity_to_objects, ObjectReference("bucket2", "target2.csv"),
                                                         FileFormat::kCsv);

  pipeline_->AddFragmentDefinition(fragment_definition_1);
  EXPECT_EQ(pipeline_->FragmentDefinitions().size(), 1);
  pipeline_->AddFragmentDefinition(fragment_definition_2);
  EXPECT_EQ(pipeline_->FragmentDefinitions().size(), 2);

  EXPECT_EQ(pipeline_->FragmentDefinitions().at(0), fragment_definition_1);
  EXPECT_EQ(pipeline_->FragmentDefinitions().at(1), fragment_definition_2);
}

TEST_F(PqpPipelineTest, PredecessorSuccessorManagement) {
  auto p_1 = std::make_shared<PqpPipeline>("p_1", pipeline_plan_);
  EXPECT_EQ(p_1->Predecessors().size(), 0);
  EXPECT_EQ(p_1->Successors().size(), 0);

  auto p_2 = std::make_shared<PqpPipeline>("p_2", pipeline_plan_);
  p_1->SetAsPredecessorOf(p_2);
  EXPECT_EQ(p_1->Predecessors().size(), 0);
  EXPECT_EQ(p_1->Successors().size(), 1);
  EXPECT_EQ(p_1->Successors().front(), p_2);
  EXPECT_EQ(p_2->Predecessors().size(), 1);
  EXPECT_EQ(p_2->Predecessors().at(0).lock(), p_1);

  auto p_3 = std::make_shared<PqpPipeline>("p_3", pipeline_plan_);
  p_1->SetAsPredecessorOf(p_3);
  p_2->SetAsPredecessorOf(p_3);
  EXPECT_TRUE(p_1->Predecessors().empty());
  EXPECT_EQ(p_1->Successors().size(), 2);
  EXPECT_EQ(p_1->Successors().at(0), p_2);
  EXPECT_EQ(p_1->Successors().at(1), p_3);
  EXPECT_EQ(p_2->Predecessors().size(), 1);
  EXPECT_EQ(p_2->Predecessors().at(0).lock(), p_1);
  EXPECT_EQ(p_2->Successors().size(), 1);
  EXPECT_EQ(p_2->Successors().at(0), p_3);
  EXPECT_EQ(p_3->Predecessors().size(), 2);
  EXPECT_EQ(p_3->Predecessors().at(0).lock(), p_1);
  EXPECT_EQ(p_3->Predecessors().at(1).lock(), p_2);
  EXPECT_TRUE(p_3->Successors().empty());
}

TEST_F(PqpPipelineTest, ResultCacheHashWithoutPredecessors) {
  const auto p_1 = std::make_shared<PqpPipeline>("p_1", pipeline_plan_);
  const auto p_2 = std::make_shared<PqpPipeline>("p_2", pipeline_plan_);
  const auto p_3 = std::make_shared<PqpPipeline>("p_3", ExportOperatorProxy::Dummy(pipeline_plan_));
  const size_t p_1_hash = p_1->ResultCacheHash();
  const size_t p_2_hash = p_2->ResultCacheHash();
  const size_t p_3_hash = p_3->ResultCacheHash();
  EXPECT_NE(p_1_hash, 0);
  EXPECT_NE(p_3_hash, 0);
  EXPECT_EQ(p_1_hash, p_2_hash);
  EXPECT_NE(p_1_hash, p_3_hash);
}

TEST_F(PqpPipelineTest, ResultCacheHashWithPredecessors) {
  const auto p_1 = std::make_shared<PqpPipeline>("p_1", pipeline_plan_);
  const auto p_2 = std::make_shared<PqpPipeline>("p_2", pipeline_plan_);
  const size_t p_2_hash_without_predecessor = p_2->ResultCacheHash();
  p_1->SetAsPredecessorOf(p_2);
  const size_t p_1_hash = p_1->ResultCacheHash();
  const size_t p_2_hash = p_2->ResultCacheHash();
  EXPECT_NE(p_2_hash, p_2_hash_without_predecessor);
  EXPECT_NE(p_1_hash, 0);
  EXPECT_NE(p_1_hash, p_2_hash);

  // Set p_2 synthetic.
  p_2->SetSynthetic(true);
  EXPECT_EQ(p_2->ResultCacheHash(), p_2_hash);
  const size_t p_2_hash_synthetic = p_2->ResultCacheHash(true);
  EXPECT_EQ(p_1_hash, p_2_hash_synthetic);
  EXPECT_NE(p_2_hash, p_2_hash_synthetic);

  auto p_3 = std::make_shared<PqpPipeline>("p_3", ExportOperatorProxy::Dummy(pipeline_plan_));
  p_2->SetAsPredecessorOf(p_3);
  const size_t p_3_hash = p_3->ResultCacheHash();
  const size_t p_3_hash_synthetic = p_3->ResultCacheHash(true);
  EXPECT_NE(p_3_hash, p_3_hash_synthetic);
  EXPECT_NE(p_1_hash, p_3_hash);
  EXPECT_NE(p_1_hash, p_3_hash_synthetic);
  EXPECT_NE(p_2_hash, p_3_hash);
  EXPECT_NE(p_2_hash, p_3_hash_synthetic);

  p_3->SetSynthetic(true);
  EXPECT_EQ(p_1_hash, p_3->ResultCacheHash(true));

  auto p_4 = std::make_shared<PqpPipeline>("p_4", ExportOperatorProxy::Dummy(pipeline_plan_));
  p_3->SetAsPredecessorOf(p_4);
  EXPECT_EQ(p_3_hash_synthetic, p_4->ResultCacheHash(true));
}

TEST_F(PqpPipelineTest, ResultCacheHashIgnoresPredecessorOrder) {
  auto p_1 = std::make_shared<PqpPipeline>("p_1", ExportOperatorProxy::Dummy(pipeline_plan_));
  auto p_2 = std::make_shared<PqpPipeline>("p_2", pipeline_plan_);
  auto p_3 = std::make_shared<PqpPipeline>("p_3", pipeline_plan_);
  const auto p_3_hash_without_predecessors = p_3->ResultCacheHash();
  p_1->SetAsPredecessorOf(p_3);
  const auto p_3_hash_with_one_predecessors = p_3->ResultCacheHash();
  p_2->SetAsPredecessorOf(p_3);
  const auto p_3_hash_with_two_predecessors = p_3->ResultCacheHash();
  EXPECT_NE(p_3_hash_with_one_predecessors, p_3_hash_without_predecessors);
  EXPECT_NE(p_3_hash_with_one_predecessors, p_3_hash_with_two_predecessors);

  const size_t hash_a = p_3->ResultCacheHash();

  // In PqpPipeline, the pointers to predecessor pipelines are stored in a vector.
  // The ResultCacheHash must be independent of the vector's elements order.
  p_1 = std::make_shared<PqpPipeline>("p_1", ExportOperatorProxy::Dummy(pipeline_plan_));
  p_2 = std::make_shared<PqpPipeline>("p_2", pipeline_plan_);
  p_3 = std::make_shared<PqpPipeline>("p_3", pipeline_plan_);
  p_2->SetAsPredecessorOf(p_3);
  p_1->SetAsPredecessorOf(p_3);
  const size_t hash_b = p_3->ResultCacheHash();

  EXPECT_EQ(hash_a, hash_b);
}

}  // namespace skyrise
