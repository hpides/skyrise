#include "compiler/physical_query_plan/pipeline_fragment_template.hpp"

#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/join_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/union_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_utils.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class PipelineFragmentTemplateTest : public ::testing::Test {
 public:
  void SetUp() override {
    const std::vector<ObjectReference> import_objects = {ObjectReference("import", "obj.orc")};
    const std::vector<ColumnId> import_column_ids = {ColumnId{0}};

    import_proxy_a_ = ImportOperatorProxy::Make(import_objects, import_column_ids);
    import_proxy_a_->SetIdentity("import_a");

    import_proxy_b_ = ImportOperatorProxy::Make(import_objects, import_column_ids);
    import_proxy_b_->SetIdentity("import_b");

    a_a_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a_a");
    a_b_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "a_b");
    b_x_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "b_x");
    b_y_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b_y");
  }

 protected:
  std::shared_ptr<ImportOperatorProxy> import_proxy_a_, import_proxy_b_;
  std::shared_ptr<PqpColumnExpression> a_a_, a_b_;
  std::shared_ptr<PqpColumnExpression> b_x_, b_y_;
};

TEST_F(PipelineFragmentTemplateTest, ValidatePlanExport) {
  // clang-format off
  const auto pqp =
  FilterOperatorProxy::Make(LessThan_(a_b_, 123),
    import_proxy_a_);
  // clang-format on

  // Exception is expected due to the missing export proxy.
  EXPECT_THROW(PipelineFragmentTemplate{pqp}, std::logic_error);
  EXPECT_NO_THROW(PipelineFragmentTemplate{ExportOperatorProxy::Dummy(pqp)});
}

TEST_F(PipelineFragmentTemplateTest, ValidatePlanImport) {
  // clang-format off
  auto pqp =
  UnionOperatorProxy::Make(SetOperationMode::kAll,
    FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
      import_proxy_a_),
    FilterOperatorProxy::Make(LessThan_(a_b_, 123)));
  // clang-format on

  // Exception is expected, because not all leaf nodes are import proxies.
  EXPECT_THROW(PipelineFragmentTemplate(ExportOperatorProxy::Dummy(pqp)), std::logic_error);
}

TEST_F(PipelineFragmentTemplateTest, TemplatedImportAndExportProxies) {
  // clang-format off
  auto pqp =
  ExportOperatorProxy::Make(ObjectReference("random_bucket", "random_object"), FileFormat::kCsv,
                            UnionOperatorProxy::Make(SetOperationMode::kAll,
    FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
      import_proxy_a_),
    FilterOperatorProxy::Make(LessThan_(a_b_, 123),
      import_proxy_b_)));
  // clang-format on

  const PipelineFragmentTemplate fragment_template(pqp);
  const auto templated_plan = fragment_template.TemplatedPlan();

  // Check if export proxy has cleared attributes.
  const auto template_export = std::static_pointer_cast<const ExportOperatorProxy>(templated_plan);
  EXPECT_EQ(template_export->TargetObject().bucket_name, "Placeholder");
  EXPECT_EQ(template_export->TargetObject().identifier, "Placeholder");
  EXPECT_EQ(template_export->GetExportFormat(), FileFormat::kOrc);

  // Check if import proxies have cleared attributes.
  const auto template_import_a =
      std::dynamic_pointer_cast<const ImportOperatorProxy>(templated_plan->LeftInput()->LeftInput()->LeftInput());
  const auto template_import_b =
      std::dynamic_pointer_cast<const ImportOperatorProxy>(templated_plan->LeftInput()->RightInput()->LeftInput());
  EXPECT_TRUE(template_import_a->ObjectReferences().empty());
  EXPECT_TRUE(template_import_b->ObjectReferences().empty());
}

TEST_F(PipelineFragmentTemplateTest, GenerateFragmentPlan) {
  // clang-format off
  auto pqp =
  ExportOperatorProxy::Dummy(
    FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
      import_proxy_a_));
  // clang-format on

  const PipelineFragmentTemplate fragment_template(pqp);

  // Define Fragment Plan
  std::unordered_map<std::string, std::vector<ObjectReference>> identity_to_objects;
  const std::vector<ObjectReference> import_objects = {ObjectReference("bucket", "import_key1"),
                                                       ObjectReference("bucket", "import_key2")};
  identity_to_objects.emplace(import_proxy_a_->Identity(), import_objects);
  const ObjectReference target_object("export_bucket", "export.orc");
  const auto target_format = FileFormat::kOrc;
  const PipelineFragmentDefinition fragment_definition(identity_to_objects, target_object, target_format);

  // Generate Fragment Plan
  const auto fragment_instance = fragment_template.GenerateFragmentPlan(fragment_definition);

  // Check if Export fields are set
  const auto export_proxy = std::dynamic_pointer_cast<ExportOperatorProxy>(fragment_instance);
  EXPECT_EQ(export_proxy->TargetObject(), target_object);
  EXPECT_EQ(export_proxy->GetExportFormat(), target_format);

  // Check if Import fields are set
  const auto import_proxy = std::dynamic_pointer_cast<ImportOperatorProxy>(fragment_instance->LeftInput()->LeftInput());
  ASSERT_TRUE(import_proxy);
  EXPECT_EQ(import_proxy->ObjectReferences(), import_objects);
  EXPECT_EQ(import_proxy->OutputObjectsCount(), 1);
}

TEST_F(PipelineFragmentTemplateTest, GenerateFragmentPlanMultipleImports) {
  std::vector<std::shared_ptr<JoinOperatorPredicate>> secondary_predicates;
  // clang-format off
  auto pqp =
  ExportOperatorProxy::Dummy(
    JoinOperatorProxy::Make(JoinMode::kInner, JoinOperatorPredicate_(Equals_(a_a_, b_x_)), secondary_predicates,
      UnionOperatorProxy::Make(SetOperationMode::kAll,
        FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
          import_proxy_a_),
        FilterOperatorProxy::Make(LessThan_(a_b_, 123),
          import_proxy_b_)),
      import_proxy_b_));
  // clang-format on

  const PipelineFragmentTemplate fragment_template(pqp);

  // Define Fragment Plan
  std::unordered_map<std::string, std::vector<ObjectReference>> identity_to_objects;
  const std::vector<ObjectReference> import_objects_a = {ObjectReference("bucket", "import_key_a")};
  const std::vector<ObjectReference> import_objects_b = {ObjectReference("bucket", "import_key_b")};
  identity_to_objects.emplace(import_proxy_a_->Identity(), import_objects_a);
  identity_to_objects.emplace(import_proxy_b_->Identity(), import_objects_b);
  const ObjectReference target_object("export_bucket", "export.orc");
  const auto target_format = FileFormat::kOrc;
  const PipelineFragmentDefinition fragment_definition(identity_to_objects, target_object, target_format);

  // Generate Fragment Plan
  const auto fragment_instance = fragment_template.GenerateFragmentPlan(fragment_definition);

  // Check if Export fields are set
  const auto export_proxy = std::dynamic_pointer_cast<ExportOperatorProxy>(fragment_instance);
  ASSERT_TRUE(export_proxy);
  EXPECT_EQ(export_proxy->TargetObject(), target_object);
  EXPECT_EQ(export_proxy->GetExportFormat(), target_format);

  // Check if Import fields are set
  EXPECT_EQ(PqpFindLeaves(fragment_instance).size(), 2);
  const auto import_proxy_a = std::dynamic_pointer_cast<ImportOperatorProxy>(
      fragment_instance->LeftInput()->LeftInput()->LeftInput()->LeftInput());
  const auto import_proxy_b =
      std::dynamic_pointer_cast<ImportOperatorProxy>(fragment_instance->LeftInput()->RightInput());
  EXPECT_EQ(import_proxy_a->ObjectReferences(), import_objects_a);
  EXPECT_EQ(import_proxy_b->ObjectReferences(), import_objects_b);
}

}  // namespace skyrise
