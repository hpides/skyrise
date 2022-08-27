#include "compiler/compilation_context.hpp"

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "configuration.hpp"
#include "metadata/mock_catalog.hpp"

namespace skyrise {

class CompilationContextTest : public ::testing::Test {
 public:
  void SetUp() override {
    mock_catalog_ = std::make_shared<MockCatalog>();

    // Create timestamp
    std::stringstream stream("Jul 22 2022 12:50:30");
    std::tm tm = {};
    stream >> std::get_time(&tm, "%b %d %Y %H:%M:%S");
    auto arrival_time = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    arrival_time += std::chrono::milliseconds(500);

    SqlRequest request(kMockQuery, kMockUserName, arrival_time);
    compilation_context_ = std::make_shared<CompilationContext>(request, mock_catalog_);
  }

 protected:
  std::shared_ptr<MockCatalog> mock_catalog_;
  std::shared_ptr<CompilationContext> compilation_context_;
  const std::string kMockUserName = "mock_user";
  const std::string kMockQuery = "SELECT * FROM tableXY";
  const std::string kMockExportBucketName = "mock_target_bucket";
};

TEST_F(CompilationContextTest, GettersAndSetters) {
  EXPECT_EQ(compilation_context_->QueryString(), kMockQuery);
  EXPECT_EQ(compilation_context_->Catalog(), mock_catalog_);

  EXPECT_EQ(compilation_context_->ExportBucketName(), kExportBucketName);
  compilation_context_->SetExportBucketName(kMockExportBucketName);
  EXPECT_EQ(compilation_context_->ExportBucketName(), kMockExportBucketName);

  EXPECT_EQ(compilation_context_->GetExportFormat(), ExportFormat::kCsv);
  EXPECT_EQ(compilation_context_->ExportFileExtension(), ".csv");
  compilation_context_->SetExportFormat(ExportFormat::kOrc);
  EXPECT_EQ(compilation_context_->GetExportFormat(), ExportFormat::kOrc);
  EXPECT_EQ(compilation_context_->ExportFileExtension(), ".orc");

  EXPECT_EQ(compilation_context_->MaxWorkerCount(), kMaxWorkerCountPerPipeline);
  compilation_context_->SetMaxWorkerCount(100);
  EXPECT_EQ(compilation_context_->MaxWorkerCount(), 100);
}

TEST_F(CompilationContextTest, QueryIdentity) {
  const auto expected_identity = std::to_string(boost::hash_value(kMockQuery));
  EXPECT_EQ(compilation_context_->QueryIdentity(), expected_identity);
}

TEST_F(CompilationContextTest, PipelineIdentity) {
  EXPECT_THROW(compilation_context_->PipelineIdentity(0), std::logic_error);
  EXPECT_EQ(compilation_context_->PipelineIdentity(1), compilation_context_->QueryIdentity() + "_001");
  EXPECT_EQ(compilation_context_->PipelineIdentity(10), compilation_context_->QueryIdentity() + "_010");
  EXPECT_EQ(compilation_context_->PipelineIdentity(100), compilation_context_->QueryIdentity() + "_100");
}

TEST_F(CompilationContextTest, NextPipelineId) {
  EXPECT_EQ(compilation_context_->NextPipelineId(), 1);
  EXPECT_EQ(compilation_context_->NextPipelineId(), 2);
  EXPECT_EQ(compilation_context_->NextPipelineId(), 3);
}

TEST_F(CompilationContextTest, PipelineExportPrefix) {
  std::string prefix = "result/mock_user/2022-07-22_12:50:30'500_" + compilation_context_->QueryIdentity();
  EXPECT_THROW(compilation_context_->PipelineExportPrefix(0), std::logic_error);
  EXPECT_EQ(compilation_context_->PipelineExportPrefix(1), prefix + "/pipeline_001/");
  EXPECT_EQ(compilation_context_->PipelineExportPrefix(10), prefix + "/pipeline_010/");
  EXPECT_EQ(compilation_context_->PipelineExportPrefix(100), prefix + "/pipeline_100/");
  EXPECT_EQ(compilation_context_->PipelineExportPrefix(1000), prefix + "/pipeline_1000/");
}

}  // namespace skyrise
