#include "compiler/compilation_context.hpp"

#include <iomanip>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "configuration.hpp"
#include "metadata/schema/mock_catalog.hpp"

namespace {

const std::string kTestQueryString = "SELECT * FROM tableXY";
const std::string kTestUserName = "test_user";
const std::string kTestExportBucketName = "test_target_bucket";

}  // namespace

namespace skyrise {

class CompilationContextTest : public ::testing::Test {
 public:
  void SetUp() override {
    mock_catalog_ = std::make_shared<MockCatalog>();

    // Create timestamp.
    std::stringstream stream("Jul 22 2022 12:50:30");
    std::tm tm = {};
    stream >> std::get_time(&tm, "%b %d %Y %H:%M:%S");
    // Ignore daylight savings.
    tm.tm_isdst = -1;
    auto arrival_time = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    arrival_time += std::chrono::milliseconds(500);

    const SqlRequest request(kTestQueryString, kTestUserName, arrival_time);
    compilation_context_ = std::make_shared<CompilationContext>(request, mock_catalog_);
  }

 protected:
  std::shared_ptr<MockCatalog> mock_catalog_;
  std::shared_ptr<CompilationContext> compilation_context_;
};

TEST_F(CompilationContextTest, GettersAndSetters) {
  EXPECT_EQ(compilation_context_->QueryString(), kTestQueryString);
  EXPECT_EQ(compilation_context_->Catalog(), mock_catalog_);

  EXPECT_EQ(compilation_context_->ExportBucketName(), kExportBucketName);
  compilation_context_->SetExportBucketName(kTestExportBucketName);
  EXPECT_EQ(compilation_context_->ExportBucketName(), kTestExportBucketName);

  EXPECT_EQ(compilation_context_->GetExportFormat(), FileFormat::kCsv);
  EXPECT_EQ(compilation_context_->ExportFileExtension(), ".csv");
  compilation_context_->SetExportFormat(FileFormat::kOrc);
  EXPECT_EQ(compilation_context_->GetExportFormat(), FileFormat::kOrc);
  EXPECT_EQ(compilation_context_->ExportFileExtension(), ".orc");

  EXPECT_EQ(compilation_context_->MaxWorkerCount(), kWorkerMaximumCountPerPipeline);
  compilation_context_->SetMaxWorkerCount(100);
  EXPECT_EQ(compilation_context_->MaxWorkerCount(), 100);
}

TEST_F(CompilationContextTest, QueryIdentity) {
  const auto expected_identity = std::to_string(boost::hash_value(kTestQueryString));
  EXPECT_EQ(compilation_context_->QueryIdentity(), expected_identity);
}

TEST_F(CompilationContextTest, NextPipelineId) {
  EXPECT_EQ(compilation_context_->NextPipelineId(), 1);
  EXPECT_EQ(compilation_context_->NextPipelineId(), 2);
  EXPECT_EQ(compilation_context_->NextPipelineId(), 3);
}

TEST_F(CompilationContextTest, PipelineExportPrefix) {
  const std::string prefix = "result/test_user/2022-07-22_12:50:30'500_" + compilation_context_->QueryIdentity();
  for (size_t i = 1; i < 10; ++i) {
    EXPECT_EQ(compilation_context_->PipelineExportPrefix(i), prefix + "/pipeline_00" + std::to_string(i) + "/");
  }
  for (size_t i = 10; i < 100; ++i) {
    EXPECT_EQ(compilation_context_->PipelineExportPrefix(i), prefix + "/pipeline_0" + std::to_string(i) + "/");
  }
  EXPECT_EQ(compilation_context_->PipelineExportPrefix(100), prefix + "/pipeline_100/");
  EXPECT_EQ(compilation_context_->PipelineExportPrefix(1000), prefix + "/pipeline_1000/");
}

}  // namespace skyrise
