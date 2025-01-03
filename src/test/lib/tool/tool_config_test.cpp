#include "tool/tool_config.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace skyrise {

class ToolConfigTest : public ::testing::Test {};

TEST_F(ToolConfigTest, ToolOptionToEnumValidInputFormat) {
  const std::string valid_format_enum_exist = "function-upload";
  EXPECT_EQ(ToolType::kFunctionUpload, ToolOptionToEnum(valid_format_enum_exist));
}

TEST_F(ToolConfigTest, ToolOptionToEnumInvalidInputFormat) {
  const std::string invalid_format = "upload";
  EXPECT_THROW(
      try { ToolOptionToEnum(invalid_format); } catch (const std::logic_error& exception) {
        EXPECT_THAT(exception.what(), testing::HasSubstr("Tool must be specified in format 'tool-identifier'."));
        throw;
      },
      std::logic_error);
}

TEST_F(ToolConfigTest, ToolOptionToEnumValidInputFormatButNotFound) {
  const std::string valid_format_enum_not_exist = "not-present";
  EXPECT_THROW(ToolOptionToEnum(valid_format_enum_not_exist), std::bad_optional_access);
}

TEST_F(ToolConfigTest, ConfigureCliOptions) {
  const cxxopts::Options options = ConfigureCliOptions();
  EXPECT_EQ(options.program(), kProgramName);
  EXPECT_EQ(options.groups().size(), 3);
  EXPECT_EQ(options.groups()[1], "tool");
  EXPECT_EQ(options.groups()[2], "function-upload");
}

}  // namespace skyrise
