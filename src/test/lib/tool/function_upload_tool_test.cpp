#include "tool/function_upload_tool.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "tool/tool_config.hpp"

namespace skyrise {

class FunctionUploadTest : public ::testing::Test {
  void SetUp() override {
    for (int i = 0; i < kArgumentCount; ++i) {
      upload_arguments_[i] = valid_argument_list_[i].data();
    }
  }

 protected:
  static constexpr int kArgumentCount = 9;
  const std::vector<std::string> valid_argument_list_ = {{"./skyrise"},     {"--tool"},           {"function-upload"},
                                                         {"--description"}, {"some_description"}, {"--name"},
                                                         {"someFunction"},  {"--memory_size"},    {"256"}};
  std::array<const char*, kArgumentCount> upload_arguments_ = {};
};

TEST_F(FunctionUploadTest, ConfigureAndUploadFunctionNotExistingFunction) {
  cxxopts::Options options = ConfigureCliOptions();
  const cxxopts::ParseResult parse_result =
      options.parse(kArgumentCount, reinterpret_cast<const char* const*>(upload_arguments_.data()));
  EXPECT_THROW(
      try { FunctionUploadTool(parse_result); } catch (const std::logic_error& exception) {
        EXPECT_THAT(exception.what(), testing::HasSubstr("Function ZIP 'pkg/someFunction.zip' not found."));
        throw;
      },
      std::logic_error);
}

}  // namespace skyrise
