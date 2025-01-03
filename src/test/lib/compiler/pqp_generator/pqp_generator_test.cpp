#include <aws/core/utils/json/JsonSerializer.h>
#include <gtest/gtest.h>

#include "compiler/pqp_generator/etl_pqp_generator.hpp"
#include "compiler/pqp_generator/tpch_pqp_generator.hpp"

namespace skyrise {

class PqpGeneratorTest : public ::testing::Test {};

TEST_F(PqpGeneratorTest, SerializeDeserializeCopyPqp) {
  const auto compiler_config =
      std::make_shared<EtlPqpGeneratorConfig>(CompilerName::kEtl, QueryId::kEtlCopyTpchOrders, ScaleFactor::kSf1,
                                              ObjectReference(kSkyriseTestBucket, "storage_prefix"));
  const Aws::Utils::Json::JsonValue serialized_compiler_config = compiler_config->ToJson();
  const auto deserialized_compiler_config = AbstractCompilerConfig::FromJson(serialized_compiler_config);
  const auto derived_compiler_config = std::dynamic_pointer_cast<EtlPqpGeneratorConfig>(deserialized_compiler_config);
  ASSERT_EQ(*derived_compiler_config, *compiler_config);
}

TEST_F(PqpGeneratorTest, SerializeDeserializeTpchPqp) {
  const auto compiler_config = std::make_shared<TpchPqpGeneratorConfig>(
      CompilerName::kTpch, QueryId::kTpchQ1, ScaleFactor::kSf1, ObjectReference(kSkyriseTestBucket, "storage_prefix"));
  const Aws::Utils::Json::JsonValue serialized_compiler_config = compiler_config->ToJson();
  const auto deserialized_compiler_config = AbstractCompilerConfig::FromJson(serialized_compiler_config);
  const auto derived_compiler_config = std::dynamic_pointer_cast<TpchPqpGeneratorConfig>(deserialized_compiler_config);
  ASSERT_EQ(*derived_compiler_config, *compiler_config);
}

}  // namespace skyrise
