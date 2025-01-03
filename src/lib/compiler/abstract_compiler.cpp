#include "abstract_compiler.hpp"

#include <magic_enum/magic_enum.hpp>

#include "compiler/pqp_generator/etl_pqp_generator.hpp"
#include "compiler/pqp_generator/tpch_pqp_generator.hpp"

namespace skyrise {

AbstractCompiler::AbstractCompiler(const QueryId& query_id, const ScaleFactor& scale_factor,
                                   const ObjectReference& shuffle_storage_prefix)
    : query_id_(query_id), scale_factor_(scale_factor), shuffle_storage_prefix_(shuffle_storage_prefix) {}

AbstractCompilerConfig::AbstractCompilerConfig(const CompilerName& compiler_name, const QueryId& query_id,
                                               const ScaleFactor& scale_factor,
                                               const ObjectReference& shuffle_storage_prefix)
    : compiler_name_(compiler_name),
      query_id_(query_id),
      scale_factor_(scale_factor),
      shuffle_storage_prefix_(shuffle_storage_prefix) {}

std::shared_ptr<AbstractCompilerConfig> AbstractCompilerConfig::FromJson(const Aws::Utils::Json::JsonView& json) {
  const CompilerName compiler_name =
      magic_enum::enum_cast<CompilerName>(json.GetString(kCoordinatorRequestCompilerNameAttribute)).value();
  const QueryId query_id =
      magic_enum::enum_cast<QueryId>(json.GetString(kCoordinatorRequestQueryPlanAttribute)).value();
  const ScaleFactor scale_factor =
      magic_enum::enum_cast<ScaleFactor>(json.GetString(kCoordinatorRequestScaleFactorAttribute)).value();
  const ObjectReference shuffle_storage_prefix =
      ObjectReference::FromJson(json.GetObject(kCoordinatorRequestStoragePrefixAttribute));
  switch (compiler_name) {
    case CompilerName::kEtl:
      return std::make_shared<EtlPqpGeneratorConfig>(compiler_name, query_id, scale_factor, shuffle_storage_prefix);
    case CompilerName::kProcessMining:
      Fail("PQP generator for process mining queries is not implemented.");
    case CompilerName::kSql:
      Fail("Compiler for SQL queries is not implemented.");
    case CompilerName::kTpch:
      return std::make_shared<TpchPqpGeneratorConfig>(compiler_name, query_id, scale_factor, shuffle_storage_prefix);
    case CompilerName::kTpcxbb:
      Fail("PQP generator for TPCx-BB queries is not implemented.");
    default:
      Fail("Unknown compiler configuration.");
  }
}

Aws::Utils::Json::JsonValue AbstractCompilerConfig::ToJson() const {
  Aws::Utils::Json::JsonValue serialized_compiler_config;
  serialized_compiler_config
      .WithString(kCoordinatorRequestCompilerNameAttribute, std::string(magic_enum::enum_name(compiler_name_)))
      .WithString(kCoordinatorRequestQueryPlanAttribute, std::string(magic_enum::enum_name(query_id_)))
      .WithString(kCoordinatorRequestScaleFactorAttribute, std::string(magic_enum::enum_name(scale_factor_)))
      .WithObject(kCoordinatorRequestStoragePrefixAttribute, shuffle_storage_prefix_.ToJson());

  return serialized_compiler_config;
}

}  // namespace skyrise
