#pragma once

#include "compiler/abstract_compiler.hpp"

namespace skyrise {

class EtlPqpGenerator : public AbstractCompiler {
 public:
  EtlPqpGenerator(const QueryId& query_id, const ScaleFactor& scale_factor,
                  const ObjectReference& shuffle_storage_prefix);

  std::vector<std::shared_ptr<PqpPipeline>> GeneratePqp() const final;
};

class EtlPqpGeneratorConfig : public AbstractCompilerConfig {
 public:
  EtlPqpGeneratorConfig(const CompilerName& compiler_name, const QueryId& query_id, const ScaleFactor& scale_factor,
                        const ObjectReference& shuffle_storage_prefix);

  std::shared_ptr<AbstractCompiler> GenerateCompiler() const final;
  bool operator==(const EtlPqpGeneratorConfig& other) const;
};

}  // namespace skyrise
