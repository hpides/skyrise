#pragma once

#include "compiler/physical_query_plan/pqp_pipeline.hpp"
#include "types.hpp"

namespace skyrise {

class AbstractCompiler {
 public:
  AbstractCompiler(const QueryId& query_id, const ScaleFactor& scale_factor, const ObjectReference& shuffle_storage);

  virtual ~AbstractCompiler() = default;

  virtual std::vector<std::shared_ptr<PqpPipeline>> GeneratePqp() const = 0;

 protected:
  const QueryId query_id_;
  const ScaleFactor scale_factor_;
  const ObjectReference shuffle_storage_;
};

class AbstractCompilerConfig {
 public:
  AbstractCompilerConfig(const CompilerName& compiler_name, const QueryId& query_id, const ScaleFactor& scale_factor,
                         const ObjectReference& shuffle_storage);

  virtual ~AbstractCompilerConfig() = default;

  static std::shared_ptr<AbstractCompilerConfig> FromJson(const Aws::Utils::Json::JsonView& json);
  Aws::Utils::Json::JsonValue ToJson() const;

  virtual std::shared_ptr<AbstractCompiler> GenerateCompiler() const = 0;

 protected:
  const CompilerName compiler_name_;
  const QueryId query_id_;
  const ScaleFactor scale_factor_;
  const ObjectReference shuffle_storage_;
};

}  // namespace skyrise
