#pragma once

#include <memory>
#include <string>

#include "compiler/compilation_context.hpp"
#include "operator_proxy/abstract_operator_proxy.hpp"
#include "pqp_pipeline.hpp"
#include "types.hpp"

namespace skyrise {

class PqpPipelineSlicer : public Noncopyable {
 public:
  PqpPipelineSlicer(std::shared_ptr<AbstractOperatorProxy> pqp, std::shared_ptr<CompilationContext> query_context);

  const std::vector<std::shared_ptr<PqpPipeline>>& SlicePqpIntoPipelines();

 protected:
  std::shared_ptr<PqpPipeline> TryCutOffNextPipeline(
      const std::shared_ptr<ImportOperatorProxy>& primary_import_proxy,
      std::vector<std::shared_ptr<ImportOperatorProxy>>& consumed_imports);

  /**
   * Reads @param import_operator_proxy's OriginIdentifier attribute and compares it with existing pipeline identities.
   * @returns a shared pointer to an existing PqpPipeline, if referenced by @param import_proxy.
   *          Otherwise, a null pointer is returned.
   */
  std::shared_ptr<PqpPipeline> TryAddPipelineDependency(std::shared_ptr<ImportOperatorProxy> import_proxy) const;

 private:
  std::shared_ptr<AbstractOperatorProxy> pqp_;
  std::shared_ptr<CompilationContext> compilation_context_;
  std::vector<std::shared_ptr<PqpPipeline>> pipelines_;
};

}  // namespace skyrise
