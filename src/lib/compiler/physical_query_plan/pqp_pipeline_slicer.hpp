#pragma once

#include <memory>
#include <string>

#include "compilation_context.hpp"
#include "operator_proxy/abstract_operator_proxy.hpp"
#include "pqp_pipeline.hpp"
#include "types.hpp"

namespace skyrise {

class PqpPipelineSlicer : public Noncopyable {
 public:
  PqpPipelineSlicer(std::shared_ptr<AbstractOperatorProxy> pqp, std::shared_ptr<QueryContext> query_context);

  const std::vector<std::shared_ptr<PqpPipeline>>& GetPipelines();

 protected:
  std::shared_ptr<PqpPipeline> CutNextPipelineFragment(  // TODO(Julian): change to TryCutOfNextPipeline
      const std::shared_ptr<ImportOperatorProxy>& primary_import_proxy,
      std::vector<std::shared_ptr<ImportOperatorProxy>>& consumed_imports);

  /**
   * Reads the comment attribute of @param import_operator_proxy and compares it to all available pipeline identities.
   * @returns a PqpPipeline with a matching pipeline identity, if available.
   */
  std::shared_ptr<PqpPipeline> FindPipelinePredecessor(std::shared_ptr<ImportOperatorProxy> import_proxy) const;

 private:
  std::shared_ptr<AbstractOperatorProxy> pqp_;
  std::shared_ptr<QueryContext> query_context_;
  std::vector<std::shared_ptr<PqpPipeline>> pipelines_;
};

}  // namespace skyrise
