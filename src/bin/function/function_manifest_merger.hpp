#pragma once

#include "function/function.hpp"
#include "statistics/manifest_merger.hpp"
#include "storage/backend/storage_s3.hpp"

namespace skyrise {

class FunctionManifestMerger : public Function {
 protected:
  static bool CheckPayload(const Aws::Utils::Json::JsonView& request);
  aws::lambda_runtime::invocation_response OnHandleRequest(const Aws::Utils::Json::JsonView& request) const override;
};

}  // namespace skyrise
