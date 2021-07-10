#include "coordinator.hpp"

#include <aws/core/utils/logging/LogMacros.h>
#include <aws/iam/model/GetUserRequest.h>

namespace skyrise {

Aws::IAM::Model::User Coordinator::GetUser() {
  // Use STS GetCallerIdentity for fewer permission requirements.
  const auto iam_client = client_->GetIAMClient();
  const auto outcome = iam_client->GetUser(Aws::IAM::Model::GetUserRequest{});

  if (!outcome.IsSuccess()) {
    AWS_LOGSTREAM_ERROR(kTag.c_str(), outcome.GetError().GetMessage());
  }

  return outcome.GetResult().GetUser();
}

}  // namespace skyrise
