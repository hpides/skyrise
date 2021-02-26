#include <aws/core/Aws.h>

#include "client/client.hpp"

namespace skyrise {

class Coordinator {
 public:
  explicit Coordinator(const std::shared_ptr<Client>& client) : client_(client) {}
  Aws::IAM::Model::User GetUser();

  std::shared_ptr<Client> client_;

 private:
  const Aws::String kTag = "SKYRISE/COORDINATOR";
};

}  // namespace skyrise
