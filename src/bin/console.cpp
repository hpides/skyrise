#include "coordinator.hpp"

int main() {
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  {
    const auto client = std::make_shared<skyrise::Client>();
    skyrise::Coordinator coordinator(client);
    std::cout << coordinator.GetUser().GetUserName() << std::endl;
  }
  Aws::ShutdownAPI(options);

  return 0;
}
