#include <iostream>

#include "coordinator.hpp"
#include "metering/request_tracker/request_tracker.hpp"

int main() {
  Aws::SDKOptions options;
  auto tracker = std::make_shared<skyrise::RequestTracker>();
  tracker->Install(&options);

  Aws::InitAPI(options);
  {
    const auto client = std::make_shared<skyrise::Client>();
    skyrise::Coordinator coordinator(client);
    std::cout << coordinator.GetUser().GetUserName() << std::endl;
  }
  Aws::ShutdownAPI(options);

  tracker->WriteSummaryToStream(&std::clog);

  return 0;
}
