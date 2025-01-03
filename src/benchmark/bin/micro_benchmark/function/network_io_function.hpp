#pragma once

#include "function/function.hpp"

namespace skyrise {

class NetworkIoFunction : public Function {
 protected:
  aws::lambda_runtime::invocation_response OnHandleRequest(const Aws::Utils::Json::JsonView& request) const override;

 private:
  static void CreateAndRunIperfTest(const size_t target_throughput_mbps, const int concurrent_instance_count,
                                    const int sqs_queue_size, const size_t cpu_count, const std::string& sqs_queue_url,
                                    const double report_interval_ms, const int64_t duration_s, const int64_t port,
                                    const Aws::String& public_ip, const bool enable_download, const int message_size_kb,
                                    Aws::Utils::Array<Aws::Utils::Json::JsonValue>& intervals,
                                    std::pair<bool, std::string>& result);
};

}  // namespace skyrise
