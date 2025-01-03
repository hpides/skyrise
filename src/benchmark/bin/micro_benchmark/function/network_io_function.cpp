#include "network_io_function.hpp"

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/lambda-runtime/runtime.h>

#include "client/base_client.hpp"
#include "constants.hpp"
#include "utils/assert.hpp"
#include "utils/iperf_wrapper.hpp"
#include "utils/profiling/function_host_information.hpp"
#include "utils/synchronize_instances.hpp"
#include "utils/time.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

namespace {

constexpr u_int16_t kMillisecondsToSecondsDivider = 1000;

}  // namespace

void NetworkIoFunction::CreateAndRunIperfTest(
    const size_t target_throughput_mbps, const int concurrent_instance_count, const int sqs_queue_size,
    const size_t cpu_count, const std::string& sqs_queue_url, const double report_interval_ms, const int64_t duration_s,
    const int64_t port, const Aws::String& public_ip, const bool enable_download, const int message_size,
    Aws::Utils::Array<Aws::Utils::Json::JsonValue>& interval, std::pair<bool, std::string>& result) {
  bool limit_throughput = false;
  size_t target_throughput_bps = 0;

  if (target_throughput_mbps != std::numeric_limits<size_t>::max()) {
    limit_throughput = true;
    target_throughput_bps = MBToBit(target_throughput_mbps);
  }

  IperfWrapper iperf_wrapper;
  iperf_wrapper.Initialize();
  iperf_wrapper.Defaults();
  // The message size.
  iperf_wrapper.SetMessageSize(KBToByte(message_size));
  // Set 'c' for client role.
  iperf_wrapper.SetTestRole('c');
  // Reverse to test for download.
  iperf_wrapper.SetTestReverse(enable_download);
  // Set duration.
  iperf_wrapper.SetTestDuration(duration_s);
  // Set host IP and port.
  iperf_wrapper.SetTestServerHostname(public_ip.c_str());
  iperf_wrapper.SetTestServerPort(port);
  // Set output format to JSON.
  iperf_wrapper.SetTestJsonOutput(true);
  // Set unit format to MBytes/sec.
  iperf_wrapper.SetTestUnitFormat('M');
  // Set the number of connections.
  iperf_wrapper.SetTestNumStreams(cpu_count);

  if (limit_throughput) {
    // Set the target bandwidth.
    iperf_wrapper.SetTestRate(target_throughput_bps);
  }

  // Set the interval, in seconds, to report the throughput.
  iperf_wrapper.SetTestReporterInterval(report_interval_ms / kMillisecondsToSecondsDivider);
  iperf_wrapper.SetTestStatsInterval(report_interval_ms / kMillisecondsToSecondsDivider);

  if (!sqs_queue_url.empty() && concurrent_instance_count > 1) {
    // Coordinate the start of the benchmark across multiple invocations.
    const auto client = std::make_shared<BaseClient>();
    AwaitInstances(sqs_queue_url, client->GetSqsClient(), sqs_queue_size, "id");
  }

  const std::string measurement_start_timestamp = GetFormattedTimestamp("%Y%m%dT%H%M%S");

  if (iperf_wrapper.RunClient() < 0) {
    AWS_LOGSTREAM_ERROR(kBenchmarkTag.c_str(), IperfWrapper::StringError());
    result = {false, measurement_start_timestamp};
    return;
  }

  // Transform iPerf output into JSON response.
  if (iperf_wrapper.GetTestJsonOutputString()) {
    const auto json_result = Aws::Utils::Json::JsonValue(iperf_wrapper.GetTestJsonOutputString());
    Assert(json_result.WasParseSuccessful(), json_result.GetErrorMessage());
    const auto intervals = json_result.View().GetArray("intervals");
    for (size_t i = 0; i < intervals.GetLength(); ++i) {
      interval[i] = Aws::Utils::Json::JsonValue().AsInt64(
          BitToMB(intervals.GetItem(i).GetObject("sum").GetInt64("bits_per_second")));
    }
  }

  iperf_wrapper.EndClient();
  result = {true, measurement_start_timestamp};
}

aws::lambda_runtime::invocation_response NetworkIoFunction::OnHandleRequest(
    const Aws::Utils::Json::JsonView& request) const {
  const std::string invocation_start_timestamp = GetFormattedTimestamp("%Y%m%dT%H%M%S");
  const FunctionHostInformationCollectorConfiguration config;
  FunctionHostInformationCollector collector{config};
  const FunctionHostInformationIdentification information_identification = collector.CollectInformationIdentification();
  const size_t cpu_count = collector.CollectInformationResources().cpu_count;
  const Aws::String ip{request.GetString("ip")};
  const int64_t port = request.GetInt64("port");
  std::cout << "ip=" << ip << " port=" << port << std::endl;
  const int64_t duration_s = request.GetInt64("duration_s");
  const double report_interval_ms = request.GetDouble("report_interval_ms");
  size_t target_throughput_mbps = request.GetInt64("target_throughput_mbps");
  const bool enable_download = request.GetBool("enable_download");
  const bool enable_upload = request.GetBool("enable_upload");
  const int concurrent_instance_count = request.GetInteger("concurrent_instance_count");
  const int message_size_kb = request.GetInteger("message_size_kb");
  const std::string sqs_queue_url = request.GetString("sqs_queue_url");

  const int expected_intervals = duration_s / (report_interval_ms / kMillisecondsToSecondsDivider);
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> download_intervals(expected_intervals);
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> upload_intervals(expected_intervals);
  std::pair<bool, std::string> iperf_test_download_result;
  std::pair<bool, std::string> iperf_test_upload_result;

  if (enable_download) {
    CreateAndRunIperfTest(target_throughput_mbps, concurrent_instance_count, concurrent_instance_count, cpu_count,
                          sqs_queue_url, report_interval_ms, duration_s, port, ip, true, message_size_kb,
                          download_intervals, iperf_test_download_result);
    if (!iperf_test_download_result.first) {
      return aws::lambda_runtime::invocation_response::failure(IperfWrapper::StringError(), "iPerf error");
    }
    AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Done iPerf download test.");
  }

  if (enable_upload) {
    CreateAndRunIperfTest(
        target_throughput_mbps, concurrent_instance_count,
        (enable_upload && enable_download) ? concurrent_instance_count * 2 : concurrent_instance_count, cpu_count,
        sqs_queue_url, report_interval_ms, duration_s, port, ip, false, message_size_kb, upload_intervals,
        iperf_test_upload_result);
    if (!iperf_test_upload_result.first) {
      return aws::lambda_runtime::invocation_response::failure(IperfWrapper::StringError(), "iPerf error");
    }
    AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Done iPerf upload test.");
  }

  const auto response =
      Aws::Utils::Json::JsonValue()
          .WithArray("intervals_upload",
                     enable_upload ? upload_intervals : Aws::Utils::Array<Aws::Utils::Json::JsonValue>(0))
          .WithArray("intervals_download",
                     enable_download ? download_intervals : Aws::Utils::Array<Aws::Utils::Json::JsonValue>(0))
          .WithString("environment_id", information_identification.id)
          .WithString("ip", ip)
          .WithString("invocation_start_timestamp", invocation_start_timestamp)
          .WithString("measurement_upload_start_timestamp", enable_upload ? iperf_test_upload_result.second : "-1")
          .WithString("measurement_download_start_timestamp",
                      enable_download ? iperf_test_download_result.second : "-1");
  return aws::lambda_runtime::invocation_response::success(response.View().WriteCompact(), "application/json");
}

}  // namespace skyrise

int main() {
  const skyrise::NetworkIoFunction function_network_bandwidth;
  function_network_bandwidth.HandleRequest();

  return 0;
}
