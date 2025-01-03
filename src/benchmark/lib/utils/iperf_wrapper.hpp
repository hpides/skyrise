#pragma once

#include <cstdint>

// We have to do forward declaration to avoid adding the iperf header in this file.
class iperf_test;

namespace skyrise {

/**
 * This is a simple wrapper around the the IPerf library.
 * We wrap it to avoid linking issues across the AWS SDK and libiperf.
 */
class IperfWrapper {
 public:
  void Initialize();

  int Defaults();

  void SetTestRole(const char role);

  void SetTestReverse(const bool enable_download);

  void SetTestDuration(const int duration_seconds);

  void SetTestServerHostname(const char* public_ip);

  void SetTestServerPort(const int64_t port);

  void SetTestJsonOutput(const bool is_json_output);

  void SetTestUnitFormat(const char format);

  void SetTestNumStreams(const int num_streams);

  void SetTestRate(const int target_throughput_bps);

  void SetTestReporterInterval(const double report_interval);

  void SetTestStatsInterval(const double stats_interval);

  void SetMessageSize(const int message_size);

  int RunClient();

  int EndClient();

  char* GetTestJsonOutputString();

  static char* StringError();

 private:
  iperf_test* iperf_configuration_ = nullptr;
};

}  // namespace skyrise
