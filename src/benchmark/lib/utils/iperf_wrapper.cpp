#include "iperf_wrapper.hpp"

#define HAVE_STDINT_H 1
#define HAVE_STDATOMIC_H 1
#include <iperf_api.h>

namespace skyrise {

void IperfWrapper::Initialize() { iperf_configuration_ = iperf_new_test(); }

int IperfWrapper::Defaults() { return iperf_defaults(iperf_configuration_); }

void IperfWrapper::SetTestRole(const char role) { iperf_set_test_role(iperf_configuration_, role); }

void IperfWrapper::SetTestReverse(const bool enable_download) {
  iperf_set_test_reverse(iperf_configuration_, enable_download);
}

void IperfWrapper::SetTestDuration(const int duration_seconds) {
  iperf_set_test_duration(iperf_configuration_, duration_seconds);
}

void IperfWrapper::SetTestServerHostname(const char* public_ip) {
  iperf_set_test_server_hostname(iperf_configuration_, public_ip);
}

void IperfWrapper::SetTestServerPort(const int64_t port) { iperf_set_test_server_port(iperf_configuration_, port); }

void IperfWrapper::SetTestJsonOutput(const bool is_json_output) {
  iperf_set_test_json_output(iperf_configuration_, is_json_output);
}

void IperfWrapper::SetTestUnitFormat(const char format) { iperf_set_test_unit_format(iperf_configuration_, format); }

void IperfWrapper::SetTestNumStreams(const int num_streams) {
  iperf_set_test_num_streams(iperf_configuration_, num_streams);
}

void IperfWrapper::SetTestRate(const int target_throughput_bps) {
  iperf_set_test_rate(iperf_configuration_, target_throughput_bps);
}

void IperfWrapper::SetTestReporterInterval(const double report_interval) {
  iperf_set_test_reporter_interval(iperf_configuration_, report_interval);
}

void IperfWrapper::SetTestStatsInterval(const double stats_interval) {
  iperf_set_test_stats_interval(iperf_configuration_, stats_interval);
}

void IperfWrapper::SetMessageSize(const int message_size) {
  iperf_set_test_blksize(iperf_configuration_, message_size);
}

char* IperfWrapper::GetTestJsonOutputString() { return iperf_get_test_json_output_string(iperf_configuration_); }

int IperfWrapper::RunClient() { return iperf_run_client(iperf_configuration_); }

int IperfWrapper::EndClient() { return iperf_client_end(iperf_configuration_); }

char* IperfWrapper::StringError() { return iperf_strerror(i_errno); }

}  // namespace skyrise
