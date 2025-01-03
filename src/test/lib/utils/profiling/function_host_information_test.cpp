#include "utils/profiling/function_host_information.hpp"

#include <fstream>

#include <gtest/gtest.h>

namespace skyrise {

class FunctionHostInformationTest : public ::testing::Test {
 protected:
  static void CreateFile(const std::string& content, const std::string& filename) {
    std::ofstream file(filename);
    file << content;
  }

  void SetUp() override {
    CreateFile("2:cpu,cpuacct:/sandbox-root-pQEzKi/sandbox-service-c53732/sandbox-f22810\n1:blkio:/\n",
               config_.cgroup_path);
    mkdir(config_.tmp_path.c_str(), 0777);
    CreateFile("1234.56 789.10\n", "tmp/testTmpFile");
    CreateFile("ctxt 9999999\nbtime 123456\n", config_.stat_path);
    CreateFile("1234.56 789.10\n", config_.uptime_path);
    CreateFile(
        "processor\t: 0\nmodel name\t: CpuModelName\nflags\t: three test flags\n\n"
        "processor\t: 1\nmodel name\t: CpuModelName\nflags\t: three test flags\n",
        config_.cpuinfo_path);
    CreateFile("MemTotal:      123456 kB\nMemFree:        7890 kB\nMemAvailable:      123456 kB\n",
               config_.meminfo_path);
    CreateFile("VmRSS:       232 kB\nVmSize:   114660 kB\n", config_.self_status_path);
  }

  void TearDown() override {
    remove("tmp/testTmpFile");
    rmdir(config_.tmp_path.c_str());
    remove(config_.meminfo_path.c_str());
    remove(config_.cpuinfo_path.c_str());
    remove(config_.uptime_path.c_str());
    remove(config_.stat_path.c_str());
    remove(config_.cgroup_path.c_str());
    remove(config_.self_status_path.c_str());
  }

  FunctionHostInformationCollectorConfiguration config_{"/tmp/exampleCgroupFile.txt",
                                                        "echo \"5.6.7.8\"",
                                                        false,
                                                        "echo \"OSDetails\"",
                                                        "ls",
                                                        "tmp",
                                                        "/tmp/exampleStatFile.txt",
                                                        "/tmp/exampleUptimeFile",
                                                        "/tmp/exampleCpuinfoFile",
                                                        "/tmp/exampleMeminfoFile",
                                                        "/tmp/exampleSelfstatusFile",
                                                        true};
};

TEST_F(FunctionHostInformationTest, FunctionHostInformationTestIdentificationWithoutPublicIp) {
  EXPECT_FALSE(config_.collect_ip_public);
  config_.ip_public_command = "exit 1";

  FunctionHostInformationCollector collector(config_);
  const FunctionHostInformationIdentification information_identification = collector.CollectInformationIdentification();
  EXPECT_EQ(information_identification.id, "pQEzKi");
  // We don't know the real private ip address, but at least, it should not be empty.
  EXPECT_NE(information_identification.ip_private, "");
  EXPECT_EQ(information_identification.ip_public, "");
}

TEST_F(FunctionHostInformationTest, FunctionHostInformationTestIdentificationWithPublicIp) {
  config_.collect_ip_public = true;

  FunctionHostInformationCollector collector(config_);
  const FunctionHostInformationIdentification information_identification = collector.CollectInformationIdentification();
  EXPECT_EQ(information_identification.ip_public, "5.6.7.8");
}

TEST_F(FunctionHostInformationTest, FunctionHostInformationTestEnvironment) {
  FunctionHostInformationCollector collector(config_);
  const FunctionHostInformationEnvironment information_environment = collector.CollectInformationEnvironment();
  EXPECT_EQ(information_environment.operating_system_details, "OSDetails");
  EXPECT_EQ(information_environment.file_system_details, "testTmpFile");
  EXPECT_EQ(information_environment.boot_time_seconds, 123456);
  EXPECT_EQ(information_environment.uptime_seconds, 1234);
}

TEST_F(FunctionHostInformationTest, FunctionHostInformationTestResources) {
  FunctionHostInformationCollector collector(config_);
  const FunctionHostInformationResources information_resources = collector.CollectInformationResources();
  EXPECT_EQ(information_resources.cpu_count, 2);
  EXPECT_EQ(information_resources.cpu_model, "CpuModelName");
  EXPECT_EQ(information_resources.cpu_features, "three test flags");
  EXPECT_EQ(information_resources.ram_size_mb, 123);
}

TEST_F(FunctionHostInformationTest, FunctionHostInformationTestJson) {
  const auto* expected_json = R"""({
	"identification":	{
		"id":	"pQEzKi",
		"ip_private":	"1.2.3.4",
		"ip_public":	""
	},
	"environment":	{
		"operating_system_details":	"OSDetails",
		"file_system_details":	"testTmpFile",
		"boot_time_seconds":	123456,
		"uptime_seconds":	1234
	},
	"resources":	{
		"cpu_count":	2,
		"cpu_model":	"CpuModelName",
		"cpu_features":	"three test flags",
		"ram_size_mb":	123
	}
})""";

  FunctionHostInformationCollector collector(config_);
  const std::string collected_json = collector.CollectJson();
  ASSERT_TRUE(collected_json.find("pQEzKi") != collected_json.npos);

  const auto information_identification = collector.CollectInformationIdentification();
  const auto information_environment = collector.CollectInformationEnvironment();
  const auto information_resources = collector.CollectInformationResources();
  const FunctionHostInformationIdentification mocked_information_identification{
      information_identification.id, "1.2.3.4", information_identification.ip_public};
  auto json = collector.AsJson(mocked_information_identification, information_environment, information_resources);
  EXPECT_EQ(json, expected_json);
}

TEST_F(FunctionHostInformationTest, FunctionInformationResourceUsage) {
  FunctionHostInformationCollector collector(config_);
  auto resource_usage = collector.CollectInformationResourceUsage();

  EXPECT_EQ(resource_usage.system_available_memory_kb, 123456);
  EXPECT_EQ(resource_usage.system_free_memory_kb, 7890);
  EXPECT_EQ(resource_usage.process_used_memory_kb, 114660);
  EXPECT_EQ(resource_usage.process_used_memory_in_ram_kb, 232);
}

}  // namespace skyrise
