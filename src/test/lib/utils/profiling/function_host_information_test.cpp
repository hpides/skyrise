#include "utils/profiling/function_host_information.hpp"

#include <fstream>

#include "gtest/gtest.h"

namespace skyrise {

class FunctionHostInformationTest : public ::testing::Test {
 public:
  FunctionHostInformationCollectorConfiguration config{"/tmp/exampleCgroupFile.txt",
                                                       "echo \"1.2.3.4\"",
                                                       "echo \"5.6.7.8\"",
                                                       false,
                                                       "echo \"OSDetails\"",
                                                       "ls",
                                                       "tmp",
                                                       "/tmp/exampleStatFile.txt",
                                                       "/tmp/exampleUptimeFile",
                                                       "/tmp/exampleCpuinfoFile",
                                                       "/tmp/exampleMeminfoFile",
                                                       true};

  void set_up() const {
    CreateFile("2:cpu,cpuacct:/sandbox-root-pQEzKi/sandbox-service-c53732/sandbox-f22810\n1:blkio:/\n",
               config.cgroup_path);
    CreateFile("ctxt 9999999\nbtime 123456\n", config.stat_path);
    CreateFile("1234.56 789.10\n", config.uptime_path);
    CreateFile(
        "processor\t: 0\nmodel name\t: CpuModelName\nflags\t: three test flags\n\n"
        "processor\t: 1\nmodel name\t: CpuModelName\nflags\t: three test flags\n",
        config.cpuinfo_path);
    CreateFile("MemTotal:      123456 kB\nMemFree:        7890 kB\n", config.meminfo_path);
    mkdir(config.tmp_path.c_str(), 0777);
    CreateFile("1234.56 789.10\n", "tmp/testTmpFile");
  }

  void tear_down() const {
    remove("tmp/testTmpFile");
    rmdir(config.tmp_path.c_str());
    remove(config.meminfo_path.c_str());
    remove(config.cpuinfo_path.c_str());
    remove(config.uptime_path.c_str());
    remove(config.stat_path.c_str());
    remove(config.cgroup_path.c_str());
  }

 private:
  static void CreateFile(const std::string& content, const std::string& filename) {
    std::ofstream file(filename);
    file << content;
  }
};

TEST_F(FunctionHostInformationTest, FunctionHostInformationTestIdentificationWithoutPublicIp) {
  set_up();
  EXPECT_FALSE(config.collect_ip_public);
  config.ip_public_command = "exit 1";

  FunctionHostInformationCollector collector(config);
  FunctionHostInformationIdentification information_identification = collector.CollectInformationIdentification();
  EXPECT_EQ(information_identification.id, "pQEzKi");
  EXPECT_EQ(information_identification.ip_private, "1.2.3.4");
  EXPECT_EQ(information_identification.ip_public, "");

  tear_down();
}

TEST_F(FunctionHostInformationTest, FunctionHostInformationTestIdentificationWithPublicIp) {
  set_up();
  config.collect_ip_public = true;

  FunctionHostInformationCollector collector(config);
  FunctionHostInformationIdentification information_identification = collector.CollectInformationIdentification();
  EXPECT_EQ(information_identification.ip_public, "5.6.7.8");

  tear_down();
}

TEST_F(FunctionHostInformationTest, FunctionHostInformationTestEnvironment) {
  set_up();

  FunctionHostInformationCollector collector(config);
  FunctionHostInformationEnvironment information_environment = collector.CollectInformationEnvironment();
  EXPECT_EQ(information_environment.operating_system_details, "OSDetails");
  EXPECT_EQ(information_environment.file_system_details, "testTmpFile");
  EXPECT_EQ(information_environment.boot_time_seconds, 123456);
  EXPECT_EQ(information_environment.uptime_seconds, 1234);

  tear_down();
}

TEST_F(FunctionHostInformationTest, FunctionHostInformationTestResources) {
  set_up();

  FunctionHostInformationCollector collector(config);
  FunctionHostInformationResources information_resources = collector.CollectInformationResources();
  EXPECT_EQ(information_resources.cpu_count, 2);
  EXPECT_EQ(information_resources.cpu_model, "CpuModelName");
  EXPECT_EQ(information_resources.cpu_features, "three test flags");
  EXPECT_EQ(information_resources.ram_size_mb, 120);

  tear_down();
}

TEST_F(FunctionHostInformationTest, FunctionHostInformationTestJson) {
  set_up();

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
		"ram_size_mb":	120
	}
})""";

  FunctionHostInformationCollector collector(config);
  auto json = collector.CollectJson();
  EXPECT_EQ(json, expected_json);

  tear_down();
}

}  // namespace skyrise
