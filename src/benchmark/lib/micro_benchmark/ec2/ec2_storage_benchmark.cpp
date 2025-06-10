#include "ec2_storage_benchmark.hpp"

#include <fstream>

#include <aws/ec2/model/DescribeInstancesRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/PurgeQueueRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <boost/algorithm/string/predicate.hpp>
#include <magic_enum/magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "ec2_benchmark_output.hpp"
#include "ec2_benchmark_runner.hpp"
#include "ec2_benchmark_types.hpp"
#include "ec2_startup_benchmark.hpp"
#include "function/function_utils.hpp"
#include "storage/backend/s3_storage.hpp"
#include "storage/backend/s3_utils.hpp"
#include "utils/filesystem.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

namespace {

const Aws::String kName = "ec2StorageBenchmark";
const std::string kFunctionName = "skyriseStorageIoFunction";
const std::string kContainer = kSkyriseBenchmarkContainer;
const std::string kPrefix = GetUniqueName(kName);
const std::string kObjectPrefix = kPrefix + "/data";
const std::string kExpectedInvocationResponseTemplate = "{\"request_latencies_ms\"";

}  // namespace

Ec2StorageBenchmark::Ec2StorageBenchmark(const std::vector<Ec2InstanceType>& instance_types,
                                         const std::vector<size_t>& concurrent_instance_counts,
                                         const size_t repetition_count,
                                         const std::vector<size_t>& after_repetition_delays_min,
                                         const size_t object_size_kb, const size_t object_count,
                                         const std::vector<StorageSystem>& storage_types, const bool enable_s3_eoz,
                                         const bool enable_vpc)
    : object_size_kb_(object_size_kb), object_count_(object_count) {
  if (enable_vpc) {
    std::cout << "VPC enabled." << "\n";
  }

  client_ = std::make_shared<BaseClient>();
  benchmark_configs_.reserve(instance_types.size() * concurrent_instance_counts.size());

  for (const auto after_repetition_delay_min : after_repetition_delays_min) {
    std::vector<std::function<void()>> after_repetition_callbacks;
    after_repetition_callbacks.reserve(repetition_count + 1);

    for (size_t i = 0; i < repetition_count; ++i) {
      after_repetition_callbacks.emplace_back([after_repetition_delay_min]() {
        std::this_thread::sleep_for(std::chrono::minutes(after_repetition_delay_min));
      });
    }

    after_repetition_callbacks.emplace_back([]() {});

    for (const auto instance_type : instance_types) {
      for (const auto concurrent_instance_count : concurrent_instance_counts) {
        for (const auto storage_type : storage_types) {
          // const std::string efs_arn = storage_type == StorageSystem::kEfs ? kEfsArn.data() : "";
          const size_t benchmark_id = benchmark_configs_.size();
          const auto user_data = std::function<std::string(size_t, size_t)>(
              [this, benchmark_id](size_t repetition_id, size_t invocation_id) {
                return this->GenerateUserData(benchmark_id, repetition_id, invocation_id);
              });
          const auto benchmark =
              std::function<Aws::Utils::Json::JsonValue(size_t)>([this, benchmark_id](size_t repetition_id) {
                return this->WaitForRepetitionEnd(benchmark_id, repetition_id);
              });

          std::vector<std::vector<std::string>> repetition_prefixes;
          std::vector<std::string> sqs_queue_urls;
          sqs_queue_urls.reserve(repetition_count);
          repetition_prefixes.reserve(repetition_count);
          for (size_t j = 0; j < repetition_count; ++j) {
            std::vector<std::string> instance_prefixes;
            instance_prefixes.reserve(concurrent_instance_count);
            repetition_prefixes.push_back(instance_prefixes);
            sqs_queue_urls.push_back(SetupSqsQueue());
          }

          benchmark_configs_.emplace_back(
              std::make_shared<Ec2BenchmarkConfig>(instance_type, concurrent_instance_count, repetition_count,
                                                   Ec2WarmupType::kNone, user_data, after_repetition_callbacks,
                                                   benchmark, enable_vpc),
              Ec2StorageBenchmarkParameters{.base_parameters = {.instance_type = instance_type,
                                                                .concurrent_instance_count = concurrent_instance_count,
                                                                .repetition_count = repetition_count},
                                            .storage_parameters = {.object_size_kb = object_size_kb,
                                                                   .object_count = object_count,
                                                                   .operation_type = StorageOperation::kWrite,
                                                                   .system_type = storage_type,
                                                                   .is_s3_express = enable_s3_eoz},
                                            .sqs_queue_urls = sqs_queue_urls,
                                            .repetition_prefixes = repetition_prefixes});
        }
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> Ec2StorageBenchmark::Run(
    const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) {
  const auto ec2_benchmark_runner = std::dynamic_pointer_cast<Ec2BenchmarkRunner>(benchmark_runner);
  Assert(ec2_benchmark_runner, "Ec2StorageBenchmark needs an Ec2BenchmarkRunner to run.");

  Setup();

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results_.push_back(ec2_benchmark_runner->RunEc2Config(benchmark_config.first));
  }

  Teardown();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> result_outputs(benchmark_results_.size());

  for (size_t i = 0; i < benchmark_results_.size(); ++i) {
    result_outputs[i] = GenerateResultOutput(benchmark_results_[i], benchmark_configs_[i].second);
  }

  return result_outputs;
}

const Aws::String& Ec2StorageBenchmark::Name() const { return kName; }

void Ec2StorageBenchmark::Setup() {
  UploadLocalFile(GetProjectDirectoryPath() + "bin/" + kFunctionName, "bin/" + kFunctionName);
  UploadLocalFile(GetProjectDirectoryPath() + "../script/local_execute_function.sh", "bin/local_execute_function.sh");
  UploadLocalFile(GetProjectDirectoryPath() + "../script/response_generator.sh", "bin/response_generator.sh");
}

void Ec2StorageBenchmark::Teardown() {
  for (const auto& config : benchmark_configs_) {
    for (const auto& sqs_queue_url : config.second.sqs_queue_urls) {
      client_->GetSqsClient()->DeleteQueue(Aws::SQS::Model::DeleteQueueRequest().WithQueueUrl(sqs_queue_url));
    }
  }
}

void Ec2StorageBenchmark::UploadLocalFile(const std::string& local_path, const std::string& object_id) {
  std::stringbuf string_buffer(ReadFileToString(local_path));
  auto stream = std::make_shared<Aws::IOStream>(&string_buffer);
  stream->seekg(0, std::ios::end);
  const size_t length = stream->tellg();
  stream->seekg(0, std::ios::beg);
  UploadObjectToS3(client_->GetS3Client(), kPrefix + "/" + object_id, stream, length, kContainer);
}

std::string Ec2StorageBenchmark::SetupSqsQueue() const {
  const std::string queue_name = kName + "-" + RandomString(10);
  Aws::SQS::Model::CreateQueueRequest request;
  request.WithQueueName(queue_name);
  const auto outcome = client_->GetSqsClient()->CreateQueue(request);
  Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());
  return outcome.GetResult().GetQueueUrl();
}

std::string Ec2StorageBenchmark::GenerateUserData(const size_t benchmark_id, const size_t repetition_id,
                                                  const size_t invocation_id) {
  const std::string object_prefix = kPrefix + "/data";
  const std::string response_prefix = object_prefix + "/repetition-" + std::to_string(repetition_id) +
                                      "/responses/invocation-" + std::to_string(invocation_id);
  const std::string home_prefix = "/home/ec2-user/";
  benchmark_configs_[benchmark_id].second.repetition_prefixes[repetition_id].push_back(response_prefix);
  // NOLINTBEGIN(concurrency-mt-unsafe,clang-analyzer-cplusplus.StringChecker)
  const std::string id = std::getenv("AWS_ACCESS_KEY_ID");
  const std::string key = std::getenv("AWS_SECRET_ACCESS_KEY");
  // NOLINTEND(concurrency-mt-unsafe,clang-analyzer-cplusplus.StringChecker)
  Assert(!id.empty() && !key.empty(), "AWS credentials not found in environment.");

  const std::string sqs_queue_url = benchmark_configs_[benchmark_id].second.sqs_queue_urls[repetition_id];
  const std::string storage_type_name =
      std::string(magic_enum::enum_name(benchmark_configs_[benchmark_id].second.storage_parameters.system_type));

  std::string filesystem_id = "fs-06694019da98086e1";
  /*const size_t partition_id = invocation_id % 4;
  switch (partition_id) {
    case 0:
      filesystem_id = "fs-050387cde45167631";
      break;
    case 1:
      filesystem_id = "fs-0afa83ccc58b04bdf";
      break;
    case 2:
      filesystem_id = "fs-059e40ff2d866c01a";
      break;
    case 3:
      filesystem_id = "fs-02d6590a15bbc715f";
      break;
  }*/

  auto common_payload =
      Aws::Utils::Json::JsonValue()
          .WithInteger("object_count", object_count_)
          .WithString("storage_type", storage_type_name)
          .WithString("container_name", benchmark_configs_[benchmark_id].second.storage_parameters.is_s3_express
                                            ? kS3ExpressBucket
                                            : kContainer)
          .WithString("object_prefix", object_prefix)
          .WithString("sqs_queue_url", sqs_queue_url)
          .WithInteger("repetition", repetition_id)
          .WithInteger("invocation", invocation_id);

  const Aws::String write_payload =
      common_payload
          .WithInteger("concurrent_instance_count",
                       benchmark_configs_[benchmark_id].second.base_parameters.concurrent_instance_count)
          .WithInt64("object_size_bytes", KBToByte(object_size_kb_))
          .WithString("operation_type", "kWrite")

          .View()
          .WriteCompact();

  const Aws::String read_payload =
      common_payload
          .WithInteger("concurrent_instance_count",
                       benchmark_configs_[benchmark_id].second.base_parameters.concurrent_instance_count > 1
                           ? benchmark_configs_[benchmark_id].second.base_parameters.concurrent_instance_count * 2
                           : benchmark_configs_[benchmark_id].second.base_parameters.concurrent_instance_count)
          .WithString("operation_type", "kRead")
          .View()
          .WriteCompact();

  std::string user_data = kHashbang;
  user_data += "aws configure set region " + client_->GetClientRegion() + " && ";
  user_data += "aws configure set aws_access_key_id " + id + " && ";
  user_data += "aws configure set aws_secret_access_key " + key + " && ";
  user_data += "echo 'AWS_ACCESS_KEY_ID=" + id + "' >> /etc/environment && ";
  user_data += "echo 'AWS_SECRET_ACCESS_KEY=" + key + "' >> /etc/environment && ";
  // TODO(tobodner): Remove this environment variable, once the ASAN alloc-dealloc-mismatch errors are fixed.
  user_data += "echo 'ASAN_OPTIONS=alloc_dealloc_mismatch=0' >> /etc/environment && ";
  user_data += "aws s3 cp s3://" + kContainer + "/" + kPrefix + "/bin " + home_prefix + " --recursive && ";
  user_data += "cd " + home_prefix + " && ";
  user_data += "chmod +x skyriseStorageIoFunction local_execute_function.sh response_generator.sh && ";
  user_data += "source /etc/environment && ";
  user_data += "rm -f function_out.txt && ";
  user_data += "sudo mkdir -p /mnt/efs && ";
  user_data +=
      "sudo mount -t nfs -o "
      "nfsvers=4.1,rsize=1048576,wsize=1048576,hard,sync,timeo=600,retrans=15,noresvport,"
      "noac,actimeo=0,acregmax=0,acdirmax=0,lookupcache=none " +
      filesystem_id + ".efs.us-east-1.amazonaws.com:/   /mnt/efs && ";
  user_data +=
      "sudo ./local_execute_function.sh skyriseStorageIoFunction '" + write_payload + "' 2>&1 >> function_log.txt && ";
  user_data += "aws s3 cp function_out.txt s3://" + kContainer + "/" + response_prefix +
               "-writes.txt && mv function_out.txt writes.txt && ";
  user_data += "sleep 3 && ";
  user_data +=
      "sudo ./local_execute_function.sh skyriseStorageIoFunction '" + read_payload + "' 2>&1 >> function_log.txt && ";
  user_data += "aws s3 cp function_out.txt s3://" + kContainer + "/" + response_prefix +
               "-reads.txt && mv function_out.txt reads.txt";
  return user_data;
}

Aws::Utils::Json::JsonValue Ec2StorageBenchmark::WaitForRepetitionEnd(const size_t benchmark_id,
                                                                      const size_t repetition_id) {
  Aws::Utils::Json::JsonValue json_result;
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> invocation_results(
      benchmark_configs_[benchmark_id].first->GetConcurrentInstanceCount());

  bool are_responses_present = false;
  while (!are_responses_present) {
    const std::string response_prefix = kPrefix + "/data/repetition-" + std::to_string(repetition_id) + "/responses";
    const auto list_objects_outcome = client_->GetS3Client()->ListObjects(
        Aws::S3::Model::ListObjectsRequest().WithBucket(kContainer).WithPrefix(response_prefix));
    Assert(list_objects_outcome.IsSuccess(), list_objects_outcome.GetError().GetMessage());

    const auto& list_objects_result = list_objects_outcome.GetResult();
    const auto& listed_objects = list_objects_result.GetContents();

    const size_t expected_response_count = 2 * benchmark_configs_[benchmark_id].first->GetConcurrentInstanceCount();
    if (listed_objects.size() != expected_response_count) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }
    are_responses_present = true;
  }

  auto storage = S3Storage(client_->GetS3Client(), kContainer);
  for (size_t i = 0; i < benchmark_configs_[benchmark_id].first->GetConcurrentInstanceCount(); ++i) {
    ByteBuffer write_result;
    auto reader_write_result = storage.OpenForReading(
        benchmark_configs_[benchmark_id].second.repetition_prefixes[repetition_id][i] + "-writes.txt");
    StorageError write_error = reader_write_result->Read(0, ObjectReader::kLastByteInFile, &write_result);
    Assert(!write_error.IsError(), "Error while reading response for writes.");
    reader_write_result->Close();
    std::string write_result_string(write_result.CharData(), write_result.Size());

    ByteBuffer read_result;
    auto reader_read_result = storage.OpenForReading(
        benchmark_configs_[benchmark_id].second.repetition_prefixes[repetition_id][i] + "-reads.txt");
    StorageError read_error = reader_read_result->Read(0, ObjectReader::kLastByteInFile, &read_result);
    Assert(!read_error.IsError(), "Error while reading response for reads.");
    reader_read_result->Close();
    std::string read_result_string(read_result.CharData(), read_result.Size());

    auto write_json = ExtractResponse(write_result_string);
    auto read_json = ExtractResponse(read_result_string);
    invocation_results[i] =
        Aws::Utils::Json::JsonValue()
            .WithObject("write_result", write_json)
            .WithObject("read_result", read_json)
            .WithString("bucket_prefix", benchmark_configs_[benchmark_id].second.repetition_prefixes[repetition_id][i]);
  }

  return Aws::Utils::Json::JsonValue().WithArray("invocations", invocation_results);
}

Aws::Utils::Json::JsonValue Ec2StorageBenchmark::ExtractResponse(const std::string& string_response) {
  std::smatch matches;
  const std::regex regex(R"(\{(?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*\})");
  std::string result;

  std::string::const_iterator search_start(string_response.cbegin());
  while (std::regex_search(search_start, string_response.cend(), matches, regex)) {
    if (boost::algorithm::starts_with(matches[0].str(), kExpectedInvocationResponseTemplate)) {
      result = matches[0].str();
      break;
    }
    search_start = matches.suffix().first;
  }

  Assert(!result.empty(), "Could not extract JSON response from\n" + string_response);

  auto json = Aws::Utils::Json::JsonValue(result);

  Assert(json.WasParseSuccessful(), json.GetErrorMessage());

  return json;
}

Aws::Utils::Json::JsonValue Ec2StorageBenchmark::GenerateResultOutput(const std::shared_ptr<Ec2BenchmarkResult>& result,
                                                                      const Ec2StorageBenchmarkParameters& parameters) {
  std::vector<double> write_request_latencies_ms;
  std::vector<double> read_request_latencies_ms;
  const auto& repetition_results = result->GetRepetitionResults();
  const size_t request_latency_count = repetition_results.size() *
                                       repetition_results.front().invocation_results.size() *
                                       parameters.storage_parameters.object_count;
  write_request_latencies_ms.reserve(request_latency_count);
  read_request_latencies_ms.reserve(request_latency_count);

  std::vector<double> write_invocation_throughputs_mbps;
  std::vector<double> read_invocation_throughputs_mbps;
  const size_t invocation_throughput_count =
      repetition_results.size() * repetition_results.front().invocation_results.size();
  write_invocation_throughputs_mbps.reserve(invocation_throughput_count);
  read_invocation_throughputs_mbps.reserve(invocation_throughput_count);

  for (const auto& repetition_result : repetition_results) {
    for (const auto& invocation_result : repetition_result.invocation_results) {
      // Writes.
      const double write_invocation_result_measurement_duration_ms =
          invocation_result.second.result_json.View().GetObject("write_result").GetDouble("measurement_duration_ms");
      const double write_invocation_throughput_mbps =
          static_cast<double>(KBToMB(parameters.storage_parameters.object_size_kb) *
                              parameters.storage_parameters.object_count) /
          (write_invocation_result_measurement_duration_ms / 1000);
      write_invocation_throughputs_mbps.emplace_back(write_invocation_throughput_mbps);
      const auto write_invocation_result_request_latencies_ms =
          invocation_result.second.result_json.View().GetObject("write_result").GetArray("request_latencies_ms");
      for (size_t i = 0; i < write_invocation_result_request_latencies_ms.GetLength(); ++i) {
        write_request_latencies_ms.emplace_back(write_invocation_result_request_latencies_ms[i].AsDouble());
      }
      // Reads.
      const double read_invocation_result_measurement_duration_ms =
          invocation_result.second.result_json.View().GetObject("read_result").GetDouble("measurement_duration_ms");
      const double read_invocation_throughput_mbps =
          static_cast<double>(KBToMB(parameters.storage_parameters.object_size_kb) *
                              parameters.storage_parameters.object_count) /
          (read_invocation_result_measurement_duration_ms / 1000);
      read_invocation_throughputs_mbps.emplace_back(read_invocation_throughput_mbps);
      const auto read_invocation_result_request_latencies_ms =
          invocation_result.second.result_json.View().GetObject("read_result").GetArray("request_latencies_ms");
      for (size_t i = 0; i < read_invocation_result_request_latencies_ms.GetLength(); ++i) {
        read_request_latencies_ms.emplace_back(read_invocation_result_request_latencies_ms[i].AsDouble());
      }
    }
  }

  const BenchmarkResultAggregate write_invocation_throughput_aggregates(write_invocation_throughputs_mbps, 2);
  const BenchmarkResultAggregate read_invocation_throughput_aggregates(read_invocation_throughputs_mbps, 2);
  const BenchmarkResultAggregate write_request_latency_aggregates(write_request_latencies_ms, 2);
  const BenchmarkResultAggregate read_request_latency_aggregates(read_request_latencies_ms, 2);

  return Ec2BenchmarkOutput(Name(), parameters.base_parameters, result)
      .WithStringArgument("instance_type", std::string(magic_enum::enum_name(parameters.base_parameters.instance_type)))
      .WithInt64Argument("concurrent_instance_count", parameters.base_parameters.concurrent_instance_count)
      .WithInt64Argument("repetition_count", parameters.base_parameters.repetition_count)
      .WithInt64Argument("object_size_kb", parameters.storage_parameters.object_size_kb)
      .WithInt64Argument("object_count", object_count_)
      .WithStringArgument("storage_system",
                          std::string(magic_enum::enum_name(parameters.storage_parameters.system_type)))
      .WithStringArgument("container_name", parameters.storage_parameters.is_s3_express ? kS3ExpressBucket : kContainer)
      // Benchmark metrics.
      .WithDoubleMetric("write_request_latency_minimum_ms", write_request_latency_aggregates.GetMinimum())
      .WithDoubleMetric("write_request_latency_maximum_ms", write_request_latency_aggregates.GetMaximum())
      .WithDoubleMetric("write_request_latency_average_ms", write_request_latency_aggregates.GetAverage())
      .WithDoubleMetric("write_request_latency_median_ms", write_request_latency_aggregates.GetMedian())
      .WithDoubleMetric("write_request_latency_percentile_5_ms", write_request_latency_aggregates.GetPercentile(5))
      .WithDoubleMetric("write_request_latency_percentile_25_ms", write_request_latency_aggregates.GetPercentile(25))
      .WithDoubleMetric("write_request_latency_percentile_75_ms", write_request_latency_aggregates.GetPercentile(75))
      .WithDoubleMetric("write_request_latency_percentile_90_ms", write_request_latency_aggregates.GetPercentile(90))
      .WithDoubleMetric("write_request_latency_percentile_95_ms", write_request_latency_aggregates.GetPercentile(95))
      .WithDoubleMetric("write_request_latency_percentile_99_ms", write_request_latency_aggregates.GetPercentile(99))
      .WithDoubleMetric("write_request_latency_percentile_99.9_ms",
                        write_request_latency_aggregates.GetPercentile(99.9))
      .WithDoubleMetric("write_request_latency_percentile_99.99_ms",
                        write_request_latency_aggregates.GetPercentile(99.99))
      .WithDoubleMetric("write_request_latency_std_dev_ms", write_request_latency_aggregates.GetStandardDeviation())
      .WithDoubleMetric("write_invocation_throughput_minimum_mbps", write_invocation_throughput_aggregates.GetMinimum())
      .WithDoubleMetric("write_invocation_throughput_maximum_mbps", write_invocation_throughput_aggregates.GetMaximum())
      .WithDoubleMetric("write_invocation_throughput_average_mbps", write_invocation_throughput_aggregates.GetAverage())
      .WithDoubleMetric("write_invocation_throughput_median_mbps", write_invocation_throughput_aggregates.GetMedian())
      .WithDoubleMetric("write_invocation_throughput_percentile_0.01_mbps",
                        write_invocation_throughput_aggregates.GetPercentile(0.01))
      .WithDoubleMetric("write_invocation_throughput_percentile_0.1_mbps",
                        write_invocation_throughput_aggregates.GetPercentile(0.1))
      .WithDoubleMetric("write_invocation_throughput_percentile_1_mbps",
                        write_invocation_throughput_aggregates.GetPercentile(1))
      .WithDoubleMetric("write_invocation_throughput_percentile_10_mbps",
                        write_invocation_throughput_aggregates.GetPercentile(10))
      .WithDoubleMetric("write_invocation_throughput_std_dev_mbps",
                        write_invocation_throughput_aggregates.GetStandardDeviation())
      .WithDoubleMetric("read_request_latency_minimum_ms", read_request_latency_aggregates.GetMinimum())
      .WithDoubleMetric("read_request_latency_maximum_ms", read_request_latency_aggregates.GetMaximum())
      .WithDoubleMetric("read_request_latency_average_ms", read_request_latency_aggregates.GetAverage())
      .WithDoubleMetric("read_request_latency_median_ms", read_request_latency_aggregates.GetMedian())
      .WithDoubleMetric("read_request_latency_percentile_5_ms", read_request_latency_aggregates.GetPercentile(5))
      .WithDoubleMetric("read_request_latency_percentile_25_ms", read_request_latency_aggregates.GetPercentile(25))
      .WithDoubleMetric("read_request_latency_percentile_75_ms", read_request_latency_aggregates.GetPercentile(75))
      .WithDoubleMetric("read_request_latency_percentile_90_ms", read_request_latency_aggregates.GetPercentile(90))
      .WithDoubleMetric("read_request_latency_percentile_95_ms", read_request_latency_aggregates.GetPercentile(95))
      .WithDoubleMetric("read_request_latency_percentile_99_ms", read_request_latency_aggregates.GetPercentile(99))
      .WithDoubleMetric("read_request_latency_percentile_99.9_ms", read_request_latency_aggregates.GetPercentile(99.9))
      .WithDoubleMetric("read_request_latency_percentile_99.99_ms",
                        read_request_latency_aggregates.GetPercentile(99.99))
      .WithDoubleMetric("read_request_latency_std_dev_ms", read_request_latency_aggregates.GetStandardDeviation())
      .WithDoubleMetric("read_invocation_throughput_minimum_mbps", read_invocation_throughput_aggregates.GetMinimum())
      .WithDoubleMetric("read_invocation_throughput_maximum_mbps", read_invocation_throughput_aggregates.GetMaximum())
      .WithDoubleMetric("read_invocation_throughput_average_mbps", read_invocation_throughput_aggregates.GetAverage())
      .WithDoubleMetric("read_invocation_throughput_median_mbps", read_invocation_throughput_aggregates.GetMedian())
      .WithDoubleMetric("read_invocation_throughput_percentile_0.01_mbps",
                        read_invocation_throughput_aggregates.GetPercentile(0.01))
      .WithDoubleMetric("read_invocation_throughput_percentile_0.1_mbps",
                        read_invocation_throughput_aggregates.GetPercentile(0.1))
      .WithDoubleMetric("read_invocation_throughput_percentile_1_mbps",
                        read_invocation_throughput_aggregates.GetPercentile(1))
      .WithDoubleMetric("read_invocation_throughput_percentile_10_mbps",
                        read_invocation_throughput_aggregates.GetPercentile(10))
      .WithDoubleMetric("read_invocation_throughput_std_dev_mbps",
                        read_invocation_throughput_aggregates.GetStandardDeviation())
      // Repetition metrics.
      .WithDoubleRepetitionMetric([&](const Ec2BenchmarkRepetitionResult& repetition_result) {
        std::vector<double> invocation_write_measurement_starts_ms;
        invocation_write_measurement_starts_ms.reserve(repetition_result.invocation_results.size());
        std::vector<double> invocation_write_measurement_ends_ms;
        invocation_write_measurement_ends_ms.reserve(repetition_result.invocation_results.size());
        for (auto const& invocation_result : repetition_result.invocation_results) {
          const double invocation_measurement_start_ms =
              invocation_result.second.result_json.View().GetObject("write_result").GetDouble("measurement_start_ms");
          invocation_write_measurement_starts_ms.emplace_back(invocation_measurement_start_ms);
          const double invocation_measurement_end_ms =
              invocation_result.second.result_json.View().GetObject("write_result").GetDouble("measurement_end_ms");
          invocation_write_measurement_ends_ms.emplace_back(invocation_measurement_end_ms);
        }

        const double repetition_write_measurement_start_ms =
            *std::ranges::min_element(invocation_write_measurement_starts_ms);
        const double repetition_write_measurement_end_ms =
            *std::ranges::max_element(invocation_write_measurement_ends_ms);
        const double repetition_write_measurement_duration_ms =
            repetition_write_measurement_end_ms - repetition_write_measurement_start_ms;

        const double repetition_write_throughput_mbps =
            static_cast<double>(KBToMB(parameters.storage_parameters.object_size_kb) *
                                parameters.storage_parameters.object_count *
                                parameters.base_parameters.concurrent_instance_count) /
            (repetition_write_measurement_duration_ms / 1000);
        return std::make_tuple("repetition_write_throughput_mbps", repetition_write_throughput_mbps);
      })
      .WithDoubleRepetitionMetric([&](const Ec2BenchmarkRepetitionResult& repetition_result) {
        std::vector<double> invocation_write_measurement_starts_ms;
        invocation_write_measurement_starts_ms.reserve(repetition_result.invocation_results.size());
        std::vector<double> invocation_write_measurement_ends_ms;
        invocation_write_measurement_ends_ms.reserve(repetition_result.invocation_results.size());
        for (auto const& invocation_result : repetition_result.invocation_results) {
          const double invocation_measurement_start_ms =
              invocation_result.second.result_json.View().GetObject("write_result").GetDouble("measurement_start_ms");
          invocation_write_measurement_starts_ms.emplace_back(invocation_measurement_start_ms);
          const double invocation_measurement_end_ms =
              invocation_result.second.result_json.View().GetObject("write_result").GetDouble("measurement_end_ms");
          invocation_write_measurement_ends_ms.emplace_back(invocation_measurement_end_ms);
        }

        const double repetition_write_measurement_start_ms =
            *std::ranges::min_element(invocation_write_measurement_starts_ms);
        const double repetition_write_measurement_end_ms =
            *std::ranges::max_element(invocation_write_measurement_ends_ms);
        const double repetition_write_measurement_duration_ms =
            repetition_write_measurement_end_ms - repetition_write_measurement_start_ms;

        const double repetition_write_iops = static_cast<double>(parameters.storage_parameters.object_count *
                                                                 parameters.base_parameters.concurrent_instance_count) /
                                             (repetition_write_measurement_duration_ms / 1000);
        return std::make_tuple("repetition_write_iops", repetition_write_iops);
      })
      .WithDoubleRepetitionMetric([&](const Ec2BenchmarkRepetitionResult& repetition_result) {
        std::vector<double> invocation_read_measurement_starts_ms;
        invocation_read_measurement_starts_ms.reserve(repetition_result.invocation_results.size());
        std::vector<double> invocation_read_measurement_ends_ms;
        invocation_read_measurement_ends_ms.reserve(repetition_result.invocation_results.size());
        for (auto const& invocation_result : repetition_result.invocation_results) {
          const double invocation_measurement_start_ms =
              invocation_result.second.result_json.View().GetObject("read_result").GetDouble("measurement_start_ms");
          invocation_read_measurement_starts_ms.emplace_back(invocation_measurement_start_ms);
          const double invocation_measurement_end_ms =
              invocation_result.second.result_json.View().GetObject("read_result").GetDouble("measurement_end_ms");
          invocation_read_measurement_ends_ms.emplace_back(invocation_measurement_end_ms);
        }

        const double repetition_read_measurement_start_ms =
            *std::ranges::min_element(invocation_read_measurement_starts_ms);
        const double repetition_read_measurement_end_ms =
            *std::ranges::max_element(invocation_read_measurement_ends_ms);
        const double repetition_read_measurement_duration_ms =
            repetition_read_measurement_end_ms - repetition_read_measurement_start_ms;

        const double repetition_read_throughput_mbps =
            static_cast<double>(KBToMB(parameters.storage_parameters.object_size_kb) *
                                parameters.storage_parameters.object_count *
                                parameters.base_parameters.concurrent_instance_count) /
            (repetition_read_measurement_duration_ms / 1000);
        return std::make_tuple("repetition_read_throughput_mbps", repetition_read_throughput_mbps);
      })
      .WithDoubleRepetitionMetric([&](const Ec2BenchmarkRepetitionResult& repetition_result) {
        std::vector<double> invocation_read_measurement_starts_ms;
        invocation_read_measurement_starts_ms.reserve(repetition_result.invocation_results.size());
        std::vector<double> invocation_read_measurement_ends_ms;
        invocation_read_measurement_ends_ms.reserve(repetition_result.invocation_results.size());
        for (auto const& invocation_result : repetition_result.invocation_results) {
          const double invocation_measurement_start_ms =
              invocation_result.second.result_json.View().GetObject("read_result").GetDouble("measurement_start_ms");
          invocation_read_measurement_starts_ms.emplace_back(invocation_measurement_start_ms);
          const double invocation_measurement_end_ms =
              invocation_result.second.result_json.View().GetObject("read_result").GetDouble("measurement_end_ms");
          invocation_read_measurement_ends_ms.emplace_back(invocation_measurement_end_ms);
        }

        const double repetition_read_measurement_start_ms = *std::ranges::min_element(
            invocation_read_measurement_starts_ms.begin(), invocation_read_measurement_starts_ms.end());
        const double repetition_read_measurement_end_ms =
            *std::ranges::max_element(invocation_read_measurement_ends_ms);
        const double repetition_read_measurement_duration_ms =
            repetition_read_measurement_end_ms - repetition_read_measurement_start_ms;

        const double repetition_read_iops = static_cast<double>(parameters.storage_parameters.object_count *
                                                                parameters.base_parameters.concurrent_instance_count) /
                                            (repetition_read_measurement_duration_ms / 1000);
        return std::make_tuple("repetition_read_iops", repetition_read_iops);
      })
      .WithObjectRepetitionMetric([&](const Ec2BenchmarkRepetitionResult& repetition_result) {
        std::vector<double> write_request_latencies_ms;
        std::vector<double> read_request_latencies_ms;
        const size_t request_latency_count =
            repetition_result.invocation_results.size() * parameters.storage_parameters.object_count;
        write_request_latencies_ms.reserve(request_latency_count);
        read_request_latencies_ms.reserve(request_latency_count);

        for (const auto& invocation_result : repetition_result.invocation_results) {
          const auto write_invocation_result_request_latencies_ms =
              invocation_result.second.result_json.View().GetObject("write_result").GetArray("request_latencies_ms");
          for (size_t i = 0; i < write_invocation_result_request_latencies_ms.GetLength(); ++i) {
            write_request_latencies_ms.emplace_back(write_invocation_result_request_latencies_ms[i].AsDouble());
          }
          const auto read_invocation_result_request_latencies_ms =
              invocation_result.second.result_json.View().GetObject("read_result").GetArray("request_latencies_ms");
          for (size_t i = 0; i < read_invocation_result_request_latencies_ms.GetLength(); ++i) {
            read_request_latencies_ms.emplace_back(read_invocation_result_request_latencies_ms[i].AsDouble());
          }
        }

        const BenchmarkResultAggregate write_request_latencies_ms_aggregates(write_request_latencies_ms, 2);
        const BenchmarkResultAggregate read_request_latencies_ms_aggregates(read_request_latencies_ms, 2);

        auto metrics = Aws::Utils::Json::JsonValue()
                           .WithDouble("repetition_write_request_latency_minimum_ms",
                                       write_request_latencies_ms_aggregates.GetMinimum())
                           .WithDouble("repetition_write_request_latency_maximum_ms",
                                       write_request_latencies_ms_aggregates.GetMaximum())
                           .WithDouble("repetition_write_request_latency_average_ms",
                                       write_request_latencies_ms_aggregates.GetAverage())
                           .WithDouble("repetition_write_request_latency_median_ms",
                                       write_request_latencies_ms_aggregates.GetMedian())
                           .WithDouble("repetition_write_request_latency_percentile_5_ms",
                                       write_request_latencies_ms_aggregates.GetPercentile(5))
                           .WithDouble("repetition_write_request_latency_percentile_25_ms",
                                       write_request_latencies_ms_aggregates.GetPercentile(25))
                           .WithDouble("repetition_write_request_latency_percentile_75_ms",
                                       write_request_latencies_ms_aggregates.GetPercentile(75))
                           .WithDouble("repetition_write_request_latency_percentile_90_ms",
                                       write_request_latencies_ms_aggregates.GetPercentile(90))
                           .WithDouble("repetition_write_request_latency_percentile_95_ms",
                                       write_request_latencies_ms_aggregates.GetPercentile(95))
                           .WithDouble("repetition_write_request_latency_percentile_99_ms",
                                       write_request_latencies_ms_aggregates.GetPercentile(99))
                           .WithDouble("repetition_write_request_latency_percentile_99.9_ms",
                                       write_request_latencies_ms_aggregates.GetPercentile(99.9))
                           .WithDouble("repetition_write_request_latency_percentile_99.99_ms",
                                       write_request_latencies_ms_aggregates.GetPercentile(99.99))
                           .WithDouble("repetition_write_request_latency_std_dev_ms",
                                       write_request_latencies_ms_aggregates.GetStandardDeviation())
                           .WithDouble("repetition_read_request_latency_minimum_ms",
                                       read_request_latencies_ms_aggregates.GetMinimum())
                           .WithDouble("repetition_read_request_latency_maximum_ms",
                                       read_request_latencies_ms_aggregates.GetMaximum())
                           .WithDouble("repetition_read_request_latency_average_ms",
                                       read_request_latencies_ms_aggregates.GetAverage())
                           .WithDouble("repetition_read_request_latency_median_ms",
                                       read_request_latencies_ms_aggregates.GetMedian())
                           .WithDouble("repetition_read_request_latency_percentile_5_ms",
                                       read_request_latencies_ms_aggregates.GetPercentile(5))
                           .WithDouble("repetition_read_request_latency_percentile_25_ms",
                                       read_request_latencies_ms_aggregates.GetPercentile(25))
                           .WithDouble("repetition_read_request_latency_percentile_75_ms",
                                       read_request_latencies_ms_aggregates.GetPercentile(75))
                           .WithDouble("repetition_read_request_latency_percentile_90_ms",
                                       read_request_latencies_ms_aggregates.GetPercentile(90))
                           .WithDouble("repetition_read_request_latency_percentile_95_ms",
                                       read_request_latencies_ms_aggregates.GetPercentile(95))
                           .WithDouble("repetition_read_request_latency_percentile_99_ms",
                                       read_request_latencies_ms_aggregates.GetPercentile(99))
                           .WithDouble("repetition_read_request_latency_percentile_99.9_ms",
                                       read_request_latencies_ms_aggregates.GetPercentile(99.9))
                           .WithDouble("repetition_read_request_latency_percentile_99.99_ms",
                                       read_request_latencies_ms_aggregates.GetPercentile(99.99))
                           .WithDouble("repetition_read_request_latency_std_dev_ms",
                                       read_request_latencies_ms_aggregates.GetStandardDeviation());
        return std::make_tuple("repetition_metrics_request_latency", metrics.View().WriteCompact());
      })
      .WithObjectRepetitionMetric([&](const Ec2BenchmarkRepetitionResult& repetition_result) {
        size_t write_finished_request_count = 0;
        size_t write_succeeded_request_count = 0;
        size_t write_retried_request_count = 0;
        size_t write_failed_request_count = 0;
        size_t read_finished_request_count = 0;
        size_t read_succeeded_request_count = 0;
        size_t read_retried_request_count = 0;
        size_t read_failed_request_count = 0;

        for (auto const& invocation_result : repetition_result.invocation_results) {
          // TODO(tobodner): Check for successful invocation.
          if (parameters.storage_parameters.system_type == StorageSystem::kEfs) {
            // TODO(tobodner): Implement lower-level filesystem checks.
            write_succeeded_request_count =
                parameters.base_parameters.concurrent_instance_count * parameters.storage_parameters.object_count;
            write_finished_request_count = write_succeeded_request_count;
            read_succeeded_request_count =
                parameters.base_parameters.concurrent_instance_count * parameters.storage_parameters.object_count;
            read_finished_request_count = read_succeeded_request_count;
          } else {
            std::string write_request_name;
            std::string read_request_name;
            if (parameters.storage_parameters.system_type == StorageSystem::kS3) {
              write_request_name = "S3:PutObject";
              read_request_name = "S3:GetObject";
            } else if (parameters.storage_parameters.system_type == StorageSystem::kDynamoDb) {
              write_request_name = "DynamoDB:PutItem";
              read_request_name = "DynamoDB:GetItem";
            }

            const auto& write_metering = invocation_result.second.result_json.View()
                                             .GetObject("write_result")
                                             .GetObject("metering")
                                             .GetObject(write_request_name);
            write_finished_request_count += write_metering.GetInt64("finished");
            write_succeeded_request_count += write_metering.GetInt64("succeeded");
            write_retried_request_count += write_metering.GetInt64("retried");
            write_failed_request_count += write_metering.GetInt64("failed");

            const auto& read_metering = invocation_result.second.result_json.View()
                                            .GetObject("read_result")
                                            .GetObject("metering")
                                            .GetObject(read_request_name);
            read_finished_request_count += read_metering.GetInt64("finished");
            read_succeeded_request_count += read_metering.GetInt64("succeeded");
            read_retried_request_count += read_metering.GetInt64("retried");
            read_failed_request_count += read_metering.GetInt64("failed");
          }
        }

        return std::make_tuple(
            "repetition_metrics_request_tracking",
            Aws::Utils::Json::JsonValue()
                .WithInteger("repetition_write_finished_request_count", write_finished_request_count)
                .WithInteger("repetition_write_succeeded_request_count", write_succeeded_request_count)
                .WithInteger("repetition_write_retried_request_count", write_retried_request_count)
                .WithInteger("repetition_write_failed_request_count", write_failed_request_count)
                .WithInteger("repetition_read_finished_request_count", read_finished_request_count)
                .WithInteger("repetition_read_succeeded_request_count", read_succeeded_request_count)
                .WithInteger("repetition_read_retried_request_count", read_retried_request_count)
                .WithInteger("repetition_read_failed_request_count", read_failed_request_count));
      })
      // Invocation metrics.
      .WithStringInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple("bucket_prefix", invocation_result.result_json.View().GetString("bucket_prefix"));
      })
      .WithObjectInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "write_metering",
            invocation_result.result_json.View().GetObject("write_result").KeyExists(kResponseMeteringAttribute)
                ? invocation_result.result_json.View()
                      .GetObject("write_result")
                      .GetObject(kResponseMeteringAttribute)
                      .WriteCompact()
                : "");
      })
      .WithDoubleInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "write_measurement_start_ms",
            invocation_result.result_json.View().GetObject("write_result").GetDouble("measurement_start_ms"));
      })
      .WithDoubleInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "write_measurement_end_ms",
            invocation_result.result_json.View().GetObject("write_result").GetDouble("measurement_end_ms"));
      })
      .WithDoubleInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "write_measurement_duration_ms",
            invocation_result.result_json.View().GetObject("write_result").GetDouble("measurement_duration_ms"));
      })
      .WithDoubleInvocationMetric([&](const Ec2BenchmarkInvocationResult& invocation_result) {
        const double write_invocation_result_measurement_duration_ms =
            invocation_result.result_json.View().GetObject("write_result").GetDouble("measurement_duration_ms");
        const double write_invocation_throughput_mbps =
            static_cast<double>(KBToMB(parameters.storage_parameters.object_size_kb) *
                                parameters.storage_parameters.object_count) /
            (write_invocation_result_measurement_duration_ms / 1000);
        return std::make_tuple("write_invocation_throughput_mbps", write_invocation_throughput_mbps);
      })
      .WithObjectInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "read_metering",
            invocation_result.result_json.View().GetObject("read_result").KeyExists(kResponseMeteringAttribute)
                ? invocation_result.result_json.View()
                      .GetObject("read_result")
                      .GetObject(kResponseMeteringAttribute)
                      .WriteCompact()
                : "");
      })
      .WithDoubleInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "read_measurement_start_ms",
            invocation_result.result_json.View().GetObject("read_result").GetDouble("measurement_start_ms"));
      })
      .WithDoubleInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "read_measurement_end_ms",
            invocation_result.result_json.View().GetObject("read_result").GetDouble("measurement_end_ms"));
      })
      .WithDoubleInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "read_measurement_duration_ms",
            invocation_result.result_json.View().GetObject("read_result").GetDouble("measurement_duration_ms"));
      })
      .WithDoubleInvocationMetric([&](const Ec2BenchmarkInvocationResult& invocation_result) {
        const double read_invocation_result_measurement_duration_ms =
            invocation_result.result_json.View().GetObject("read_result").GetDouble("measurement_duration_ms");
        const double read_invocation_throughput_mbps =
            static_cast<double>(KBToMB(parameters.storage_parameters.object_size_kb) *
                                parameters.storage_parameters.object_count) /
            (read_invocation_result_measurement_duration_ms / 1000);
        return std::make_tuple("read_invocation_throughput_mbps", read_invocation_throughput_mbps);
      })
      .WithStringInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "write_invocation_start_timestamp",
            invocation_result.result_json.View().GetObject("write_result").GetString("invocation_start_timestamp"));
      })
      .WithStringInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "write_measurement_start_timestamp",
            invocation_result.result_json.View().GetObject("write_result").GetString("measurement_start_timestamp"));
      })
      .WithStringInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "write_measurement_end_timestamp",
            invocation_result.result_json.View().GetObject("write_result").GetString("measurement_end_timestamp"));
      })
      .WithStringInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "read_invocation_start_timestamp",
            invocation_result.result_json.View().GetObject("read_result").GetString("invocation_start_timestamp"));
      })
      .WithStringInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "read_measurement_start_timestamp",
            invocation_result.result_json.View().GetObject("read_result").GetString("measurement_start_timestamp"));
      })
      .WithStringInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        return std::make_tuple(
            "read_measurement_end_timestamp",
            invocation_result.result_json.View().GetObject("read_result").GetString("measurement_end_timestamp"));
      })
      .WithObjectInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        auto request_latencies_ms =
            invocation_result.result_json.View().GetObject("write_result").GetArray("request_latencies_ms");
        Aws::Utils::Array<Aws::Utils::Json::JsonValue> latencies_json(request_latencies_ms.GetLength());

        for (size_t i = 0; i < request_latencies_ms.GetLength(); ++i) {
          latencies_json[i] = Aws::Utils::Json::JsonValue().AsDouble(request_latencies_ms[i].AsDouble());
        }

        return std::make_tuple("write_invocation_request_latencies_ms",
                               Aws::Utils::Json::JsonValue().AsArray(latencies_json));
      })
      .WithObjectInvocationMetric([](const Ec2BenchmarkInvocationResult& invocation_result) {
        auto request_latencies_ms =
            invocation_result.result_json.View().GetObject("read_result").GetArray("request_latencies_ms");
        Aws::Utils::Array<Aws::Utils::Json::JsonValue> latencies_json(request_latencies_ms.GetLength());

        for (size_t i = 0; i < request_latencies_ms.GetLength(); ++i) {
          latencies_json[i] = Aws::Utils::Json::JsonValue().AsDouble(request_latencies_ms[i].AsDouble());
        }

        return std::make_tuple("read_invocation_request_latencies_ms",
                               Aws::Utils::Json::JsonValue().AsArray(latencies_json));
      })
      .Build();
}

}  // namespace skyrise
