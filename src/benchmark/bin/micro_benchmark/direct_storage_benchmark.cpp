#include <CLI/CLI.hpp>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <chrono>
#include <iostream>
#include <random>
#include <vector>
#include <fstream>
#include <nlohmann/json.hpp>

using namespace std::chrono;

// Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your environment before running this benchmark.
// Example:
// export AWS_ACCESS_KEY_ID=your-access-key
// export AWS_SECRET_ACCESS_KEY=your-secret-key
// ./directStorageBenchmark --endpoint ... --bucket ...

class R2Benchmark {
public:
    R2Benchmark(const std::string& endpoint)
        : sdk_options_() {
        Aws::InitAPI(sdk_options_);
        Aws::Client::ClientConfiguration config;
        config.endpointOverride = endpoint;
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = false;
        s3_client_ = std::make_shared<Aws::S3::S3Client>(config);
    }
    ~R2Benchmark() {
        Aws::ShutdownAPI(sdk_options_);
    }
    void RunBenchmark(const std::string& bucket, const std::string& prefix, size_t num_type_a, size_t num_type_b, size_t object_size, size_t num_runs, const std::string& output_file, const std::string& bucket_name) {
        using json = nlohmann::json;
        std::vector<std::string> test_data;
        for (size_t i = 0; i < num_type_a; ++i) {
            test_data.push_back(GenerateRandomString(object_size));
        }
        // Upload (Type A)
        std::vector<double> upload_latencies;
        double upload_total_time = 0;
        for (size_t run = 0; run < num_runs; ++run) {
            for (size_t i = 0; i < test_data.size(); ++i) {
                auto start = high_resolution_clock::now();
                Aws::S3::Model::PutObjectRequest request;
                request.SetBucket(bucket);
                request.SetKey(prefix + "/object_" + std::to_string(i));
                auto input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream");
                *input_data << test_data[i];
                request.SetBody(input_data);
                auto outcome = s3_client_->PutObject(request);
                auto end = high_resolution_clock::now();
                double duration = duration_cast<microseconds>(end - start).count() / 1000.0; // ms
                upload_latencies.push_back(duration);
                upload_total_time += duration;
                if (!outcome.IsSuccess()) {
                    std::cerr << "Error uploading object " << i << ": " 
                              << outcome.GetError().GetMessage() << std::endl;
                }
            }
        }
        // Download (Type B)
        std::vector<double> download_latencies;
        double download_total_time = 0;
        for (size_t run = 0; run < num_runs; ++run) {
            for (size_t i = 0; i < num_type_b; ++i) {
                auto start = high_resolution_clock::now();
                Aws::S3::Model::GetObjectRequest request;
                request.SetBucket(bucket);
                request.SetKey(prefix + "/object_" + std::to_string(i % num_type_a));
                auto outcome = s3_client_->GetObject(request);
                auto end = high_resolution_clock::now();
                double duration = duration_cast<microseconds>(end - start).count() / 1000.0; // ms
                download_latencies.push_back(duration);
                download_total_time += duration;
                if (!outcome.IsSuccess()) {
                    std::cerr << "Error downloading object " << i << ": " 
                              << outcome.GetError().GetMessage() << std::endl;
                }
            }
        }
        // Compute stats
        auto compute_stats = [](const std::vector<double>& latencies, size_t object_size, size_t count) {
            double min_latency = *std::min_element(latencies.begin(), latencies.end());
            double max_latency = *std::max_element(latencies.begin(), latencies.end());
            double avg_latency = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
            double total_time = std::accumulate(latencies.begin(), latencies.end(), 0.0); // ms
            double throughput = (object_size * count) / (1024.0 * 1024.0) / (total_time / 1000.0); // MB/s
            return std::make_tuple(min_latency, max_latency, avg_latency, throughput, total_time);
        };
        auto [min_upload, max_upload, avg_upload, throughput_upload, upload_duration] = compute_stats(upload_latencies, object_size, upload_latencies.size());
        auto [min_download, max_download, avg_download, throughput_download, download_duration] = compute_stats(download_latencies, object_size, download_latencies.size());
        // Prepare JSON
        json j;
        j["parameters"] = {
            {"object_size_kb", object_size / 1024},
            {"object_count", num_type_a},
            {"object_count_type_b", num_type_b},
            {"thread_count", 1},
            {"bucket_name", bucket_name}
        };
        j["write"] = {
            {"latencies_ms", upload_latencies},
            {"min_latency_ms", min_upload},
            {"max_latency_ms", max_upload},
            {"avg_latency_ms", avg_upload},
            {"throughput_mbps", throughput_upload},
            {"duration_ms", upload_duration}
        };
        j["read"] = {
            {"latencies_ms", download_latencies},
            {"min_latency_ms", min_download},
            {"max_latency_ms", max_download},
            {"avg_latency_ms", avg_download},
            {"throughput_mbps", throughput_download},
            {"duration_ms", download_duration}
        };
        std::ofstream ofs(output_file);
        ofs << j.dump(2);
        ofs.close();
        std::cout << "Benchmark results written to " << output_file << std::endl;
    }
private:
    std::string GenerateRandomString(size_t length) {
        static const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);
        std::string result;
        result.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            result += charset[dis(gen)];
        }
        return result;
    }
    Aws::SDKOptions sdk_options_;
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
};

int main(int argc, char** argv) {
    CLI::App app{"R2 Storage Benchmark"};
    std::string endpoint;
    std::string bucket;
    std::string prefix = "benchmark";
    size_t num_type_a = 100;
    size_t num_type_b = 100;
    size_t object_size = 1024 * 1024;  // 1MB
    size_t num_runs = 3;
    std::string output_file = "benchmark_results.json";
    app.add_option("--endpoint", endpoint, "R2 endpoint URL")->required();
    app.add_option("--bucket", bucket, "R2 bucket name")->required();
    app.add_option("--prefix", prefix, "Object key prefix");
    app.add_option("--type-a", num_type_a, "Number of Type A (upload) operations");
    app.add_option("--type-b", num_type_b, "Number of Type B (download) operations");
    app.add_option("--object-size", object_size, "Size of each object in bytes");
    app.add_option("--num-runs", num_runs, "Number of benchmark runs");
    app.add_option("--output", output_file, "Output JSON file");
    CLI11_PARSE(app, argc, argv);
    R2Benchmark benchmark(endpoint);
    benchmark.RunBenchmark(bucket, prefix, num_type_a, num_type_b, object_size, num_runs, output_file, bucket);
    return 0;
} 