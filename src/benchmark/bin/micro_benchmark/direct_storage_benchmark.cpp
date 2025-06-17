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
#include "configuration.hpp"
#include <thread>
#include <mutex>

using namespace std::chrono;

// Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your environment before running this benchmark.
// Example:
// export AWS_ACCESS_KEY_ID=your-access-key
// export AWS_SECRET_ACCESS_KEY=your-secret-key
// ./directStorageBenchmark --endpoint ... --bucket ...
// python3 script/benchmark/experiment/visualize_benchmark_r2.py test_results.json

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
    void RunBenchmark(const std::string& bucket, const std::string& prefix, size_t num_type_a, size_t num_type_b, size_t object_size, size_t num_runs, const std::string& output_file, const std::string& bucket_name, size_t thread_count) {
        using json = nlohmann::json;
        std::cout << "Starting R2 microbenchmark with parameters:\n"
                  << "  Bucket: " << bucket << "\n"
                  << "  Prefix: " << prefix << "\n"
                  << "  Type A (upload) ops: " << num_type_a << "\n"
                  << "  Type B (download) ops: " << num_type_b << "\n"
                  << "  Object size: " << object_size << " bytes\n"
                  << "  Runs: " << num_runs << "\n"
                  << "  Threads: " << thread_count << "\n"
                  << "  Output file: " << output_file << std::endl;
        std::vector<std::string> test_data;
        for (size_t i = 0; i < num_type_a; ++i) {
            test_data.push_back(GenerateRandomString(object_size));
        }
        // Upload (Type A)
        std::cout << "\n[Upload Phase]" << std::endl;
        std::vector<double> upload_latencies;
        std::mutex upload_mutex;
        auto upload_t0 = high_resolution_clock::now();
        for (size_t run = 0; run < num_runs; ++run) {
            std::cout << "  Run " << (run + 1) << "/" << num_runs << std::endl;
            std::vector<std::thread> threads;
            size_t objects_per_thread = (test_data.size() + thread_count - 1) / thread_count;
            for (size_t t = 0; t < thread_count; ++t) {
                threads.emplace_back([&, t]() {
                    size_t start = t * objects_per_thread;
                    size_t end = std::min(start + objects_per_thread, test_data.size());
                    std::vector<double> local_latencies;
                    for (size_t i = start; i < end; ++i) {
                        if ((i % 10 == 0 || i == end - 1) && t == 0) {
                            std::cout << "    Uploading object " << (i + 1) << "/" << test_data.size() << "..." << std::endl;
                        }
                        auto start_time = high_resolution_clock::now();
                        Aws::S3::Model::PutObjectRequest request;
                        request.SetBucket(bucket);
                        request.SetKey(prefix + "/object_" + std::to_string(i));
                        auto input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream");
                        *input_data << test_data[i];
                        request.SetBody(input_data);
                        auto outcome = s3_client_->PutObject(request);
                        auto end_time = high_resolution_clock::now();
                        double duration = duration_cast<microseconds>(end_time - start_time).count() / 1000.0; // ms
                        local_latencies.push_back(duration);
                        if (!outcome.IsSuccess()) {
                            std::lock_guard<std::mutex> lock(upload_mutex);
                            std::cerr << "Error uploading object " << i << ": " 
                                      << outcome.GetError().GetMessage() << std::endl;
                        }
                    }
                    std::lock_guard<std::mutex> lock(upload_mutex);
                    upload_latencies.insert(upload_latencies.end(), local_latencies.begin(), local_latencies.end());
                });
            }
            for (auto& th : threads) th.join();
        }
        auto upload_t1 = high_resolution_clock::now();
        double upload_wall_time_ms = duration_cast<microseconds>(upload_t1 - upload_t0).count() / 1000.0;
        std::cout << "[Upload Phase Complete]" << std::endl;
        // Download (Type B)
        std::cout << "\n[Download Phase]" << std::endl;
        std::vector<double> download_latencies;
        std::mutex download_mutex;
        auto download_t0 = high_resolution_clock::now();
        for (size_t run = 0; run < num_runs; ++run) {
            std::cout << "  Run " << (run + 1) << "/" << num_runs << std::endl;
            std::vector<std::thread> threads;
            size_t objects_per_thread = (num_type_b + thread_count - 1) / thread_count;
            for (size_t t = 0; t < thread_count; ++t) {
                threads.emplace_back([&, t]() {
                    size_t start = t * objects_per_thread;
                    size_t end = std::min(start + objects_per_thread, num_type_b);
                    std::vector<double> local_latencies;
                    for (size_t i = start; i < end; ++i) {
                        if ((i % 10 == 0 || i == end - 1) && t == 0) {
                            std::cout << "    Downloading object " << (i + 1) << "/" << num_type_b << "..." << std::endl;
                        }
                        auto start_time = high_resolution_clock::now();
                        Aws::S3::Model::GetObjectRequest request;
                        request.SetBucket(bucket);
                        request.SetKey(prefix + "/object_" + std::to_string(i % num_type_a));
                        auto outcome = s3_client_->GetObject(request);
                        auto end_time = high_resolution_clock::now();
                        double duration = duration_cast<microseconds>(end_time - start_time).count() / 1000.0; // ms
                        local_latencies.push_back(duration);
                        if (!outcome.IsSuccess()) {
                            std::lock_guard<std::mutex> lock(download_mutex);
                            std::cerr << "Error downloading object " << i << ": " 
                                      << outcome.GetError().GetMessage() << std::endl;
                        }
                    }
                    std::lock_guard<std::mutex> lock(download_mutex);
                    download_latencies.insert(download_latencies.end(), local_latencies.begin(), local_latencies.end());
                });
            }
            for (auto& th : threads) th.join();
        }
        auto download_t1 = high_resolution_clock::now();
        double download_wall_time_ms = duration_cast<microseconds>(download_t1 - download_t0).count() / 1000.0;
        std::cout << "[Download Phase Complete]" << std::endl;
        // Compute stats
        auto compute_stats = [](const std::vector<double>& latencies, size_t object_size, size_t count) {
            double min_latency = *std::min_element(latencies.begin(), latencies.end());
            double max_latency = *std::max_element(latencies.begin(), latencies.end());
            double avg_latency = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
            double total_time = std::accumulate(latencies.begin(), latencies.end(), 0.0); // ms
            double throughput = (object_size * count) / (1024.0 * 1024.0) / (total_time / 1000.0); // MB/s (sequential equivalent)
            return std::make_tuple(min_latency, max_latency, avg_latency, throughput, total_time);
        };
        auto [min_upload, max_upload, avg_upload, throughput_upload_seq, upload_duration] = compute_stats(upload_latencies, object_size, upload_latencies.size());
        auto [min_download, max_download, avg_download, throughput_download_seq, download_duration] = compute_stats(download_latencies, object_size, download_latencies.size());
        // Wall-clock throughput
        double upload_wall_throughput = (object_size * upload_latencies.size()) / (1024.0 * 1024.0) / (upload_wall_time_ms / 1000.0);
        double download_wall_throughput = (object_size * download_latencies.size()) / (1024.0 * 1024.0) / (download_wall_time_ms / 1000.0);
        // Prepare JSON
        json j;
        j["parameters"] = {
            {"object_size_kb", object_size / 1024},
            {"object_count", num_type_a},
            {"object_count_type_b", num_type_b},
            {"thread_count", thread_count},
            {"bucket_name", bucket_name}
        };
        j["write"] = {
            {"latencies_ms", upload_latencies},
            {"min_latency_ms", min_upload},
            {"max_latency_ms", max_upload},
            {"avg_latency_ms", avg_upload},
            {"throughput_mbps", throughput_upload_seq},
            {"wall_throughput_mbps", upload_wall_throughput},
            {"duration_ms", upload_duration},
            {"wall_duration_ms", upload_wall_time_ms}
        };
        j["read"] = {
            {"latencies_ms", download_latencies},
            {"min_latency_ms", min_download},
            {"max_latency_ms", max_download},
            {"avg_latency_ms", avg_download},
            {"throughput_mbps", throughput_download_seq},
            {"wall_throughput_mbps", download_wall_throughput},
            {"duration_ms", download_duration},
            {"wall_duration_ms", download_wall_time_ms}
        };
        std::ofstream ofs(output_file);
        ofs << j.dump(2);
        ofs.close();
        std::cout << "\nBenchmark results written to " << output_file << std::endl;
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
    std::string endpoint = skyrise::kR2Endpoint;
    std::string bucket = "benchmarking";
    std::string prefix = "benchmark";
    size_t num_type_a = 10000;
    size_t num_type_b = 100000;
    size_t object_size = 1024 * 1024;  // 1GB
    size_t num_runs = 5;
    size_t thread_count = 4;
    std::string output_file = "benchmark_results.json";
    app.add_option("--endpoint", endpoint, "R2 endpoint URL");
    app.add_option("--bucket", bucket, "R2 bucket name");
    app.add_option("--prefix", prefix, "Object key prefix");
    app.add_option("--type-a", num_type_a, "Number of Type A (upload) operations");
    app.add_option("--type-b", num_type_b, "Number of Type B (download) operations");
    app.add_option("--object-size", object_size, "Size of each object in bytes");
    app.add_option("--num-runs", num_runs, "Number of benchmark runs");
    app.add_option("--thread-count", thread_count, "Number of parallel threads");
    app.add_option("--output", output_file, "Output JSON file");
    CLI11_PARSE(app, argc, argv);
    R2Benchmark benchmark(endpoint);
    benchmark.RunBenchmark(bucket, prefix, num_type_a, num_type_b, object_size, num_runs, output_file, bucket, thread_count);
    return 0;
} 