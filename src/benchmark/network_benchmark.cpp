#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/lambda-runtime/runtime.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <algorithm>
#include <fstream>
#include <iterator>
#include <regex>
#include <tuple>
#include <vector>

// globals
namespace lambda_rt = aws::lambda_runtime;
namespace jutils = Aws::Utils::Json;
char const TAG[] = "S3_BENCHMARK";

// forward declarations
const Aws::Client::ClientConfiguration get_client_config(const bool is_local,
                                                         const std::optional<const Aws::String>& minio_endpoint);
const Aws::S3::S3Client get_client(const bool is_local, const std::optional<const Aws::String>& minio_endpoint);

/**
 * A simple converter for converting a value to a string (ts = to_string)
 *
 * @param val: The value that should be converted to a string
 * @return returns val, represented as a string
 */
auto to_str = [](auto val) -> Aws::String { return Aws::Utils::StringUtils::to_string(val); };

/**
 * Converts string to string of upper chars in place.
 *
 * @param s: The string that should be converted.
 */
auto string_to_upper = [](Aws::String& s) { std::transform(s.begin(), s.end(), s.begin(), ::toupper); };

/**
 * Protects an invocation response from being called with a nullptr. Will call the invocation response with provided
 * arguments, if not nullpointer. Else will call it with a custom error message.
 *
 * @param resp The invocation response
 * @param msg_msg The message to pass to the response
 * @param msg_type The message type to pass to the response
 * @return returns an invocation response
 */
lambda_rt::invocation_response check_nullptr_response(lambda_rt::invocation_response (*resp)(const Aws::String& a,
                                                                                             const Aws::String& b),
                                                      const Aws::String& msg, const Aws::String msg_type) {
  if (msg.empty()) {
    return resp("tried to call response will nullptr. Original error_type was " + msg_type, "nullptr");
  }
  return resp(msg, msg_type);
}

/**
 * join a vector to a string.
 *
 * @param v: The vector.
 * @param s: The string that is placed between the elements in v.
 * @return Returns the vector represented as a string
 */
template <typename T>
Aws::String join_vector(const Aws::Vector<T>& v, const Aws::String& s) {
  std::ostringstream oss;
  for (auto it = v.begin(); it != v.end() - 1; ++it) {
    oss << *it << s;
  }
  oss << v.back();
  return oss.str();
}

/**
 * join a vector to a string, with join string ",".
 *
 * @param v: The vector.
 * @return Returns the vector represented as a string.
 */
template <typename T>
Aws::String join_vector(const Aws::Vector<T>& v) {
  return join_vector(v, ",");
}

/**
 * Check if an element is in a vector.
 *
 * @param ele: The element that is supposed to be in the vector.
 * @param vec: The vector that is supposed to contain ele.
 * @return true if vec contains ele, else false.
 */
template <typename T>
bool in_vector(const T& ele, const Aws::Vector<T>& vec) {
  return std::find(vec.begin(), vec.end(), ele) != vec.end();
}

/**
 * Converts the response to a HTTP request to a string.
 * @param resp: The response of the request.
 * @return Gets the error message of the response, if request failed, else returns "No error".
 */
template <typename R, typename E>
Aws::String response_to_err_msg(Aws::Utils::Outcome<R, E>& resp) {
  return resp.IsSuccess() ? "No error" : resp.GetError().GetMessage();
}

/***
 * Retrieves the object from storage and determines its size in bytes.
 *
 * @param client: The S3 or Minio client
 * @param bucket: The bucket in which the object resides
 * @param key: The key of the object
 * @return the size of the retrieved object
 */
const std::optional<int> key_to_size(const Aws::S3::S3Client& client, const Aws::String bucket, const Aws::String key) {
  AWS_LOGSTREAM_INFO(TAG, "Determining the size of the object at key " + key);
  Aws::S3::Model::GetObjectRequest get_obj_req;
  get_obj_req.SetBucket(bucket);
  get_obj_req.SetKey(key);
  const auto outcome = client.GetObject(get_obj_req);
  if (!outcome.IsSuccess()) {
    return std::nullopt;
  }
  const int size = outcome.GetResult().GetContentLength();
  AWS_LOGSTREAM_INFO(TAG, "Size of object at " + key + " is " + to_str(size));
  return size;
}

/**
 * Counts how long it takes to GET on object from/to the given destination.
 *
 * @param client: The S3Client to use.
 * @param bucket: The bucket that contains the object to interact with.
 * @param key: The key of the object,
 * @param test_duration_sec: The time in seconds that the benchmark witll run.
 * @param is_local: If true, runs test against local Minio instance, else against S3.
 * @param is_dry_run: If true this function will not return the latency of the requests, but a list of errors that
 * occurred when running requests against the storage.
 * @param request_id_latency_success_out: A list of tuples, that record the id, latency and success status of each
 * request
 * @param request_id_error_out: A list of tuples, that record the id and error. Used if is_dry_run is true
 * @return an optional error, if there was any
 */
const std::optional<Aws::String> s3_benchmark_generic_get(
    const Aws::S3::S3Client& client, Aws::String bucket, Aws::String key, int test_duration_sec, const bool is_dry_run,
    Aws::List<std::tuple<int, int, bool>>& request_id_latency_success_out,
    Aws::List<std::tuple<int, Aws::String>>& request_id_error_out) {
  // execute benchmark
  AWS_LOGSTREAM_INFO(TAG, "Executing GET latency benchmark");
  auto bench_duration = std::chrono::seconds{test_duration_sec};
  auto now = std::chrono::steady_clock::now;
  auto stop_time = now() + bench_duration;
  int id = 0;

  // delete begin
  Aws::Client::ClientConfiguration config;
  config.endpointOverride = "192.168.97.2:9000";
  config.verifySSL = false;
  config.scheme = Aws::Http::Scheme::HTTP;
  auto credentials_provider = Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>(TAG);
  auto tmp_client = Aws::S3::S3Client(credentials_provider, config,
                                      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always, false);

  for (int i = 0; i < 3; ++i) {
    Aws::S3::Model::GetObjectRequest get_obj_req;
    get_obj_req.SetBucket(bucket);
    get_obj_req.SetKey(key);
    auto resp = client.GetObject(get_obj_req);
    if (resp.IsSuccess()) {
      request_id_latency_success_out.push_back(std::tuple<int, int, bool>{++id, -1, true});
    }
  }
  return std::nullopt;

  // delete end

  while (true) {
    // declare requests
    AWS_LOGSTREAM_INFO(TAG, "Bucket: " + bucket + " key: " + key);  // delete
    Aws::S3::Model::GetObjectRequest get_obj_req;
    get_obj_req.SetBucket(bucket);
    get_obj_req.SetKey(key);
    auto begin_req_time = now();
    AWS_LOGSTREAM_INFO(TAG, "Executing Get request in get benchmark");  // delete
    auto resp = tmp_client.GetObject(get_obj_req);                      // delete & change back to client
    AWS_LOGSTREAM_INFO(TAG, "Hi there");                                // delete
    auto stop_req_time = now();

    if (now() >= stop_time) {
      // break if benchmark runs out of time
      AWS_LOGSTREAM_INFO(TAG, "benchmark ending");  // delete
      request_id_latency_success_out.push_back(std::tuple<int, int, bool>{++id, -1, false});
      break;
    }
    if (!is_dry_run && resp.IsSuccess()) {
      // successfull request and not dryrun
      AWS_LOGSTREAM_INFO(TAG, "add successfull request");  // delete
      auto request_time = std::chrono::duration_cast<std::chrono::microseconds>(stop_req_time - begin_req_time);
      request_id_latency_success_out.push_back(std::tuple<int, int, bool>{++id, request_time.count(), true});
    } else {
      // not dryrun and failed request || dryrun
      AWS_LOGSTREAM_INFO(TAG, "add failed request");  // delete
      request_id_error_out.push_back(std::tuple<int, Aws::String>{++id, response_to_err_msg(resp)});
    }
  };
  AWS_LOGSTREAM_INFO(TAG, "return nullopt");  // delete
  return std::nullopt;
}

/**
 * Counts how long it takes to PUT on object from/to the given destination.
 *
 * @param client: The S3Client to use.
 * @param bucket: The bucket that contains the object to interact with.
 * @param key: The key of the object,
 * @param test_duration_sec: The time in seconds that the benchmark will run.
 * @param is_local: If true, runs test against local Minio instance, else against S3.
 * @param is_dry_run: If true this function will not return the latency of the requests, but a list of errors that
 * occurred when running requests against the storage.
 * @param request_id_latency_success_out: A list of tuples, that record the id, latency and success status of each
 * request
 * @param request_id_error_out: A list of tuples, that record the id and error. Used if is_dry_run is true
 * @return returns an optional error, if there was any
 */
const std::optional<Aws::String> s3_benchmark_generic_put(
    const Aws::S3::S3Client& client, Aws::String bucket, Aws::String key, int test_duration_sec, const bool is_dry_run,
    Aws::List<std::tuple<int, int, bool>>& request_id_latency_success_out,
    Aws::List<std::tuple<int, Aws::String>>& request_id_error_out) {
  // get request
  Aws::S3::Model::GetObjectRequest get_obj_req;
  get_obj_req.SetBucket(bucket);
  get_obj_req.SetKey(key);
  AWS_LOGSTREAM_INFO(TAG, "Attempting to download file with key " + key);
  auto get_object_outcome = client.GetObject(get_obj_req);
  if (!get_object_outcome.IsSuccess()) {
    return "Error in GET request to bucket " + bucket + " and key " + key + ". Error was " +
           get_object_outcome.GetError().GetMessage() + "\n";
  }

  // Get an Aws::IOStream reference to the retrieved file
  AWS_LOGSTREAM_INFO(TAG, "Configuring PUT request");
  Aws::String content_tye = get_object_outcome.GetResult().GetContentType();
  Aws::IOStream& retrieved_file = get_object_outcome.GetResultWithOwnership().GetBody();

  // execute benchmark
  AWS_LOGSTREAM_INFO(TAG, "Executing PUT latency benchmark");
  auto bench_duration = std::chrono::seconds{test_duration_sec};
  auto now = std::chrono::steady_clock::now;
  auto stop_time = now() + bench_duration;
  int id = 0;
  while (true) {
    // configure put request
    Aws::S3::Model::PutObjectRequest put_obj_req;
    put_obj_req.SetBucket(bucket);
    put_obj_req.SetKey(key);
    put_obj_req.SetBody(Aws::MakeShared<Aws::IOStream>(TAG, retrieved_file.rdbuf()));

    // reset buffer
    retrieved_file.seekg(0, retrieved_file.beg);

    // measure put request time
    auto begin_req_time = now();
    auto resp = client.PutObject(put_obj_req);
    auto stop_req_time = now();
    if (now() >= stop_time) {
      // break if benchmark runs out of time
      request_id_latency_success_out.push_back(std::tuple<int, int, bool>{++id, -1, false});
      break;
    }

    Aws::String request_id = to_str(++id);
    if (!is_dry_run) {
      if (resp.IsSuccess()) {
        // successfull request and not dryrun
        auto request_duration = std::chrono::duration_cast<std::chrono::microseconds>(stop_req_time - begin_req_time);
        request_id_latency_success_out.push_back(std::tuple<int, int, bool>{++id, request_duration.count(), true});
      } else {
        // failed request and not dryrun
        auto request_duration = std::chrono::duration_cast<std::chrono::microseconds>(stop_req_time - begin_req_time);
        request_id_latency_success_out.push_back(std::tuple<int, int, bool>{++id, request_duration.count(), false});
      }
    } else {
      // dryrun, successfull or failed request
      request_id_error_out.push_back(std::tuple<int, Aws::String>{++id, response_to_err_msg(resp)});
    }
  };
  return std::nullopt;
}

/**
 * A generic benchmark function
 *
 * @param client: The S3Client to use.
 * @param bucket: The bucket that contains the object to interact with.
 * @param key: The key of the object,
 * @param test_duration_sec: The time in seconds that the benchmark will run.
 * @param is_local: If true, runs test against local Minio instance, else against S3.
 * @param is_dry_run: If true this function will not return the latency of the requests, but a list of errors that
 * occurred when running requests against the storage.
 * @param lambda_size_mb: The size of the Lambda instance in MB.
 * @param storage_name: The name of the underlying storage (e.g. S3).
 * @param request_type: The HTTP request
 * @param read_mitigation: The read mitigation time. -1 if none is set.
 * @param write_mitigation: The write mitigation time. -1 if none is set.
 * @param nr_concurrent_requests: The number of concurrent requests
 * @param double_write: Determines if double write is used
 * @param csv_has_header: Determines if the resulting csv string has a header row
 * @return returns a csv string. Each line is representing one request.
 */
const Aws::String s3_benchmark_generic(const Aws::S3::S3Client& client, Aws::String bucket, Aws::String key,
                                       int test_duration_sec, const bool is_dry_run, const int lambda_size_mb,
                                       const Aws::String storage_name, const Aws::String request_type,
                                       const int read_mitigation, const int write_mitigation,
                                       const int nr_concurrent_requests, const bool double_write,
                                       const bool csv_has_header) {
  // get the current key size
  const std::optional<int> key_size = key_to_size(client, bucket, key);
  if (!key_size) {
    return "Could not calculate the requested key size. Bucket was " + bucket + ", key was " + key;
  }

  AWS_LOGSTREAM_INFO(TAG, "Object size is " + to_str(key_size.value()));

  // record the results of the requests (request_id, request_latency, request_success)
  Aws::List<std::tuple<int, int, bool>> request_id_latency_success;
  // record errors
  Aws::List<std::tuple<int, Aws::String>> request_id_error;

  Aws::S3::Model::GetObjectRequest get_obj_req;
  get_obj_req.SetBucket(bucket);
  get_obj_req.SetKey(key);

  // execute benchmark
  std::optional<Aws::String> benchmark_error;
  if (request_type == "GET") {
    benchmark_error = s3_benchmark_generic_get(client, bucket, key, test_duration_sec, is_dry_run,
                                               request_id_latency_success, request_id_error);
  } else if (request_type == "PUT") {
    benchmark_error = s3_benchmark_generic_put(client, bucket, key, test_duration_sec, is_dry_run,
                                               request_id_latency_success, request_id_error);
  } else {
    return "Invalid request type " + request_type;
  }
  if (benchmark_error) {
    return benchmark_error.value();
  }

  AWS_LOGSTREAM_INFO(TAG, "benchmark finished without error");  // delete

  // create resulting csv depending on the parameters is_dry_run and csv_has_header
  Aws::String csv_string =
      "lambda_size_MB,storage,request_type,object_size,write_mitigation, "
      "read_mitigation,double_write,date,experiment_duration_sec,nr_of_concurrent_requests,request_id,";
  csv_string += is_dry_run ? "request_error\n" : "request_latency,request_success\n";
  csv_string = csv_has_header ? csv_string : "";
  Aws::String key_size_str = to_str(key_size.value());
  AWS_LOGSTREAM_INFO(TAG, "The size of the requested object is " + key_size_str + "in bytes");

  // converter
  auto get_date = []() -> Aws::String {
    return to_str(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
  };

  AWS_LOGSTREAM_INFO(TAG, "After get_date");  // delete

  // the info that never changes
  Aws::String csv_basic_info = to_str(lambda_size_mb) + "," + storage_name + "," + request_type + "," + key_size_str +
                               "," + to_str(write_mitigation) + "," + to_str(read_mitigation) + "," +
                               to_str(double_write) + "," + get_date() + "," + to_str(test_duration_sec) + "," +
                               to_str(nr_concurrent_requests);

  AWS_LOGSTREAM_INFO(TAG, "After basic_info");  // delete

  if (!is_dry_run) {
    AWS_LOGSTREAM_INFO(TAG, "in !is_dry_run");  // delete
    for (const auto& [id, latency, success] : request_id_latency_success) {
      csv_string += csv_basic_info + "," + to_str(id) + "," + to_str(latency) + "," + to_str(success) + "\n";
    }
  } else {
    for (const auto& [id, error] : request_id_error) {
      csv_string += csv_basic_info + "," + to_str(id) + "," + to_str(error) + "\n";
    }
  }

  AWS_LOGSTREAM_INFO(TAG, "return csv_string");  // delete
  return csv_string;
}

/**
 * Simple function checking for parameter values which are not implemented yet.
 *
 * @param storage_name: The name of the underlying storage. Should be Minio or S3.
 * @param read_mitigation: The read mitigation time. Disabled if -1.
 * @param write_mitgation: The write mitigation time. Disabled if -1.
 * @param double_write: Indicates if double write is enabled
 * @param nr_concurrent_requests: The number of concurrent requests. If 1, requests are synchronous.
 * @return An error message if there was a not implemented feature requested.
 */
std::optional<Aws::String> uses_not_implemented_features(const Aws::String& storage_name, const int read_mitigation,
                                                         const int write_mitigation, const bool double_write,
                                                         const int nr_concurrent_requests) {
  Aws::Vector<Aws::String> valid_storage_names{"S3", "MINIO"};
  if (!in_vector(storage_name, valid_storage_names)) {
    return "Invalid storage_name. Valid values are " + join_vector(valid_storage_names) + "Value was: " + storage_name;
  }
  if (read_mitigation != -1) {
    return "Read mitigation has to be disabled (-1). Value was: " + to_str(read_mitigation);
  }
  if (write_mitigation != -1) {
    return "Write mitigation has to be disabled (-1). Value was: " + to_str(write_mitigation);
  }
  if (double_write) {
    return "Double write has to be disabled (false)";
  }
  if (nr_concurrent_requests != 1) {
    return "No asynchronous requests supported. The number of concurrent requests has to be equal to 1. Value was: " +
           to_str(nr_concurrent_requests);
  }
  return std::nullopt;
}

/**
 * Checks the incoming request for errors and returns the first error it encounters, if any.
 *
 * @param req The invocation request.
 * @param other The other parameters are the keys of the values in the request
 * @return returns an optional object that contains a string if an error was found.
 */
std::optional<Aws::String> request_has_error(
    const jutils::JsonValue& json_payload, const Aws::String& duration_key, const Aws::String& is_local_key,
    const Aws::String& s3_bucket_key, const Aws::String& s3_key_key, const Aws::String& is_dry_run_key,
    const Aws::String& minio_endpoint_key, const Aws::String& lambda_size_key, const Aws::String& storage_name_key,
    const Aws::String& read_mitigation_key, const Aws::String& write_mitigation_key,
    const Aws::String& nr_concurrent_requests_key, const Aws::String& double_write_key,
    const Aws::String& csv_has_header_key) {
  // check if parsing payload was successfull
  if (!json_payload.WasParseSuccessful()) {
    return "Failed to parse input JSON";
  }

  // check default values.
  enum value_type { STR, INT, BOOL };
  auto valid_values = [](const Aws::List<Aws::String>& string_keys, const jutils::JsonView& v,
                         const value_type vt) -> bool {
    for (Aws::String string_key : string_keys) {
      if (v.ValueExists(string_key)) {
        jutils::JsonView obj = v.GetObject(string_key);
        bool has_correct_type = vt == STR ? obj.IsString() : vt == INT ? obj.IsIntegerType() : obj.IsBool();
        if (!has_correct_type) {
          AWS_LOGSTREAM_INFO(TAG, "key " + string_key + " has incorrect type");  // delete
          return false;
        }
      } else {
        AWS_LOGSTREAM_INFO(TAG, "key " + string_key + " was not found in view");  // delete
        return false;
      }
    }
    return true;
  };

  jutils::JsonView v = json_payload.View();
  // delete begin
  Aws::String tmp;
  if (!valid_values(Aws::List<Aws::String>{s3_bucket_key, s3_key_key, storage_name_key}, v, STR)) {
    tmp = "first";
  }
  if (!valid_values(Aws::List<Aws::String>{duration_key, lambda_size_key, nr_concurrent_requests_key,
                                           read_mitigation_key, duration_key, write_mitigation_key},
                    v, INT)) {
    tmp += "second";
  }
  if (!valid_values(Aws::List<Aws::String>{is_local_key, is_dry_run_key, double_write_key, csv_has_header_key}, v,
                    BOOL)) {
    tmp += "third";
  }
  // delete end
  if (!valid_values(Aws::List<Aws::String>{s3_bucket_key, s3_key_key, storage_name_key}, v, STR) ||
      !valid_values(Aws::List<Aws::String>{duration_key, lambda_size_key, nr_concurrent_requests_key,
                                           read_mitigation_key, duration_key, write_mitigation_key},
                    v, INT) ||
      !valid_values(Aws::List<Aws::String>{is_local_key, is_dry_run_key, double_write_key, csv_has_header_key}, v,
                    BOOL)) {
    return tmp + "Missing input values for keys " + is_local_key + ", " + s3_bucket_key + ", " + duration_key + ", " +
           s3_key_key + ", " + is_dry_run_key + ", " + double_write_key + ", " + storage_name_key + ", " +
           lambda_size_key + ", " + nr_concurrent_requests_key + ", " + read_mitigation_key + ", " +
           csv_has_header_key + " or " + write_mitigation_key;
  }

  // specific values for local test
  if (v.GetBool(is_local_key)) {
    // if the test is local, you have to pass the minio_endpoint_key
    // the coresponding value can either be an empty string or a valid ip address
    if (!v.ValueExists(minio_endpoint_key) || !v.GetObject(minio_endpoint_key).IsString()) {
      return "Missing input value for key " + minio_endpoint_key +
             ". This key is necessary if you wish to run a benchmark against a local Minio instance.";
    }

    Aws::String rgx_str{"\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+"};
    std::regex rgx{rgx_str};
    Aws::String endpoint_string{v.GetString(minio_endpoint_key)};
    if (endpoint_string != "" && !std::regex_match(endpoint_string, rgx)) {
      return "Invalid value for key " + minio_endpoint_key + "Value passed was " + endpoint_string +
             ". This key is necessary if you wish to run a benchmark against a local minio instance. If pass this key "
             "with a blank value (empty string), then this application will use a default value. Endpoint is of "
             "format " +
             rgx_str + ", e.g. 192.168.97.2:9000.";
    }
  }
  return std::nullopt;
}

/**
 * Response handler that calls a latency or throughput benchmark.
 *
 * @param req: The invocation request.
 */
static lambda_rt::invocation_response s3_benchmark_invocation_handler(const lambda_rt::invocation_request& req) {
  // field names in json
  const Aws::String duration_key = "testDurationSec";
  const Aws::String is_local_key = "isLocal";
  const Aws::String s3_bucket_key = "s3Bucket";
  const Aws::String s3_key_key = "s3Key";
  const Aws::String request_type_key = "requestType";
  const Aws::String is_dry_run_key = "isDryRun";
  const Aws::String minio_endpoint_key = "minioEndpoint";
  const Aws::String lambda_size_key = "lambdaSize";
  const Aws::String storage_name_key = "storageName";
  const Aws::String read_mitigation_key = "readMitigation";
  const Aws::String write_mitigation_key = "writeMitigation";
  const Aws::String nr_concurrent_requests_key = "nrConcurrentRequests";
  const Aws::String double_write_key = "doubleWrite";
  const Aws::String csv_has_header_key = "csvHasHeader";

  jutils::JsonValue json_payload{req.payload};

  const std::optional<Aws::String> request_error =
      request_has_error(json_payload, duration_key, is_local_key, s3_bucket_key, s3_key_key, is_dry_run_key,
                        minio_endpoint_key, lambda_size_key, storage_name_key, read_mitigation_key,
                        write_mitigation_key, nr_concurrent_requests_key, double_write_key, csv_has_header_key);
  if (request_error) {
    return check_nullptr_response(lambda_rt::invocation_response::failure, request_error.value(), "invalidJSON");
  }

  const jutils::JsonView v = json_payload.View();
  const Aws::String bucket = v.GetString(s3_bucket_key);
  const Aws::String key = v.GetString(s3_key_key);
  Aws::String tmp = v.GetString(storage_name_key);
  string_to_upper(tmp);
  const Aws::String storage_name = tmp;
  const bool double_write = v.GetBool(double_write_key);
  const bool is_dry_run = v.GetBool(is_dry_run_key);
  const bool is_local = v.GetBool(is_local_key);
  const int lambda_size = v.GetInteger(lambda_size_key);
  const int nr_concurrent_requests = v.GetInteger(nr_concurrent_requests_key);
  const int read_mitigation = v.GetInteger(read_mitigation_key);
  const int test_duration_sec = v.GetInteger(duration_key);
  const int write_mitigation = v.GetInteger(write_mitigation_key);
  const Aws::String minio_endpoint_passed_value = v.GetString(minio_endpoint_key);
  const bool csv_had_header = v.GetBool(csv_has_header_key);
  const std::optional<Aws::String> minio_endpoint =
      minio_endpoint_passed_value == "" ? std::nullopt : std::optional<Aws::String>{minio_endpoint_passed_value};

  const std::optional<Aws::String> not_implemented_error = uses_not_implemented_features(
      storage_name, read_mitigation, write_mitigation, double_write, nr_concurrent_requests);
  if (not_implemented_error) {
    return check_nullptr_response(lambda_rt::invocation_response::failure, not_implemented_error.value(),
                                  "notImplementedError");
  }

  const Aws::S3::S3Client client = get_client(is_local, minio_endpoint);

  // generic benchmark
  Aws::String request_type = v.GetString(request_type_key);
  string_to_upper(request_type);
  return check_nullptr_response(
      lambda_rt::invocation_response::success,
      s3_benchmark_generic(client, bucket, key, test_duration_sec, is_dry_run, lambda_size, storage_name, request_type,
                           read_mitigation, write_mitigation, nr_concurrent_requests, double_write, csv_had_header),
      "text/plain");
}

/**
 * Get the console logger factory
 *
 * @return returns a shared pointer to a ConsoleLogSystem
 */
std::function<std::shared_ptr<Aws::Utils::Logging::LogSystemInterface>()> GetConsoleLoggerFactory() {
  return [] {
    return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_logger",
                                                                  Aws::Utils::Logging::LogLevel::Trace);
  };
}

/**
 * Get the client configuration used for S3 or a local Minio instance.
 *
 * @param is_local defines is this config is for an S3Client or a Minio Client.
 * @param minio_endpoint optional parameter that will only be used if the is_local is true. Will default to
 * 172.17.0.2:9000
 * @return returns a client configuration.
 */
const Aws::Client::ClientConfiguration get_client_config(bool is_local,
                                                         const std::optional<const Aws::String>& minio_endpoint) {
  Aws::Client::ClientConfiguration config;
  if (is_local) {
    // Minio
    config.endpointOverride = minio_endpoint.value_or("172.17.0.2:9000");
    config.verifySSL = false;
    config.scheme = Aws::Http::Scheme::HTTP;
    return config;
  }
  // S3
  config.region = Aws::Environment::GetEnv("AWS_REGION");
  config.caFile = "/etc/pki/tls/certs/ca-bundle.crt";
  return config;
}

/**
 * Get the client used for S3 or a local Minio instance.
 *
 * @param is_local defines is this config is for an S3Client or a Minio Client
 * @param minio_endpoint optional parameter that will only be used if the is_local is true. Will default to
 * 172.17.0.2:9000
 * @return returns a client configuration.
 */
const Aws::S3::S3Client get_client(const bool is_local, const std::optional<const Aws::String>& minio_endpoint) {
  Aws::String interact_with = (is_local) ? "Minio" : "S3";
  AWS_LOGSTREAM_INFO(TAG, "Creating client to interact with " + interact_with);
  Aws::Client::ClientConfiguration config = get_client_config(is_local, minio_endpoint);
  auto credentials_provider = Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>(TAG);
  if (is_local) {
    return Aws::S3::S3Client(credentials_provider, config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always,
                             false);
  }
  return Aws::S3::S3Client(credentials_provider, config);
}

int main() {
  // SDK
  Aws::SDKOptions options;
  options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
  options.loggingOptions.logger_create_fn = GetConsoleLoggerFactory();

  // API
  InitAPI(options);
  {
    auto handler_fn = [](lambda_rt::invocation_request const& req) { return s3_benchmark_invocation_handler(req); };
    lambda_rt::run_handler(handler_fn);
  }
  ShutdownAPI(options);
  return 0;
}