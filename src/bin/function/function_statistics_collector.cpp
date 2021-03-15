#include "function_statistics_collector.hpp"

#include "statistics/manifest_writer.hpp"
#include "statistics/statistics_collector.hpp"
#include "storage/backend/storage_s3.hpp"

namespace skyrise {

bool FunctionStatisticsCollector::PayloadIsValid(const Aws::Utils::Json::JsonView& request) {
  return request.KeyExists("manifest_bucket") && request.KeyExists("manifest_object") &&
         request.KeyExists("source_bucket") && request.KeyExists("source_objects");
}

aws::lambda_runtime::invocation_response FunctionStatisticsCollector::OnHandleRequest(
    const Aws::Utils::Json::JsonView& request) const {
  if (!PayloadIsValid(request)) {
    return aws::lambda_runtime::invocation_response::failure("You provided invalid arguments.", kInvalidArguments);
  }

  std::string manifest_bucket = request.GetString("manifest_bucket");
  std::string manifest_object = request.GetString("manifest_object");
  std::string source_bucket = request.GetString("source_bucket");
  std::vector<std::string> objects;
  auto source_objects = request.GetArray("source_objects");
  objects.reserve(source_objects.GetLength());
  for (size_t i = 0; i < source_objects.GetLength(); i++) {
    objects.emplace_back(source_objects.GetItem(i).AsString());
  }

  auto client = std::make_shared<Aws::S3::S3Client>();
  auto manifest_storage = std::make_shared<S3Storage>(client, manifest_bucket);
  auto source_storage = std::make_shared<S3Storage>(client, source_bucket);
  ManifestWriter writer(manifest_storage->OpenForWriting(manifest_object));

  auto return_error = [&writer, &manifest_storage, &manifest_object](const std::string& message,
                                                                     const std::string& type) {
    writer.Close();
    manifest_storage->Delete(manifest_object);
    return aws::lambda_runtime::invocation_response::failure(message, type);
  };

  // Iterate over passed objects and write their statistics into one manifest file.
  for (const auto& object_identifier : objects) {
    // Check if object is available.
    ObjectStatus status = source_storage->GetStatus(object_identifier);
    if (status.GetError()) {
      return return_error("Could not get status of object with identifier " + object_identifier, kObjectNotAccessible);
    }

    try {
      // Collect statistics for object.
      StatisticsCollector collector(source_storage, status);
      ObjectStatistics statistics = collector.GetAllStatistics();
      // Write statistics into assigned output manifest object.
      if (!writer.WritePartition(statistics)) {
        std::stringstream message;
        message << "Could not write partition data to manifest for object with identifier " << object_identifier
                << ". An StorageError occured: " << writer.GetError().GetMessage() << " ("
                << magic_enum::enum_name(writer.GetError().GetType()) << ")";
        return return_error(message.str(), kIoError);
      }
    } catch (const orc::ParseError& err) {
      return return_error("Could not parse object with identifier " + object_identifier, kParsingError);
    } catch (const std::logic_error& err) {
      return return_error("Unknown error with object with identifier " + object_identifier, kLogicError);
    }
  }

  writer.Close();

  return aws::lambda_runtime::invocation_response::success(
      R"({"message": "Statistics collection and manifest writing was successful."})", "application/json");
}

}  // namespace skyrise

int main() {
  skyrise::FunctionStatisticsCollector statistics_collector;
  statistics_collector.HandleRequest();

  return 0;
}
