#include "function_manifest_merger.hpp"

#include <aws/lambda-runtime/runtime.h>

namespace skyrise {

bool FunctionManifestMerger::CheckPayload(const Aws::Utils::Json::JsonView& request) {
  return request.KeyExists("manifest_bucket") && request.KeyExists("manifest_source_objects") &&
         request.KeyExists("manifest_target_object");
}

aws::lambda_runtime::invocation_response FunctionManifestMerger::OnHandleRequest(
    const Aws::Utils::Json::JsonView& request) const {
  if (!CheckPayload(request)) {
    return aws::lambda_runtime::invocation_response::failure("Invalid arguments provided.", "text/plain");
  }

  const std::string manifest_bucket = request.GetString("manifest_bucket");
  const std::string manifest_target_object = request.GetString("manifest_target_object");
  const auto object_array = request.GetArray("manifest_source_objects");

  std::vector<std::string> objects;
  objects.reserve(object_array.GetLength());
  for (size_t i = 0; i < object_array.GetLength(); i++) {
    objects.emplace_back(object_array.GetItem(i).AsString());
  }

  const auto client = std::make_shared<Aws::S3::S3Client>();
  const auto manifest_storage = std::make_shared<S3Storage>(client, manifest_bucket);

  ManifestMerger merger(manifest_storage);
  if (merger.Merge(objects, manifest_target_object)) {
    return aws::lambda_runtime::invocation_response::success("Successful: Manifest was written.", "text/plain");
  } else {
    return aws::lambda_runtime::invocation_response::failure("Error: Manifest merge failed", "text/plain");
  }
}

}  // namespace skyrise

int main() {
  const skyrise::FunctionManifestMerger manifest_merger;
  manifest_merger.HandleRequest();

  return 0;
}
