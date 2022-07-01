#include "processor.hpp"

namespace skyrise {

void DataPushProcessor::SetOutput(std::function<void(const char* data, size_t length)> callback) {
  callback_ = std::move(callback);
}

void DataPullProcessor::SetOutput(std::function<void(const char* data, size_t length)> callback) {
  output_ = std::move(callback);
}
void DataPullProcessor::SetInput(std::function<void(const char** data, size_t* length)> callback) {
  input_ = std::move(callback);
}

}  // namespace skyrise
