#pragma once

#include <cstdlib>
#include <functional>

namespace skyrise {

class DataPushProcessor {
 public:
  virtual ~DataPushProcessor() = default;
  virtual bool Process(const char* data, size_t length) = 0;
  virtual bool Finish() = 0;
  virtual void SetOutput(std::function<void(const char* data, size_t length)> callback);

 protected:
  std::function<void(const char* data, size_t length)> callback_;
};

class DataPullProcessor {
 public:
  virtual ~DataPullProcessor() = default;
  virtual bool Process() = 0;
  virtual void SetOutput(std::function<void(const char* data, size_t length)> callback);
  virtual void SetInput(std::function<void(const char** data, size_t* length)> callback);

 protected:
  std::function<void(const char* data, size_t length)> output_;
  std::function<void(const char** data, size_t* length)> input_;
};

}  // namespace skyrise
