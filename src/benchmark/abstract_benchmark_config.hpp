#pragma once

#include <string>

#include <CLI/CLI.hpp>

namespace skyrise {

class AbstractBenchmarkConfig {
 public:
  AbstractBenchmarkConfig(const std::string& name, const std::string& description)
      : name_(name), description_(description) {}

  virtual ~AbstractBenchmarkConfig() = default;

  virtual void AddCliOptions(CLI::App& app) = 0;

  const std::string& GetName() const { return name_; }
  const std::string& GetDescription() const { return description_; }

 private:
  std::string name_;
  std::string description_;
};

}  // namespace skyrise 