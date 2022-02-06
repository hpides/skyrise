#pragma once

#include <memory>
#include <string>

#include <aws/core/utils/json/JsonSerializer.h>

#include "operator/abstract_operator.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "types.hpp"

namespace skyrise {

using StorageFactory = std::function<std::shared_ptr<Storage>(const std::string& storage_identifier)>;

/**
 * AbstractOperatorProxy is the base class for all proxy operators. A proxy operator holds all information necessary
 * to instantiate a corresponding operator. Its purpose is to encapsulate serialization and deserialization logic.
 */
class AbstractOperatorProxy : public std::enable_shared_from_this<AbstractOperatorProxy>, private Noncopyable {
 public:
  AbstractOperatorProxy(const OperatorType type, std::shared_ptr<AbstractOperatorProxy> left = nullptr,
                        std::shared_ptr<AbstractOperatorProxy> right = nullptr);
  virtual ~AbstractOperatorProxy() = default;

  OperatorType Type() const;
  virtual const std::string& Name() const = 0;
  virtual std::string Description(DescriptionMode description_mode = DescriptionMode::kSingleLine) const;

  std::shared_ptr<AbstractOperatorProxy> GetLeftInput() const;
  std::shared_ptr<AbstractOperatorProxy> GetRightInput() const;

  void SetLeftInput(std::shared_ptr<AbstractOperatorProxy> left_input);
  void SetRightInput(std::shared_ptr<AbstractOperatorProxy> right_input);

  virtual Aws::Utils::Json::JsonValue ToJson() const;

  std::shared_ptr<AbstractOperator> GetOrCreateOperatorInstance();

  std::string GetIdentity() const;

 protected:
  virtual std::shared_ptr<AbstractOperator> CreateOperatorInstance() = 0;

  const OperatorType type_;

  // Shared pointers to input operator proxies. If there is no input operator it has nullptr as a value.
  std::shared_ptr<AbstractOperatorProxy> left_input_;
  std::shared_ptr<AbstractOperatorProxy> right_input_;

  // An instance of the corresponding operator is cached to avoid multiple instantiations of the same operator.
  std::shared_ptr<AbstractOperator> operator_instance_;
};

}  // namespace skyrise
