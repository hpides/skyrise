#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <aws/core/utils/json/JsonSerializer.h>

#include "compiler/abstract_plan_node.hpp"
#include "operator/abstract_operator.hpp"
#include "types.hpp"

namespace skyrise {

inline const std::string kJsonKeyOperatorType = "operator_type";

/**
 * AbstractOperatorProxy is the base class for all operator proxies. An operator proxy
 *  - can reference other operator proxies as its left and right input.
 *  - can instantiate a corresponding operator, and thus encapsulates all information necessary for execution.
 *  - provides serialization and deserialization logic.
 *
 * Physical Query Plan (PQP)
 *  Operator proxies can be linked together in tree structures since they are plan nodes. A tree of operator proxies
 *  describing a query execution plan is called a physical query plan (PQP).
 *  Thanks to the (de)serialization logic of operator proxies, PQPs or parts of them can be transferred across network
 *  boundaries and moved to, for example, cloud function workers.
 */
class AbstractOperatorProxy : public AbstractPlanNode<AbstractOperatorProxy> {
 public:
  explicit AbstractOperatorProxy(const OperatorType type);

  OperatorType Type() const;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * @return an identity string unique to this operator proxy.
   * For details, see the comment about 'Operator Identity Concept' down below.
   */
  std::string Identity() const;
  void PrefixIdentity(const std::string& prefix);
  void SetIdentity(const std::string& identity);

  /**
   * Optimization-relevant attributes
   */
  size_t InputObjectsCount() const;
  virtual size_t OutputObjectsCount() const;
  virtual size_t OutputColumnsCount() const;
  virtual bool IsPipelineBreaker() const = 0;

  /**
   * Recursively copies the input operator proxies and
   * @return a new instance of this operator proxy with the same configuration. Deduplication of plans will be
   *          preserved. See lqp_translator.cpp for more info.
   */
  std::shared_ptr<AbstractOperatorProxy> DeepCopy() const;

  /**
   * Implements AbstractOperatorProxy::DeepCopy and uses
   * @param copied_proxies to preserve deduplication for operator plans. See lqp_translator.cpp for more info.
   */
  std::shared_ptr<AbstractOperatorProxy> DeepCopy(
      std::unordered_map<const AbstractOperatorProxy*, std::shared_ptr<AbstractOperatorProxy>>& copied_proxies) const;

  /**
   * @return a hash value incorporating the data fields of the operator proxy and all of its inputs.
   *
   * Please note that hash conflicts are possible. This means that two PQPs can have the same hash although being
   * structurally different.
   */
  size_t Hash() const;

  /**
   * This function recursively creates an operator tree capable of processing actual data, if not already done.
   * @return a shared pointer to the root operator.
   * @pre The input operator proxies must be set or bound before calling this function.
   */
  std::shared_ptr<AbstractOperator> GetOrCreateOperatorInstance();

  /**
   * Serializes proxy attributes, such as operator type, and proxy inputs involving operator identities.
   * This function is intended be called from the overriding function via AbstractOperatorProxy::ToJson.
   */
  virtual Aws::Utils::Json::JsonValue ToJson() const;

  /**
   * Binds input proxies from @param identity_to_operator_proxies according to the operator identities attributes
   * resulting from the deserialization.
   * @pre Proxy instance was created as a result of deserialization. Input proxies are unset, but specified with
   *      operator identities.
   * @pre The map @param identity_to_operator_proxies includes the operator identity keys specified by this proxy.
   */
  void BindInputs(
      const std::unordered_map<std::string, std::shared_ptr<AbstractOperatorProxy>>& identity_to_operator_proxies);

 protected:
  OperatorType type_;  // mutable to allow for changes during optimization

  virtual std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const = 0;

  /**
   * Override to hash data fields in derived types. We do not need to take care of the input nodes here since they are
   * already handled by the calling methods.
   */
  virtual size_t ShallowHash() const = 0;

  /**
   * Creates and returns a tree of corresponding operators using recursion, starting from top to bottom.
   * @pre All operator proxies must have bound input operator proxies.
   */
  virtual std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() = 0;

  /**
   * Sets comment and operator identity attributes, including identity placeholders for the left and right inputs,
   * if specified in the given @param json.
   */
  void SetAttributesFromJson(const Aws::Utils::Json::JsonView& json);

  // An instance of the corresponding operator is cached to avoid multiple instantiations of the same operator.
  std::shared_ptr<AbstractOperator> operator_instance_;

 private:
  /**
   * Operator Identity Concept:
   *  To keep track of operator runtime behavior in logs and metrics, for example, operators are assigned
   *  unique identity strings for referencing purposes.
   *  Since operator proxies instantiate operators, they must manage operator identities. By default, a unique operator
   *  identity is generated from the operator proxy's memory address and the operator's name. However, it can be
   *  manipulated further using the ::PrefixIdentity and ::SetIdentity functions.
   *
   *  In addition to referencing purposes during runtime, operator identity strings are also used in the
   *  (de)serialization process of PQP (sub)plans:
   *   - During serialization, a tree of operator proxies is transformed into a flat list, so that deeply nested
   *     structures and operator duplication can be prevented in the serialization output. For this purpose, the inputs
   *     of operator proxies are modeled with the corresponding identity strings as placeholders.
   *   - When deserializing, all operator proxies are instantiated one-by-one, without linking them to their input
   *     operator proxies. The tree structure is recovered afterwards in a binding process using operator identity
   *     attributes.
   */

  // Mutable because the default identity cannot be set from the constructor. Instead, it is set in the ::Identity
  // function, which should stay const.
  mutable std::string identity_;

  // Used during (de)serialization, cf. AbstractOperatorProxy::ToJson and AbstractOperatorProxy::SetAttributesFromJson.
  std::string left_input_identity_;
  std::string right_input_identity_;
};

std::ostream& operator<<(std::ostream& stream, const AbstractOperatorProxy& root_operator_proxy);

}  // namespace skyrise
