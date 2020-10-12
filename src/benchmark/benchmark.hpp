#include <aws/core/Aws.h>

namespace Skyrise {

class Benchmark {
  virtual Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run() = 0;
};

}  // namespace Skyrise
