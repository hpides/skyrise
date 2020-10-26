#include <aws/core/Aws.h>

namespace skyrise {

// Merge a nested Aws::Utils::Array of Aws::Utils::Array<T> instances into a flat Aws::Utils::Array<T>
template <typename T>
Aws::Utils::Array<T> FlattenArrays(Aws::Utils::Array<Aws::Utils::Array<T>>* arrays) {
  Aws::Vector<Aws::Utils::Array<T>*> array_pointers(arrays->GetLength());

  for (size_t i = 0; i < arrays->GetLength(); ++i) {
    array_pointers[i] = &(*arrays)[i];
  }

  return Aws::Utils::Array<T>(std::move(array_pointers));
}

}  // namespace skyrise
