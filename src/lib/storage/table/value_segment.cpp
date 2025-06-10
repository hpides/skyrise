/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "value_segment.hpp"

namespace skyrise {

template <typename T>
ValueSegment<T>::ValueSegment(bool nullable, ChunkOffset capacity) : BaseValueSegment(DataTypeFromType<T>()) {
  values_.reserve(capacity);
  if (nullable) {
    null_values_ = std::vector<bool>();
    null_values_->reserve(capacity);
  }
}

template <typename T>
ValueSegment<T>::ValueSegment(std::vector<T>&& values)
    : BaseValueSegment(DataTypeFromType<T>()), values_(std::move(values)) {}

template <typename T>
ValueSegment<T>::ValueSegment(std::vector<T>&& values, std::vector<bool>&& null_values)
    : BaseValueSegment(DataTypeFromType<T>()), values_(std::move(values)), null_values_(std::move(null_values)) {}

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  // Segment supports NULL values and value is NULL
  if (IsNullable() && (*null_values_)[chunk_offset]) {
    return kNullValue;
  }

  return values_.at(chunk_offset);
}

template <typename T>
bool ValueSegment<T>::IsNull(const ChunkOffset chunk_offset) const {
  return IsNullable() && (*null_values_)[chunk_offset];
}

template <typename T>
T ValueSegment<T>::Get(ChunkOffset chunk_offset) const {
  return values_.at(chunk_offset);
}

template <typename T>
void ValueSegment<T>::Append(const AllTypeVariant& val) {
  const bool is_null = VariantIsNull(val);

  if (IsNullable()) {
    (*null_values_).push_back(is_null);
    values_.push_back(is_null ? T{} : std::get<T>(val));
    return;
  }

  values_.push_back(std::get<T>(val));
}

template <typename T>
const std::vector<T>& ValueSegment<T>::Values() const {
  return values_;
}

template <typename T>
std::vector<T>& ValueSegment<T>::Values() {
  return values_;
}

template <typename T>
bool ValueSegment<T>::IsNullable() const {
  return null_values_.has_value();
}

template <typename T>
const std::vector<bool>& ValueSegment<T>::NullValues() const {
  return *null_values_;
}

template <typename T>
void ValueSegment<T>::SetNullValue(ChunkOffset chunk_offset) {
  const std::lock_guard<std::mutex> lock{null_value_modification_mutex_};
  (*null_values_)[chunk_offset] = true;
}

template <typename T>
ChunkOffset ValueSegment<T>::Size() const {
  return static_cast<ChunkOffset>(values_.size());
}

template <typename T>
void ValueSegment<T>::Resize(size_t size) {
  values_.resize(size);
  if (IsNullable()) {
    const std::lock_guard<std::mutex> lock{null_value_modification_mutex_};
    null_values_->resize(size);
  }
}

template <typename T>
size_t ValueSegment<T>::MemoryUsage() const {
  size_t null_value_vector_size{0};
  if (null_values_) {
    null_value_vector_size = null_values_->capacity() / CHAR_BIT;
  }

  const auto common_elements_size = sizeof(*this) + null_value_vector_size;

  if constexpr (std::is_same_v<T, std::string>) {
    return common_elements_size + StringVectorMemoryUsage(values_);
  }

  return common_elements_size + (values_.capacity() * sizeof(T));
}

template class ValueSegment<int32_t>;
template class ValueSegment<int64_t>;
template class ValueSegment<float>;
template class ValueSegment<double>;
template class ValueSegment<std::string>;

}  // namespace skyrise
