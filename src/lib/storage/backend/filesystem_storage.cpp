#include "filesystem_storage.hpp"

#include <filesystem>
#include <utility>

#if defined(__linux__) || defined(__APPLE__)
#include <cerrno>

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#else
#error "FilesystemStorage is not implemented on your platform."
#endif

namespace skyrise {

namespace {

StorageError ErrnoToStorageError() {
  // errno is set by `unlink` or `stat`
  switch (errno) {
    case EACCES:
    case EPERM:
    case EROFS:
      return StorageError(StorageErrorType::kPermissionDenied);
    case EBUSY:
      return StorageError(StorageErrorType::kTemporary);
    case EIO:
      return StorageError(StorageErrorType::kIOError);
    case ENOENT:
    case ENOTDIR:
      return StorageError(StorageErrorType::kNotFound);
    case ENAMETOOLONG:
    case ELOOP:
    case EOVERFLOW:
      return StorageError(StorageErrorType::kOperationNotSupported);
    case EFAULT:
    case ENOMEM:
      return StorageError(StorageErrorType::kInternalError);
    default:
      return StorageError(StorageErrorType::kUnknown);
  }
}

// This function wraps a call to `stat` and returns the result as an `ObjectStatus`. Optionally the first characters of
// the filepath can be hidden to simulate behaviour like in a chroot. Be aware that this is not a security feature and
// can easily be circumvented.
ObjectStatus GetFileStatus(const std::string& filename, size_t num_characters_hidden = 0) {
  struct stat buffer{};
  const int stat_result = stat(filename.c_str(), &buffer);
  const int64_t last_modified = buffer.st_mtime;

  // Did `stat` fail?
  if (stat_result == -1) {
    return ObjectStatus(ErrnoToStorageError());
  }

  // We do not get a proper hash at this point, so we will return the filename itself for now
  return {filename.substr(num_characters_hidden), last_modified, filename, static_cast<size_t>(buffer.st_size)};
}

}  // namespace

FilesystemWriter::FilesystemWriter(const std::string& filename) {
  std::filesystem::path path(filename);
  std::filesystem::create_directories(path.parent_path());
  out_.open(filename.c_str(), std::ios::out | std::ios::binary);
}

FilesystemWriter::~FilesystemWriter() {
  if (out_.is_open()) {
    out_.close();
  }
}

StorageError FilesystemWriter::Write(const char* data, size_t length) {
  auto pos_before_write = out_.tellp();
  if (pos_before_write == std::ofstream::pos_type(-1)) {
    return StorageError(StorageErrorType::kIOError);
  }

  out_.write(data, length);

  auto pos_after_write = out_.tellp();
  if (std::cmp_not_equal(pos_after_write - pos_before_write, length)) {
    return StorageError(StorageErrorType::kIOError);
  }
  return StorageError::Success();
}

StorageError FilesystemWriter::Close() {
  if (out_.is_open()) {
    out_.close();
  }
  return StorageError::Success();
}

StorageError FilesystemStorage::ListDirectoryRecursively(const std::string& directory_name, const std::string& prefix,
                                                         std::vector<ObjectStatus>* output_vector) {
  DIR* dir = opendir(directory_name.c_str());
  if (dir == nullptr) {
    return StorageError(StorageErrorType::kNotFound);
  }

  struct dirent* dir_entry{};
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  while ((dir_entry = readdir(dir)) != nullptr) {
    if (dir_entry->d_name[0] == '.') {
      continue;
    }
    const std::string filename = JoinPath(directory_name, dir_entry->d_name);
    if (!filename.starts_with(prefix)) {
      continue;
    }
    if (dir_entry->d_type == DT_REG) {
      output_vector->emplace_back(GetFileStatus(filename, root_directory_.size()));
    } else if (dir_entry->d_type == DT_DIR) {
      StorageError has_error = ListDirectoryRecursively(filename, prefix, output_vector);
      if (has_error) {
        return has_error;
      }
    }
  }

  return StorageError::Success();
}

std::pair<std::vector<ObjectStatus>, StorageError> FilesystemStorage::List(const std::string& object_prefix = "") {
  std::vector<ObjectStatus> result_vector;
  const std::string full_prefix = JoinPath(root_directory_, object_prefix);
  return std::make_pair(result_vector, ListDirectoryRecursively(root_directory_, full_prefix, &result_vector));
}

FilesystemReader::FilesystemReader(const std::string& filename, size_t num_characters_hidden)
    : error_(StorageErrorType::kNoError), filename_(filename), num_characters_hidden_(num_characters_hidden) {
  in_.open(filename.c_str(), std::ios::in | std::ios::binary);
  if (!in_.is_open()) {
    error_ = StorageError(StorageErrorType::kNotFound);
  }
}
FilesystemReader::~FilesystemReader() {
  if (in_.is_open()) {
    in_.close();
  }
}

const ObjectStatus& FilesystemReader::GetStatus() {
  if (status_.GetError()) {
    status_ = GetFileStatus(filename_, num_characters_hidden_);
  }
  return status_;
}

StorageError FilesystemReader::Close() {
  if (in_.is_open()) {
    in_.close();
  }
  return StorageError::Success();
}
StorageError FilesystemReader::Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) {
  if (error_) {
    return error_;
  }

  in_.clear();

  const std::ifstream::pos_type p = in_.tellg();
  if (p == std::ifstream::pos_type(-1)) {
    return StorageError(StorageErrorType::kIOError);
  }

  if (p != std::ifstream::pos_type(first_byte)) {
    in_.seekg(std::ifstream::pos_type(first_byte));
  }

  if (!in_.good()) {
    return StorageError(StorageErrorType::kIOError);
  }

  // How many bytes do we want to read?
  //  From `first_byte` to `last_byte` including both!
  size_t bytes_left = last_byte - first_byte + 1;
  if (last_byte == ObjectReader::kLastByteInFile) {
    bytes_left = ObjectReader::kLastByteInFile;
  }

  // We cannot read more bytes than the file has.
  const size_t file_size = GetStatus().GetSize();
  if (first_byte + bytes_left > file_size) {
    bytes_left = file_size - first_byte;
  }

  buffer->Resize(bytes_left);
  in_.read(buffer->CharData(), bytes_left);

  if (std::cmp_not_equal(in_.gcount(), bytes_left) || !in_.good()) {
    return StorageError(StorageErrorType::kIOError);
  }

  return StorageError::Success();
}

StorageError FilesystemStorage::Delete(const std::string& object_identifier) {
  const std::string full_path = JoinPath(root_directory_, object_identifier);
  if (!unlink(full_path.c_str())) {
    return StorageError::Success();
  }
  return ErrnoToStorageError();
}

std::string FilesystemStorage::JoinPath(const std::string& part_a, const std::string& part_b) {
  if (part_a.empty()) {
    return part_b;
  }
  if (part_b.empty()) {
    return part_a;
  }

  const bool part_a_ends_with_separator = part_a.back() == '/';
  const bool part_b_starts_with_separator = part_b.front() == '/';

  if (!part_a_ends_with_separator && !part_b_starts_with_separator) {
    return part_a + '/' + part_b;
  }

  if (part_a_ends_with_separator && part_b_starts_with_separator) {
    return part_a + part_b.substr(1);
  }

  return part_a + part_b;
}

std::unique_ptr<ObjectWriter> FilesystemStorage::OpenForWriting(const std::string& object_identifier) {
  return std::make_unique<FilesystemWriter>(JoinPath(root_directory_, object_identifier));
}

std::unique_ptr<ObjectReader> FilesystemStorage::OpenForReading(const std::string& object_identifier) {
  return std::make_unique<FilesystemReader>(JoinPath(root_directory_, object_identifier), root_directory_.size());
}

}  // namespace skyrise
