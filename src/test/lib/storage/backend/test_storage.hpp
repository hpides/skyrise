#pragma once

#include "storage/backend/filesystem_storage.hpp"

namespace skyrise {

class TestStorage : public FilesystemStorage {
 public:
  TestStorage();
};

}  // namespace skyrise
