#pragma once

#include "storage/backend/storage_filesystem.hpp"

namespace skyrise {

class TestdataStorage : public FilesystemStorage {
 public:
  TestdataStorage();
};

}  // namespace skyrise
