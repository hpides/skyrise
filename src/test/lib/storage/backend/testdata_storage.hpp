#pragma once

#include "storage/backend/filesystem_storage.hpp"

namespace skyrise {

class TestdataStorage : public FilesystemStorage {
 public:
  TestdataStorage();
};

}  // namespace skyrise
