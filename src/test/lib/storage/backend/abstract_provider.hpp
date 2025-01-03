#pragma once

#include "storage/backend/abstract_storage.hpp"

namespace skyrise {

class StorageProvider {
 public:
  virtual ~StorageProvider() = default;

  virtual bool IsEventuallyConsistent() { return false; }

  virtual Storage& GetStorage() = 0;
  virtual void SetUp() = 0;
  virtual void TearDown() = 0;
};

}  // namespace skyrise
