/**
 * Taken and modified from our third party submodule cmake-git-version-tracking
 */
#pragma once

#include <string>

class GitMetadata {
 public:
  // Check for uncommitted changes that are not reflected in the commit hash
  static bool HasUncommittedChanges();

  // The Git commit SHA-1 hash
  static std::string CommitSha1();
};
