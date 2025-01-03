#include <map>
#include <vector>

#include <gtest/gtest.h>

#include "storage/backend/s3_storage.hpp"

namespace skyrise {

class AwsS3Test : public ::testing::Test {};

TEST_F(AwsS3Test, TestErrorTranslation) {
  const std::map<StorageErrorType, std::vector<Aws::S3::S3Errors>> mapping = {
      {StorageErrorType::kInvalidArgument,
       {Aws::S3::S3Errors::INCOMPLETE_SIGNATURE, Aws::S3::S3Errors::INVALID_ACTION,
        Aws::S3::S3Errors::INVALID_PARAMETER_COMBINATION, Aws::S3::S3Errors::INVALID_PARAMETER_VALUE,
        Aws::S3::S3Errors::INVALID_QUERY_PARAMETER, Aws::S3::S3Errors::INVALID_SIGNATURE,
        Aws::S3::S3Errors::MALFORMED_QUERY_STRING, Aws::S3::S3Errors::MISSING_ACTION,
        Aws::S3::S3Errors::MISSING_PARAMETER, Aws::S3::S3Errors::OPT_IN_REQUIRED, Aws::S3::S3Errors::REQUEST_EXPIRED,
        Aws::S3::S3Errors::REQUEST_TIME_TOO_SKEWED}},
      {StorageErrorType::kInternalError, {Aws::S3::S3Errors::INTERNAL_FAILURE, Aws::S3::S3Errors::SERVICE_UNAVAILABLE}},
      {StorageErrorType::kAlreadyExist, {Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS}},
      {StorageErrorType::kPermissionDenied,
       {Aws::S3::S3Errors::ACCESS_DENIED, Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU,
        Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID, Aws::S3::S3Errors::INVALID_CLIENT_TOKEN_ID,
        Aws::S3::S3Errors::MISSING_AUTHENTICATION_TOKEN, Aws::S3::S3Errors::SIGNATURE_DOES_NOT_MATCH,
        Aws::S3::S3Errors::UNRECOGNIZED_CLIENT, Aws::S3::S3Errors::VALIDATION}},

      {StorageErrorType::kNotFound, {Aws::S3::S3Errors::RESOURCE_NOT_FOUND}},
      {StorageErrorType::kTemporary, {Aws::S3::S3Errors::SLOW_DOWN, Aws::S3::S3Errors::THROTTLING}},
      {StorageErrorType::kIOError, {Aws::S3::S3Errors::NETWORK_CONNECTION, Aws::S3::S3Errors::REQUEST_TIMEOUT}},
      {StorageErrorType::kNotFound,
       {Aws::S3::S3Errors::NO_SUCH_BUCKET, Aws::S3::S3Errors::NO_SUCH_KEY, Aws::S3::S3Errors::NO_SUCH_UPLOAD}},
      {StorageErrorType::kUnknown, {Aws::S3::S3Errors::UNKNOWN}}

  };

  for (const auto& check : mapping) {
    for (const auto& error : check.second) {
      ASSERT_EQ(TranslateS3Error(error), check.first);
    }
  }
}

}  // namespace skyrise
