#pragma once

#include <memory>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/elasticfilesystem/EFSClient.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/s3/S3Client.h>
#include <aws/sqs/SQSClient.h>

#include "configuration.hpp"

namespace skyrise {

class BaseClient {
 public:
  BaseClient();
  BaseClient(const BaseClient&) = delete;
  BaseClient(BaseClient&&) = default;
  const BaseClient& operator=(const BaseClient&) = delete;
  BaseClient& operator=(BaseClient&&) = default;

  ~BaseClient() = default;

  std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> GetDynamoDbClient() const;
  std::shared_ptr<const Aws::EFS::EFSClient> GetEfsClient() const;
  std::shared_ptr<const Aws::Lambda::LambdaClient> GetLambdaClient() const;
  std::shared_ptr<const Aws::S3::S3Client> GetS3Client() const;
  std::shared_ptr<const Aws::SQS::SQSClient> GetSqsClient() const;

  const Aws::String& GetClientRegion() const;

 protected:
  static Aws::Client::ClientConfiguration GenerateClientConfig();

 private:
  std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> dynamodb_client_;
  std::shared_ptr<const Aws::EFS::EFSClient> efs_client_;
  std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client_;
  std::shared_ptr<const Aws::S3::S3Client> s3_client_;
  std::shared_ptr<const Aws::SQS::SQSClient> sqs_client_;

  Aws::String client_region_;

  inline static const Aws::Http::Scheme kHttpScheme = Aws::Http::Scheme::HTTPS;
  static constexpr size_t kMaxConnections = 100;
  static constexpr size_t kRequestTimeoutMs = kLambdaFunctionTimeoutSeconds * 1'000;
  static constexpr bool kEnableTcpKeepAlive = true;
  static constexpr bool kVerifySsl = true;
  // Default location of certificate authority file on Amazon Linux 2023
  inline static const Aws::String kCaFile{"/etc/pki/tls/certs/ca-bundle.crt"};
};

}  // namespace skyrise
