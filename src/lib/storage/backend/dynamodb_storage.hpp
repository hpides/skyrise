#pragma once

#include <aws/dynamodb/DynamoDBClient.h>

#include "abstract_storage.hpp"

namespace skyrise {

class DynamoDbObjectWriter : public ObjectWriter {
 public:
  DynamoDbObjectWriter(std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client, const std::string& table_name,
                       const std::string& object_id);

  /**
   * Just buffers the data. Close() flushes the buffer.
   */
  StorageError Write(const char* data, size_t length) override;

  /**
   * Flushes the buffered data.
   */
  StorageError Close() override;

 private:
  StorageError UploadBuffer();

  const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client_;
  const std::string table_name_;
  std::string buffer_;
  const std::string object_id_;
  bool closed_{false};
};

class DynamoDbObjectReader : public ObjectReader {
 public:
  DynamoDbObjectReader(std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client, const std::string table_name,
                       std::string object_id);

  /**
   * Due to DynamoDB's read mechanics, the entire item is fetched, independent of the passed byte range. The request's
   * latency and cost (one read unit per 4 KB data) solely depends on the object's size (max kDynamoDbMaxItemSize).
   *
   * A higher level caching strategy may be useful in case many small byte ranges are fetched
   * (e.g., by a FormatReader).
   */
  StorageError Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) override;
  const ObjectStatus& GetStatus() override;
  StorageError Close() override;

 private:
  const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client_;
  const std::string table_name_;
  const std::string object_id_;
};

/**
 * This class mocks the Storage interface on top of DynamoDB to provide the same storage semantics as S3.
 * The LastModifiedTimestamp is taken from the uploading machine at point of upload.
 */
class DynamoDbStorage : public Storage {
 public:
  DynamoDbStorage(std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client, std::string table_name);

  static StorageError CreateTable(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client,
                                  const std::string& table);
  static StorageError DeleteTable(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client,
                                  const std::string& table);

  std::unique_ptr<ObjectWriter> OpenForWriting(const std::string& object_identifier) override {
    return std::make_unique<DynamoDbObjectWriter>(client_, table_name_, object_identifier);
  }

  /**
   * Returns a non-caching object reader.
   */
  std::unique_ptr<ObjectReader> OpenForReading(const std::string& object_identifier) override {
    return std::make_unique<DynamoDbObjectReader>(client_, table_name_, object_identifier);
  }

  StorageError Delete(const std::string& object_identifier) override;

  std::pair<std::vector<ObjectStatus>, StorageError> List(const std::string& object_prefix) override;

 private:
  const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client_;
  const std::string table_name_;
};

}  // namespace skyrise
