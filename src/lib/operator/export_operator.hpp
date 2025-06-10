#pragma once

#include "abstract_operator.hpp"
#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/formats/abstract_chunk_writer.hpp"

namespace skyrise {

/**
 * ExportOperator has one input operator and no output. The input table is written to the given Storage. Currently kCsv,
 * kParquet and kOrc are supported as output formats.
 */
class ExportOperator : public AbstractOperator {
 public:
  ExportOperator(std::shared_ptr<const AbstractOperator> input_operator, std::string bucket_name,
                 std::string target_object_key, FileFormat export_format);

  const std::string& Name() const override;
  std::shared_ptr<const Table> OnExecute(const std::shared_ptr<OperatorExecutionContext>& context = nullptr) override;

  size_t GetBytesWritten() const;

 private:
  std::unique_ptr<AbstractFormatWriter> GetWriter();

  std::string bucket_name_;
  std::shared_ptr<Storage> storage_;
  std::string target_object_key_;
  FileFormat export_format_;
  size_t bytes_written_ = 0;
};

}  // namespace skyrise
