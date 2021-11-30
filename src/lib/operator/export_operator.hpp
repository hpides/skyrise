#pragma once

#include "abstract_operator.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/formats/abstract_chunk_writer.hpp"

namespace skyrise {

/**
 * ExportOperator has one input operator and no output. The input table is written to the given Storage. Currently kCsv
 * and kOrc are supported as output formats. If kOrcPartitioned is chosen, row offsets to each processed chunk (produced
 * by for example PartitionOperator) can be stored as metadata to allow reading only relevant parts of the table. See
 * OrcWriter for more details on partitioning.
 */
class ExportOperator : public AbstractOperator {
 public:
  enum class OutputFormat { kCsv, kOrc, kOrcPartitioned };

  ExportOperator(const std::shared_ptr<const AbstractOperator>& input_operator, std::string bucket_name,
                 std::string target_object_key, OutputFormat output_format);

  const std::string& Name() const override;
  std::shared_ptr<const Table> OnExecute(const std::shared_ptr<OperatorExecutionContext>& context = nullptr) override;

 private:
  std::unique_ptr<AbstractFormatWriter> GetWriter();

  std::string bucket_name_;
  std::shared_ptr<Storage> storage_;
  std::string target_object_key_;
  OutputFormat output_format_;
};

}  // namespace skyrise
