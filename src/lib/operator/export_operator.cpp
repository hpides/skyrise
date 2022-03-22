#include "export_operator.hpp"

#include "storage/formats/csv_writer.hpp"
#include "storage/formats/orc_writer.hpp"

namespace {

const std::string kName = "Export";

}  // namespace

namespace skyrise {

ExportOperator::ExportOperator(const std::shared_ptr<const AbstractOperator>& input_operator, std::string bucket_name,
                               std::string target_object_key, ExportFormat export_format)
    : AbstractOperator(OperatorType::kExport, input_operator),
      bucket_name_(std::move(bucket_name)),
      target_object_key_(std::move(target_object_key)),
      export_format_(export_format) {}

const std::string& ExportOperator::Name() const { return kName; }

std::unique_ptr<AbstractFormatWriter> ExportOperator::GetWriter() {
  switch (export_format_) {
    case ExportFormat::kCsv: {
      CsvFormatWriterOptions options;
      return std::make_unique<CsvFormatWriter>(options);
    }
    case ExportFormat::kOrc: {
      OrcFormatWriterOptions options;
      return std::make_unique<OrcFormatWriter>(options);
    }
    case ExportFormat::kOrcPartitioned: {
      OrcFormatWriterOptions options;
      options.save_chunk_offsets = true;
      return std::make_unique<OrcFormatWriter>(options);
    }
      // Missed case will trigger compile-time linter error.
  }

  // Unreachable code, but required to compile on gcc.
  return nullptr;
}

std::shared_ptr<const Table> ExportOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  const std::unique_ptr<ObjectWriter> output =
      operator_execution_context->GetStorage(bucket_name_)->OpenForWriting(target_object_key_);
  const std::unique_ptr<AbstractFormatWriter> formatter = GetWriter();
  const std::shared_ptr<const skyrise::Table> input = LeftInputTable();
  StorageError error = StorageError::Success();

  formatter->SetOutputHandler([&output, &error](const char* data, size_t length) {
    if (error) {
      return;
    }
    error = output->Write(data, length);
  });

  formatter->Initialize(input->ColumnDefinitions());

  for (ChunkId i = 0; i < input->ChunkCount(); ++i) {
    formatter->ProcessChunk(input->GetChunk(i));
  }

  formatter->Finalize();
  output->Close();

  Assert(!error.IsError(), error.GetMessage());

  return nullptr;
}

}  // namespace skyrise
