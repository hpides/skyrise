#pragma once

#include "storage/backend/abstract_storage.hpp"
#include "storage/formats/abstract_format_writer.hpp"
#include "storage/table/table_writer.hpp"
#include "table_builder.hpp"

namespace skyrise {

class AbstractDataGenerator {
 public:
  explicit AbstractDataGenerator(TableWriterFactory&& table_writer_factory)
      : table_writer_factory_(std::move(table_writer_factory)) {}
  virtual ~AbstractDataGenerator() = default;

  virtual void Generate() = 0;

 protected:
  const TableWriterFactory& GetTableWriterFactory() { return table_writer_factory_; }

 private:
  TableWriterFactory table_writer_factory_;
};

}  // namespace skyrise
