/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_data_generator.hpp"

namespace skyrise {

enum class TpchTable { kPart, kPartSupp, kSupplier, kCustomer, kOrders, kLineItem, kNation, kRegion };

TableColumnDefinitions TpchColumnDefinitionsByTable(TpchTable table);

class TpchDataGenerator : public AbstractDataGenerator {
 public:
  TpchDataGenerator(PartitionedChunkWriterFactory chunk_writer_factory, float scale_factor);
  void EnableTable(TpchTable table);
  bool IsTableEnabled(TpchTable table);
  void DisableTable(TpchTable table);
  void EnableAllTables();
  void DisableAllTables();
  void Generate();

 private:
  float scale_factor_;
  std::unordered_map<TpchTable, bool> tables_enabled_;
};

}  // namespace skyrise
