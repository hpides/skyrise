#include "tpch_mock_catalog.hpp"

#include "data_generation/tpch/tpch_generator.hpp"
#include "storage/table/table_column_definition.hpp"
#include "table_schema.hpp"

namespace skyrise {

TpchMockCatalog::TpchMockCatalog() {
  // For each TPC-H table, create a TableSchema and add it to the mock catalog.

  // Table `customer`
  auto customer_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kCustomer);
  auto customer_table_schema = std::make_shared<TableSchema>(customer_column_definitions);
  AddTableSchema("customer", customer_table_schema);

  // Table `lineitem`
  auto lineitem_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kLineItem);
  auto lineitem_table_schema = std::make_shared<TableSchema>(lineitem_column_definitions);
  AddTableSchema("lineitem", lineitem_table_schema);

  // Table `nation`
  auto nation_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kNation);
  auto nation_table_schema = std::make_shared<TableSchema>(nation_column_definitions);
  AddTableSchema("nation", nation_table_schema);

  // Table `orders`
  auto orders_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kOrders);
  auto orders_table_schema = std::make_shared<TableSchema>(orders_column_definitions);
  AddTableSchema("orders", orders_table_schema);

  // Table `part`
  auto part_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kPart);
  auto part_table_schema = std::make_shared<TableSchema>(part_column_definitions);
  AddTableSchema("part", part_table_schema);

  // Table `partsupp`
  auto partsupp_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kPartSupp);
  auto partsupp_table_schema = std::make_shared<TableSchema>(partsupp_column_definitions);
  AddTableSchema("partsupp", partsupp_table_schema);

  // Table `region`
  auto region_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kRegion);
  auto region_table_schema = std::make_shared<TableSchema>(region_column_definitions);
  AddTableSchema("region", region_table_schema);

  // Table `supplier`
  auto supplier_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kSupplier);
  auto supplier_table_schema = std::make_shared<TableSchema>(supplier_column_definitions);
  AddTableSchema("supplier", supplier_table_schema);
}

}  // namespace skyrise
