#include "tpch_mock_catalog.hpp"

#include "data_generation/tpch/tpch_generator.hpp"
#include "storage/table/table_column_definition.hpp"
#include "table_schema.hpp"

namespace skyrise {

TpchMockCatalog::TpchMockCatalog() {
  // For each TPC-H table, create a TableSchema and add it to the mock catalog.

  // Table `customer`
  auto customer_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kCustomer);
  auto customer_table_schema = TableSchema::FromTableColumnDefinitions(customer_column_definitions);
  customer_table_schema->AddKeyConstraint(
      {{customer_table_schema->ColumnIdByName("c_custkey")}, KeyConstraintType::kPrimaryKey});
  AddTableSchema("customer", customer_table_schema);

  // Table `lineitem`
  auto lineitem_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kLineItem);
  auto lineitem_table_schema = TableSchema::FromTableColumnDefinitions(lineitem_column_definitions);
  const TableKeyConstraint lineitem_primary_key_constraint(
      {lineitem_table_schema->ColumnIdByName("l_orderkey"), lineitem_table_schema->ColumnIdByName("l_linenumber")},
      KeyConstraintType::kPrimaryKey);
  lineitem_table_schema->AddKeyConstraint(lineitem_primary_key_constraint);
  AddTableSchema("lineitem", lineitem_table_schema);

  // Table `nation`
  auto nation_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kNation);
  auto nation_table_schema = TableSchema::FromTableColumnDefinitions(nation_column_definitions);
  const TableKeyConstraint nation_primary_key_constraint =
      TableKeyConstraint{{nation_table_schema->ColumnIdByName("n_nationkey")}, KeyConstraintType::kPrimaryKey};
  nation_table_schema->AddKeyConstraint(nation_primary_key_constraint);
  AddTableSchema("nation", nation_table_schema);

  // Table `orders`
  auto orders_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kOrders);
  auto orders_table_schema = TableSchema::FromTableColumnDefinitions(orders_column_definitions);
  const TableKeyConstraint orders_primary_key_constraint =
      TableKeyConstraint{{orders_table_schema->ColumnIdByName("o_orderkey")}, KeyConstraintType::kPrimaryKey};
  orders_table_schema->AddKeyConstraint(orders_primary_key_constraint);
  AddTableSchema("orders", orders_table_schema);

  // Table `part`
  auto part_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kPart);
  auto part_table_schema = TableSchema::FromTableColumnDefinitions(part_column_definitions);
  const TableKeyConstraint part_table_primary_key_constraint =
      TableKeyConstraint{{part_table_schema->ColumnIdByName("p_partkey")}, KeyConstraintType::kPrimaryKey};
  part_table_schema->AddKeyConstraint(part_table_primary_key_constraint);
  AddTableSchema("part", part_table_schema);

  // Table `partsupp`
  auto partsupp_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kPartSupp);
  auto partsupp_table_schema = TableSchema::FromTableColumnDefinitions(partsupp_column_definitions);
  const TableKeyConstraint partsupp_primary_key_constraint(
      {partsupp_table_schema->ColumnIdByName("ps_partkey"), partsupp_table_schema->ColumnIdByName("ps_suppkey")},
      KeyConstraintType::kPrimaryKey);
  partsupp_table_schema->AddKeyConstraint(partsupp_primary_key_constraint);
  AddTableSchema("partsupp", partsupp_table_schema);

  // Table `region`
  auto region_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kRegion);
  auto region_table_schema = TableSchema::FromTableColumnDefinitions(region_column_definitions);
  const TableKeyConstraint region_primary_key_constraint({region_table_schema->ColumnIdByName("r_regionkey")},
                                                         KeyConstraintType::kPrimaryKey);
  region_table_schema->AddKeyConstraint(region_primary_key_constraint);
  AddTableSchema("region", region_table_schema);

  // Table `supplier`
  auto supplier_column_definitions = TpchColumnDefinitionsByTable(TpchTable::kSupplier);
  auto supplier_table_schema = TableSchema::FromTableColumnDefinitions(supplier_column_definitions);
  const TableKeyConstraint supplier_primary_key_constraint({supplier_table_schema->ColumnIdByName("s_suppkey")},
                                                           KeyConstraintType::kPrimaryKey);
  supplier_table_schema->AddKeyConstraint(supplier_primary_key_constraint);
  AddTableSchema("supplier", supplier_table_schema);
}

}  // namespace skyrise
