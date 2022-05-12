#include "tpch_generator.hpp"

#include <cmath>

#include "../table_builder.hpp"

extern "C" {
#include <dss.h>
#include <dsstypes.h>
#include <rnd.h>
}

namespace {

using namespace skyrise;  // NOLINT(google-build-using-namespace)

const auto kCustomerColumnTypes =
    std::tuple<int32_t, std::string, std::string, int32_t, std::string, float, std::string, std::string>();
const auto kCustomerColumnNames = std::make_tuple("c_custkey", "c_name", "c_address", "c_nationkey", "c_phone",
                                                  "c_acctbal", "c_mktsegment", "c_comment");

const auto kOrderColumnTypes =
    std::tuple<int32_t, int32_t, std::string, float, std::string, std::string, std::string, int32_t, std::string>();
const auto kOrderColumnNames =
    std::make_tuple("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority",
                    "o_clerk", "o_shippriority", "o_comment");

const auto kLineitemColumnTypes =
    std::tuple<int32_t, int32_t, int32_t, int32_t, float, float, float, float, std::string, std::string, std::string,
               std::string, std::string, std::string, std::string, std::string>();
const auto kLineitemColumnNames =
    std::make_tuple("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice",
                    "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
                    "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment");

const auto kPartColumnTypes =
    std::tuple<int32_t, std::string, std::string, std::string, std::string, int32_t, std::string, float, std::string>();
const auto kPartColumnNames = std::make_tuple("p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size",
                                              "p_container", "p_retailsize", "p_comment");

const auto kPartsuppColumnTypes = std::tuple<int32_t, int32_t, int32_t, float, std::string>();
const auto kPartsuppColumnNames =
    std::make_tuple("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment");

const auto kSupplierColumnTypes =
    std::tuple<int32_t, std::string, std::string, int32_t, std::string, float, std::string>();
const auto kSupplierColumnNames =
    std::make_tuple("s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment");

const auto kNationColumnTypes = std::tuple<int32_t, std::string, int32_t, std::string>();
const auto kNationColumnNames = std::make_tuple("n_nationkey", "n_name", "n_regionkey", "n_comment");

const auto kRegionColumnTypes = std::tuple<int32_t, std::string, std::string>();
const auto kRegionColumnNames = std::make_tuple("r_regionkey", "r_name", "r_comment");

std::unordered_map<TpchTable, std::underlying_type_t<TpchTable>> tpch_table_to_dbgen_id = {
    {TpchTable::kPart, PART},     {TpchTable::kPartSupp, PSUPP}, {TpchTable::kSupplier, SUPP},
    {TpchTable::kCustomer, CUST}, {TpchTable::kOrders, ORDER},   {TpchTable::kLineItem, LINE},
    {TpchTable::kNation, NATION}, {TpchTable::kRegion, REGION}};

float ConvertMoney(DSS_HUGE cents) {
  const auto dollars = cents / 100;
  cents %= 100;
  return dollars + (static_cast<float>(cents)) / 100.0f;
}

std::unordered_map<TpchTable, std::string> tpch_table_names = {
    {TpchTable::kPart, "part"},         {TpchTable::kPartSupp, "partsupp"}, {TpchTable::kSupplier, "supplier"},
    {TpchTable::kCustomer, "customer"}, {TpchTable::kOrders, "orders"},     {TpchTable::kLineItem, "lineitem"},
    {TpchTable::kNation, "nation"},     {TpchTable::kRegion, "region"}};

template <typename DSSType, typename RowGeneratorResultType, typename... Args>
DSSType CallDbgenRowGenerator(size_t idx,
                              RowGeneratorResultType (*row_generator_function)(DSS_HUGE, DSSType* val, Args...),
                              TpchTable table, Args... args) {
  // Preserve calling scheme (row_start(); mk...(); row_stop(); as in dbgen's gen_tbl())

  const auto dbgen_table_id = tpch_table_to_dbgen_id.at(table);

  row_start(dbgen_table_id);

  DSSType value{};
  row_generator_function(idx, &value, std::forward<Args>(args)...);

  row_stop(dbgen_table_id);

  return value;
}

}  // namespace

namespace skyrise {

TableColumnDefinitions TpchColumnDefinitionsByTable(TpchTable table) {
  switch (table) {
    case TpchTable::kCustomer:
      return GetSchemaFromTypesAndNames(kCustomerColumnTypes, kCustomerColumnNames);
    case TpchTable::kLineItem:
      return GetSchemaFromTypesAndNames(kLineitemColumnTypes, kLineitemColumnNames);
    case TpchTable::kNation:
      return GetSchemaFromTypesAndNames(kNationColumnTypes, kNationColumnNames);
    case TpchTable::kOrders:
      return GetSchemaFromTypesAndNames(kOrderColumnTypes, kOrderColumnNames);
    case TpchTable::kPart:
      return GetSchemaFromTypesAndNames(kPartColumnTypes, kPartColumnNames);
    case TpchTable::kPartSupp:
      return GetSchemaFromTypesAndNames(kPartsuppColumnTypes, kPartsuppColumnNames);
    case TpchTable::kRegion:
      return GetSchemaFromTypesAndNames(kRegionColumnTypes, kRegionColumnNames);
    case TpchTable::kSupplier:
      return GetSchemaFromTypesAndNames(kSupplierColumnTypes, kSupplierColumnNames);
    default:
      Fail("Undefined for given TPC-H table.");
  }
}

TPCHGenerator::TPCHGenerator(PartitionedChunkWriterFactory chunk_writer_factory, float scale_factor)
    : AbstractDataGenerator(std::move(chunk_writer_factory)), scale_factor_(scale_factor) {}

void TPCHGenerator::EnableTable(TpchTable table) { tables_enabled_[table] = true; }

bool TPCHGenerator::IsTableEnabled(TpchTable table) { return tables_enabled_.find(table) != tables_enabled_.cend(); }

void TPCHGenerator::DisableTable(TpchTable table) {
  if (IsTableEnabled(table)) {
    tables_enabled_.erase(table);
  }
}

void TPCHGenerator::EnableAllTables() {
  EnableTable(TpchTable::kCustomer);
  EnableTable(TpchTable::kLineItem);
  EnableTable(TpchTable::kNation);
  EnableTable(TpchTable::kOrders);
  EnableTable(TpchTable::kPart);
  EnableTable(TpchTable::kPartSupp);
  EnableTable(TpchTable::kRegion);
  EnableTable(TpchTable::kSupplier);
}

void TPCHGenerator::DisableAllTables() { tables_enabled_.clear(); }

void TPCHGenerator::Generate() {
  auto null_writer_factory = [](const std::string& /*name*/,
                                const TableColumnDefinitions& /*schema*/) -> std::shared_ptr<PartitionedChunkWriter> {
    return nullptr;
  };

  // Init tpch_dbgen - it is important this is done before any data structures from tpch_dbgen are read.
  dbgen_reset_seeds();
  dbgen_init_scale_factor(scale_factor_);

  const auto customer_count = static_cast<size_t>(tdefs[CUST].base * scale);
  const auto order_count = static_cast<size_t>(tdefs[ORDER].base * scale);
  const auto part_count = static_cast<size_t>(tdefs[PART].base * scale);
  const auto supplier_count = static_cast<size_t>(tdefs[SUPP].base * scale);
  const auto nation_count = static_cast<size_t>(tdefs[NATION].base);
  const auto region_count = static_cast<size_t>(tdefs[REGION].base);

  if (IsTableEnabled(TpchTable::kCustomer)) {
    TableBuilder builder(GetPartitionedChunkWriterFactory(), tpch_table_names.at(TpchTable::kCustomer),
                         kCustomerColumnTypes, kCustomerColumnNames);
    for (size_t i = 0; i < customer_count; ++i) {
      auto customer = CallDbgenRowGenerator<customer_t>(i + 1, mk_cust, TpchTable::kCustomer);
      builder.AppendRow(customer.custkey, customer.name, customer.address, customer.nation_code, customer.phone,
                        ConvertMoney(customer.acctbal), customer.mktsegment, customer.comment);
    }
  }

  if (IsTableEnabled(TpchTable::kOrders) || IsTableEnabled(TpchTable::kLineItem)) {
    TableBuilder order_builder(
        IsTableEnabled(TpchTable::kOrders) ? GetPartitionedChunkWriterFactory() : null_writer_factory,
        tpch_table_names.at(TpchTable::kOrders), kOrderColumnTypes, kOrderColumnNames);
    TableBuilder lineitem_builder(
        IsTableEnabled(TpchTable::kLineItem) ? GetPartitionedChunkWriterFactory() : null_writer_factory,
        tpch_table_names.at(TpchTable::kLineItem), kLineitemColumnTypes, kLineitemColumnNames);

    for (size_t i = 0; i < order_count; ++i) {
      const auto order = CallDbgenRowGenerator<order_t>(i + 1, mk_order, TpchTable::kOrders, 0l);
      order_builder.AppendRow(order.okey, order.custkey, std::string(1, order.orderstatus),
                              ConvertMoney(order.totalprice), order.odate, order.opriority, order.clerk,
                              order.spriority, order.comment);

      if (!IsTableEnabled(TpchTable::kLineItem)) {
        continue;
      }
      for (size_t i = 0; i < static_cast<size_t>(order.lines); ++i) {
        const auto& lineitem = order.l[i];
        lineitem_builder.AppendRow(lineitem.okey, lineitem.partkey, lineitem.suppkey, lineitem.lcnt, lineitem.quantity,
                                   ConvertMoney(lineitem.eprice), ConvertMoney(lineitem.discount),
                                   ConvertMoney(lineitem.tax), std::string(1, lineitem.rflag[0]),
                                   std::string(1, lineitem.lstatus[0]), lineitem.sdate, lineitem.cdate, lineitem.rdate,
                                   lineitem.shipinstruct, lineitem.shipmode, lineitem.comment);
      }
    }
  }

  if (IsTableEnabled(TpchTable::kPart) || IsTableEnabled(TpchTable::kPartSupp)) {
    TableBuilder part_builder(
        IsTableEnabled(TpchTable::kPart) ? GetPartitionedChunkWriterFactory() : null_writer_factory,
        tpch_table_names.at(TpchTable::kPart), kPartColumnTypes, kPartColumnNames);
    TableBuilder partsupp_builder(
        IsTableEnabled(TpchTable::kPartSupp) ? GetPartitionedChunkWriterFactory() : null_writer_factory,
        tpch_table_names.at(TpchTable::kPartSupp), kPartsuppColumnTypes, kPartsuppColumnNames);

    for (size_t i = 0; i < part_count; ++i) {
      const auto part = CallDbgenRowGenerator<part_t>(i + 1, mk_part, TpchTable::kPart);

      part_builder.AppendRow(part.partkey, part.name, part.mfgr, part.brand, part.type, part.size, part.container,
                             ConvertMoney(part.retailprice), part.comment);

      // Some scale factors (e.g., 0.05) are not supported by tpch-dbgen as they produce non-unique partkey/suppkey
      // combinations. The reason is probably somewhere in the magic in PART_SUPP_BRIDGE. As the partkey is
      // ascending, those are easy to identify:

      DSS_HUGE last_partkey = 0;
      std::vector<DSS_HUGE> suppkeys;

      if (!IsTableEnabled(TpchTable::kPartSupp)) {
        continue;
      }
      for (const auto& partsupp : part.s) {
        {
          // Make sure we do not generate non-unique combinations (see above)
          if (partsupp.partkey != last_partkey) {
            Assert(partsupp.partkey > last_partkey, "Expected partkey to be generated in ascending order.");
            last_partkey = partsupp.partkey;
            suppkeys.clear();
          }
          Assert(std::find(suppkeys.begin(), suppkeys.end(), partsupp.suppkey) == suppkeys.end(),
                 "Scale factor unsupported by tpch-dbgen. Consider choosing a \"round\" number.");
          suppkeys.emplace_back(partsupp.suppkey);
        }

        partsupp_builder.AppendRow(partsupp.partkey, partsupp.suppkey, partsupp.qty, ConvertMoney(partsupp.scost),
                                   partsupp.comment);
      }
    }
  }

  if (IsTableEnabled(TpchTable::kSupplier)) {
    TableBuilder builder(GetPartitionedChunkWriterFactory(), tpch_table_names.at(TpchTable::kSupplier),
                         kSupplierColumnTypes, kSupplierColumnNames);
    for (size_t i = 0; i < supplier_count; ++i) {
      const auto supplier = CallDbgenRowGenerator<supplier_t>(i + 1, mk_supp, TpchTable::kSupplier);

      builder.AppendRow(supplier.suppkey, supplier.name, supplier.address, supplier.nation_code, supplier.phone,
                        ConvertMoney(supplier.acctbal), supplier.comment);
    }
  }

  if (IsTableEnabled(TpchTable::kNation)) {
    TableBuilder builder(GetPartitionedChunkWriterFactory(), tpch_table_names.at(TpchTable::kNation),
                         kNationColumnTypes, kNationColumnNames);
    for (size_t i = 0; i < nation_count; ++i) {
      const auto nation = CallDbgenRowGenerator<code_t>(i + 1, mk_nation, TpchTable::kNation);
      builder.AppendRow(nation.code, nation.text, nation.join, nation.comment);
    }
  }

  if (IsTableEnabled(TpchTable::kRegion)) {
    TableBuilder builder(GetPartitionedChunkWriterFactory(), tpch_table_names.at(TpchTable::kRegion),
                         kRegionColumnTypes, kRegionColumnNames);
    for (size_t i = 0; i < region_count; ++i) {
      const auto region = CallDbgenRowGenerator<code_t>(i + 1, mk_region, TpchTable::kRegion);
      builder.AppendRow(region.code, region.text, region.comment);
    }
  }

  dbgen_cleanup();
}

}  // namespace skyrise
