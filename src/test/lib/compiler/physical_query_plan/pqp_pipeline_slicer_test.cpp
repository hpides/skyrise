#include "compiler/physical_query_plan/pqp_pipeline_slicer.hpp"

#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "benchmark/lib/tpch/tpch_data_generator.hpp"
#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_utils.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class PqpPipelineSlicerTest : public ::testing::Test {
 public:
  //void SetUp() override { }

//  std::shared_ptr<ExportOperatorProxy> CreateQ1Pqp(const std::vector<std::string>& import_keys) {
//    std::vector<ColumnID> column_ids = { ColumnID{4}, ColumnID{5}, ColumnID{6}, ColumnID{7}, ColumnID{8}, ColumnID{9}, ColumnID{10} };
//      // clang-format off
//      const auto q1_pqp =
//      ExportOperatorProxy::Dummy(
//        AliasOperatorProxy::Make(
//          SortOperatorProxy::Make(
//            ProjectionOperatorProxy::Make(
//              AggregateOperatorProxy::Make(
//                ExchangeOperatorProxy::Make(
//                  AggregateOperatorProxy::Make( // pre-agg
//                    ProjectionOperatorProxy::Make(
//                      FilterOperatorProxy::Make(
//                        CreateImportProxy(import_keys, column_ids))))))))));
//    // clang-format on
//    return q1_pqp;
//  }

  std::shared_ptr<ExportOperatorProxy> CreateQ3Pqp(const std::vector<std::string>& import_keys_customer, const std::vector<std::string>& import_keys_orders,
                                                   const std::vector<std::string>& import_keys_lineitem) {
    std::vector<ColumnID> column_ids = { ColumnID{4}, ColumnID{5}, ColumnID{6}, ColumnID{7}, ColumnID{8}, ColumnID{9}, ColumnID{10} };
      // clang-format off
      const auto q3_subplan_a =
      ExchangeOperatorProxy::Make( // TODO partition
        FilterOperatorProxy::Make(GreaterThan_(PqpColumn_(ColumnId{0}, DataType::kLong, false, "l_shipdate"), "1995-03-18"),
          CreateTpchImportProxy(TpchTable::kLineItem, std::vector<std::string>{"l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"})));


      const auto q3_subplan_b =
      ExchangeOperatorProxy // TODO partition
        FilterOperatorProxy::Make( // o_orderdate < 1995-03-18
          JoinOperatorProxy::Make( // o_custkey = c_custkey
            ExchangeOperatorProxy( // TODO partition
              CreateTpchImportProxy(TpchTable::kOrders, std::vector<std::string>{"o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"}))));
            ExchangeOperatorProxy( // TODO partition
              FilterOperatorProxy::Make(Equals_(PqpColumn_(ColumnId{1}, DataType::kString, false, "c_mktsegment"), "AUTOMOBILE"),
                CreateTpchImportProxy(TpchTable::kCustomer, std::vector<std::string>{"c_custkey", "c_mktsegment"}))));

      const auto q3_pqp =
      ExportOperatorProxy::Dummy(
        AliasOperatorProxy::Make(
          ProjectionOperatorProxy::Make(
            LimitOperatorProxy::Make(
              SortOperatorProxy::Make(
                AggregateOperatorProxy:Make(
                  ExchangeOperatorProxy::Make( // partial merge
                    AggregateOperatorProxy:Make( // pre-agg
                      ProjectionOperatorProxy::Make(
                        JoinOperatorProxy::Make( // l_order_key = o_order_key
                          q3_subplan_a,
                          q3_subplan_b))))))))));
      // clang-format on
  }
  
  std::shared_ptr<ImportOperatorProxy> CreateImportProxy(const std::vector<std::string>& import_keys,
                                                         const std::vector<ColumnID>& column_ids) {
    std::vector<ObjectReference>& object_references;
    for (const auto& import_key : import_keys) {
      object_references.emplace("mock_bucket", import_key, "mock_etag");
    }
    return ImportOperatorProxy::Make(object_references, column_ids);
  }
  
  std::shared_ptr<ImportOperatorProxy> CreateTpchImportProxy(const TpchTable table, std::vector<std::string> column_names) {
    const TableColumnDefinitions column_definitions = TpchColumnDefinitionsByTable(table);

    // Determine ColumnIDs to import from the provided column names.
    std::vector<ColumnID> import_column_ids;
    for (const std::string& column_name : column_names) {
      ColumnID import_column_id = kInvalidColumnId;
      for (ColumnID column_id = 0; column_id < column_definitions.size(); ++column_id) {
        if (column_definitions.at(column_id).name == column_name) {
          import_column_id = column_id;
          break;
        }
      }
      ASSERT_NE(import_column_id, kInvalidColumnId);
      import_column_ids.insert(import_column_id);
    }

    return CreateImportProxy(import_keys, import_column_ids)
  }

 protected:
};

TEST_F(PqpPipelineSlicerTest, TpchQ1) {
  // clang-format off
  const auto q1_pqp =
  ExportOperatorProxy::Dummy(
    AliasOperatorProxy::Make(
      SortOperatorProxy::Make(
        ProjectionOperatorProxy::Make(
          AggregateOperatorProxy::Make()
)))
  );
  // clang-format on
}

TEST_F(PqpPipelineSlicerTest, TpchQ3) {

}

}  // namespace skyrise