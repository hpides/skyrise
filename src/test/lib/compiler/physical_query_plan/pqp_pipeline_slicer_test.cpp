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
  // void SetUp() override { }

  //  std::shared_ptr<ExportOperatorProxy> CreateQ1Pqp(const std::vector<std::string>& import_keys) {
  //    std::vector<ColumnId> column_ids = { ColumnId{4}, ColumnId{5}, ColumnId{6}, ColumnId{7}, ColumnId{8},
  //    ColumnId{9}, ColumnId{10} };
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

  //  std::shared_ptr<ExportOperatorProxy> CreateTpchQ3Pqp() {
  //    // clang-format off
  //    const auto q3_subplan_a =
  //    ExchangeOperatorProxy::Make( // TODO partition
  //      FilterOperatorProxy::Make(GreaterThan_(TpchPqpColumn("l_shipdate"), "1995-03-18"),
  //        TpchImportProxy(std::vector<std::string>{"l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"},
  //        CreateMockObjectReferences("lineitem_", 6000));
  //
  //
  //    const auto q3_subplan_b =
  //    ExchangeOperatorProxy // TODO partition
  //      FilterOperatorProxy::Make( // o_orderdate < 1995-03-18
  //        JoinOperatorProxy::Make( // o_custkey = c_custkey
  //          ExchangeOperatorProxy( // TODO partition
  //            TpchImportProxy(std::vector<std::string>{"o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"},
  //            CreateMockObjectReferences("orders_", 1500)),
  //          ExchangeOperatorProxy( // TODO partition
  //            FilterOperatorProxy::Make(Equals_(TpchPqpColumn("c_mktsegment"), "AUTOMOBILE"),
  //              TpchImportProxy(std::vector<std::string>{"c_custkey", "c_mktsegment"},
  //              CreateMockObjectReferences("customer_", 150)))));
  //
  //    const auto q3_pqp =
  //    ExportOperatorProxy::Dummy(
  //      AliasOperatorProxy::Make(
  //        ProjectionOperatorProxy::Make(
  //          LimitOperatorProxy::Make(
  //            SortOperatorProxy::Make(
  //              AggregateOperatorProxy:Make(
  //                ExchangeOperatorProxy::Make( // partial merge
  //                  AggregateOperatorProxy:Make( // pre-agg
  //                    ProjectionOperatorProxy::Make(
  //                      JoinOperatorProxy::Make( // l_order_key = o_order_key
  //                        q3_subplan_a,
  //                        q3_subplan_b))))))))));
  //    // clang-format on
  //
  //    return q3_pqp;
  //  }

 protected:
};

TEST_F(PqpPipelineSlicerTest, TpchQ1) {
  // clang-format off
//  const auto q1_pqp =
  // clang-format on
}

// TEST_F(PqpPipelineSlicerTest, TpchQ3) {}

}  // namespace skyrise