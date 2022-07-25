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
  //    std::vector<ColumnID> column_ids = { ColumnID{4}, ColumnID{5}, ColumnID{6}, ColumnID{7}, ColumnID{8},
  //    ColumnID{9}, ColumnID{10} };
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

  std::shared_ptr<PqpColumnExpression> PqpColumnFrom(ColumnID column_id, std::shared_ptr<AbstractExpression> expression) {
    // We assume nullable=false because there is no easy way to derive this information from the input expression.
    return PqpColumn_(column_id, expression->GetDataType(), false, expression->AsColumnName());
  }

  std::shared_ptr<ExportOperatorProxy> CreateTpchQ3Pqp() {
    // clang-format off
    const auto q3_subplan_a =
    ExchangeOperatorProxy::Make( // TODO partition
      FilterOperatorProxy::Make(GreaterThan_(TpchPqpColumn("l_shipdate"), "1995-03-18"),
        TpchImportProxy(std::vector<std::string>{"l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"}, CreateMockObjectReferences("lineitem_", 6000));


    const auto q3_subplan_b =
    ExchangeOperatorProxy // TODO partition
      FilterOperatorProxy::Make( // o_orderdate < 1995-03-18
        JoinOperatorProxy::Make( // o_custkey = c_custkey
          ExchangeOperatorProxy( // TODO partition
            TpchImportProxy(std::vector<std::string>{"o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"}, CreateMockObjectReferences("orders_", 1500)),
          ExchangeOperatorProxy( // TODO partition
            FilterOperatorProxy::Make(Equals_(TpchPqpColumn("c_mktsegment"), "AUTOMOBILE"),
              TpchImportProxy(std::vector<std::string>{"c_custkey", "c_mktsegment"}, CreateMockObjectReferences("customer_", 150)))));

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

    return q3_pqp;
  }

  std::shared_ptr<ExportOperatorProxy> CreateTpchQ1Pqp() {
    std::vector<SortColumnDefinition> sort_definitions;

    const auto l_shipdate = TpchPqpColumn("l_shipdate");
    const auto l_quantity = TpchPqpColumn("l_quantity");
    const auto l_extendedprice = TpchPqpColumn("l_extendedprice");
    const auto l_discount = TpchPqpColumn("l_discount");
    const auto l_returnflag = TpchPqpColumn("l_returnflag");
    const auto l_linestatus = TpchPqpColumn("l_linestatus");
    const auto l_extendedprice_MUL_SUB_l_discount = Mul_(l_extendedprice, Sub_(1, l_discount));
    const auto l_extendedprice_MUL_SUB_l_discount_MUL_ADD_l_tax = Mul_(l_extendedprice_MUL_SUB_l_discount, Add_(1, TpchPqpColumn("l_tax")));

    // clang-format off
    const auto q1_subplan_p1 =
    ExchangeOperatorProxy::Make(,
      AggregateOperatorProxy::Make(std::vector<ColumnId>{ColumnID{5}, ColumnID{6}}, // Pre-Aggregate: l_returnflag, l_linestatus
                                   std::vector<std::shared_ptr<AbstractExpression>>{Sum_(PqpColumnFrom(ColumnID{0}, l_quantity)), Sum_(PqpColumnFrom(ColumnID{0}, l_extendedprice), }
        ProjectionOperatorProxy::Make(ExpressionVector_(l_quantity, l_extendedprice, l_extendedprice_MUL_SUB_l_discount, l_extendedprice_MUL_SUB_l_discount_MUL_ADD_l_tax, l_discount, l_returnflag, l_linestatus),
          FilterOperatorProxy::Make(LessThan_(l_shipdate, "1998-09-02"),
            TpchImportProxy(std::vector<std::string>{"l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate"},
                            CreateMockObjectReferences("lineitem_", 5000))))));

    const auto q1_pqp =
    ExportOperatorProxy::Dummy(
      AliasOperatorProxy::Make(std::vector<ColumnID{}, std::vector<std::string>{},
        SortOperatorProxy::Make(sort_definitions,
          ProjectionOperatorProxy::Make(,
            AggregateOperatorProxy::Make(,
              q1_subplan)))));
    // clang-format on

    return q1_pqp;
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

TEST_F(PqpPipelineSlicerTest, TpchQ3) {}

}  // namespace skyrise