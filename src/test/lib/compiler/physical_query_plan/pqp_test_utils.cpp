#include "pqp_test_utils.hpp"

#include "constants.hpp"

namespace skyrise {

namespace {

/**
 * TODO
 * @param column_id
 * @param expression
 * @return
 */
std::shared_ptr<PqpColumnExpression> PqpColumnFrom(ColumnID column_id, std::shared_ptr<AbstractExpression> expression) {
    // We assume nullable=false because there is no easy way to derive this information from the input expression.
    return PqpColumn_(column_id, expression->GetDataType(), false, expression->AsColumnName());
}

/**
 * @return the according TpchTable, based on the prefix of @param tpch_column_name.
 */
std::unordered_map<std::string, ColumnID> ColumnIdsByColumnNames(const TableColumnDefinitions column_definitions) {
  std::unordered_map<std::string, ColumnID> column_id_by_column_name;
  column_id_by_column_name.reserve(column_definitions.size());
  for (ColumnID i = 0; i < column_definitions.size(); ++i) {
    column_id_by_column_name.emplace(column_definitions[i].name, i);
  }
  return column_id_by_column_name;
}

/**
 * @return the according TpchTable, based on the prefix of @param tpch_column_name.
 */
TpchTable ResolveTpchTable(const std::string& tpch_column_name) {
  switch (tpch_column_name.at(0)) {
    case 'c':
      return TpchTable::kCustomer;
    case 'l':
      return TpchTable::kLineItem;
    case 'o':
      return TpchTable::kOrders;
    case 'p':
      if (tpch_column_name.at(1) == 's') {
        return TpchTable::kPartSupp;
      }
      return TpchTable::kPart;
    case 's':
      return TpchTable::kSupplier;
    default:
      Fail("Could not resolve TpchTable, given the column name '" + tpch_column_name + "'");
  }
}

}  // namespace

std::shared_ptr<ImportOperatorProxy> CreateMockObjectReferences(const std::string& key_prefix, size_t count) {
  std::vector<ObjectReference>& object_references;
  object_references.reserve(count);

  for (size_t i = 0; i < count; ++i) {
    const std::string key = key_prefix + std::to_string(i) + kOrcExtension;
    object_references.emplace_back("mock_bucket", key, "mock_etag");
  }

  return object_references;
}

std::shared_ptr<PqpColumnExpression> TpchPqpColumn(const std::string tpch_column_name) {
  const TpchTable tpch_table = ResolveTpchTable(column_name);

  // Resolve ColumnID
  ColumnID column_id = kInvalidColumnID;
  const TableColumnDefinitions column_definitions = TpchColumnDefinitionsByTable(tpch_table);
  for (ColumnID i = 0; i < column_definitions.size(); ++i) {
    if (column_definitions[i].name != tpch_column_name) {
      continue;
    }
    column_id = i;
    break;
  }
  if (column_id == kInvalidColumnID) {
    Fail("Could not resolve Tpch PQP column definition.");
  }

  // Create PqpColumnExpression
  const auto column_definition = column_definitions[column_id];
  return PqpColumn_(column_id, column_definition.data_type, column_definition.nullable, column_definition.name);
}

std::shared_ptr<ImportOperatorProxy> TpchImportProxy(const std::vector<std::string> column_names,
                                                                const std::vector<ObjectReference> object_references) {
  Assert(!column_names.empty(), "At least one column name must be provided for a TpchTable ImportOperatorProxy.");
  const TpchTable tpch_table = ResolveTpchTable(column_names[0]);

  const TableColumnDefinitions column_definitions = TpchColumnDefinitionsByTable(tpch_table);
  const auto column_id_by_column_name = ColumnIdsByColumnNames(column_definitions);

  // Resolve import ColumnIDs.
  std::vector<ColumnID> import_column_ids;
  for (const std::string& column_name : column_names) {
    const auto column_id_by_column_name_iter = column_id_by_column_name.find(column_name);
    Assert(column_id_by_column_name_iter != column_id_by_column_name.cend(),
           "Could not resolve ColumnID for column '" + column_name + "'");
    ColumnID import_column_id = *column_id_by_column_name_iter;
    Assert(import_column_ids.empty() || import_column_ids.back() < import_column_id,
           "Expected TPC-H table column name order as defined by tpch_data_generator.cpp");
    import_column_ids.push_back(column_id);
  }

  // Create proxy & set TPC-H table name as a comment to support debugging.
  const auto import_proxy = ImportOperatorProxy::Make(object_references, import_column_ids);
  import_proxy->SetComment(std::string(magic_enum::enum_name(tpch_table)));

  return import_proxy;
}

  std::shared_ptr<ExportOperatorProxy> CreateTpchQ1Pqp(size_t lineitem_mock_objects_count, std::vector<size_t> combiner_stages_worker_count) {
    // (1) Define pipeline 1 or pre-aggregation stage for TPC-H Q1
    const auto l_shipdate = TpchPqpColumn("l_shipdate");
    const auto l_quantity = TpchPqpColumn("l_quantity");
    const auto l_extendedprice = TpchPqpColumn("l_extendedprice");
    const auto l_discount = TpchPqpColumn("l_discount");
    const auto l_returnflag = TpchPqpColumn("l_returnflag");
    const auto l_linestatus = TpchPqpColumn("l_linestatus");
    // clang-format off
    const auto l_extendedprice_l_discount = Mul_(l_extendedprice, Sub_(1, l_discount));                               // <=>  l_extendedprice * (1 - l_discount)
    const auto l_extendedprice_l_discount_l_tax = Mul_(l_extendedprice_l_discount, Add_(1, TpchPqpColumn("l_tax")));  // <=> (l_extendedprice * (1 - l_discount)) * (1 + l_tax),

    const auto q1_pre_aggregation_subplan =
    AggregateOperatorProxy::Make(std::vector<ColumnId>{ColumnID{5}, ColumnID{6}}, // Group By l_returnflag, l_linestatus
                                 std::vector<std::shared_ptr<AbstractExpression>>{Sum_(PqpColumnFrom(ColumnID{0}, l_quantity)),
                                                                                  Sum_(PqpColumnFrom(ColumnID{1}, l_extendedprice)),
                                                                                  Sum_(PqpColumnFrom(ColumnID{2}, l_extendedprice_l_discount)),
                                                                                  Sum_(PqpColumnFrom(ColumnID{3}, l_extendedprice_l_discount_l_tax)),
                                                                                  CountStarPqp_(),
                                                                                  Sum_(PqpColumnFrom(ColumnID{4}, l_discount))},
      ProjectionOperatorProxy::Make(ExpressionVector_(l_quantity, l_extendedprice, l_extendedprice_l_discount, l_extendedprice_l_discount_l_tax, l_discount, l_returnflag, l_linestatus),
        FilterOperatorProxy::Make(LessThan_(l_shipdate, "1998-09-02"),
          TpchImportProxy(std::vector<std::string>{"l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate"},
                          CreateMockObjectReferences("lineitem_", lineitem_mock_objects_count)))));

    // (2) Define combiner stages for TPC-H Q1
    const auto get_q1_combine_aggregates_proxy = [&]() {
      return AggregateOperatorProxy::Make(std::vector<ColumnId>{ColumnID{0}, ColumnID{1}}, // Combiner Stage: Group By l_returnflag, l_linestatus & use SUM(*) instead of COUNT(*)
                                          std::vector<std::shared_ptr<AbstractExpression>>{Sum_(PqpColumnFrom(ColumnID{2}, Sum_(l_quantity))),
                                                                                           Sum_(PqpColumnFrom(ColumnID{3}, Sum_(l_extendedprice))),
                                                                                           Sum_(PqpColumnFrom(ColumnID{4}, Sum_(l_extendedprice_l_discount))),
                                                                                           Sum_(PqpColumnFrom(ColumnID{5}, Sum_(l_extendedprice_l_discount_l_tax))),
                                                                                           Sum_(PqpColumnFrom(ColumnID{6}, Sum_(CountStarPqp_()))),
                                                                                           Sum_(PqpColumnFrom(ColumnID{7}, Sum_(l_discount)))});
    };
    // clang-format off

    const auto current_plan = q1_pre_aggregation_subplan;
    for (size_t i = 0; i < combiner_stages_worker_count.size(); ++i) {
      Assert(combiner_stages_worker_count[i] > 1, "The worker count for combiner stages must be greater than one.");
      const auto exchange_proxy = ExchangeOperatorProxy::Make(current_plan);
      exchange_proxy->SetToPartialMerge(combiner_stages_worker_count[i]);
      current_plan = get_q1_combine_aggregates_proxy();
      current_plan->SetLeftInput(exchange_proxy);
    }

    // (3) Define final stage for TPC-H Q1
    const auto exchange_proxy = ExchangeOperatorProxy::Make(current_plan);
    exchange_proxy->SetToFullMerge();
    current_plan = get_q1_combine_aggregates_proxy();
    current_plan->SetLeftInput(exchange_proxy);

    const auto sum_l_quantity = PqpColumnFrom(ColumnID{2}, Sum_(l_quantity));
    const auto sum_l_extended_price = PqpColumnFrom(ColumnID{3}, Sum_(l_extendedprice));
    const auto sum_count_star = PqpColumnFrom(ColumnID{6}, Sum_(CountStarPqp_()));
    const auto sum_l_discount = PqpColumnFrom(ColumnID{7}, Sum_(l_discount));

    // clang-format off
    const auto q1_pqp =
    ExportOperatorProxy::Dummy(
      AliasOperatorProxy::Make(std::vector<ColumnID{}, std::vector<std::string>{},
        SortOperatorProxy::Make(sort_definitions,
          ProjectionOperatorProxy::Make(ExpressionVector_(PqpColumnFrom(ColumnID{0}, l_returnflag),
                                                          PqpColumnFrom(ColumnID{1}, l_linestatus),
                                                          sum_l_quantity,
                                                          sum_l_extended_price,
                                                          PqpColumnFrom(ColumnID{4}, Sum_(l_extendedprice_l_discount)),
                                                          PqpColumnFrom(ColumnID{5}, Sum_(l_extendedprice_l_discount_l_tax)),
                                                          Div_(Cast_(sum_l_quantity, DataType::kDouble), sum_count_star),         // Calculate AVG(l_quantity)
                                                          Div_(Cast_(sum_l_extended_price, DataType::kDouble), sum_count_star),   // Calculate AVG(l_extended_price)
                                                          Div_(Cast_(sum_l_discount, DataType::kDouble), sum_count_star),         // Calculate AVG(l_discount)
                                                          sum_count_star),
            current_plan))));
    // clang-format on

    return q1_pqp;
  }

}  // namespace skyrise
