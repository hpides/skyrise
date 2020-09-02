/*
 * This file demonstrates that tpch-dbgen has been linked successfully and gives an example on how to use the library.
 * It will be replaced by the actual Skyrise benchmark CLI in a later commit.
 */
#include <iostream>

extern "C" {
#include <dss.h>
#include <dsstypes.h>
#include <rnd.h>
}

void test_data_generation() {
  dbgen_reset_seeds();
  dbgen_init_scale_factor(1);
  order_t order;

  row_start(ORDER);
  mk_order(1, &order, 0l);
  row_stop(ORDER);

  // Test if the generated row contains the expected values, given a default seed.
  if (order.okey != 1 || order.custkey != 36901 || order.totalprice != 17366547 || order.orderstatus != 'O') {
    std::cerr << "something went wrong with the data generation." << std::endl;
  }

  dbgen_cleanup();
}

int main([[maybe_unused]] int argc, [[maybe_unused]] char* argv[]) {
  test_data_generation();
  return 0;
}
