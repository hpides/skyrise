/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 * [TPCHBenchmarkItemRunner]
 */
#include "tpch_query_generator.hpp"

extern "C" {
#include <tpch_dbgen.h>
}

#include <iomanip>
#include <random>
#include <sstream>
#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "tpch_queries.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT(google-build-using-namespace)

namespace {

using namespace skyrise;  // NOLINT(google-build-using-namespace)

/**
 * Adds (or subtracts) specified number of months and days.
 */
std::string CalculateDate(boost::gregorian::date date, int months, int days = 0) {
  date = date + boost::gregorian::months(months) + boost::gregorian::days(days);

  std::stringstream output;
  output << static_cast<short>(date.year()) << "-" << std::setw(2) << std::setfill('0')
         << static_cast<short>(date.month()) << "-" << std::setw(2) << std::setfill('0')
         << static_cast<short>(date.day());
  return output.str();
}

/**
 * Fills the "?" placeholders with values and @returns the resulting SQL string.
 */
std::string SubstitutePlaceholders(const BenchmarkItemId item_id, const std::vector<std::string>& parameter_values) {
  // Take the SQL query (from tpch_queries.cpp) and replace one placeholder (question mark) after another
  auto query_template = std::string{tpch_queries.find(item_id + 1)->second};

  for (const auto& parameter_value : parameter_values) {
    boost::replace_first(query_template, "?", parameter_value);
  }

  Assert(query_template.find('?') == std::string::npos, "Unreplaced Placeholder");

  return query_template;
}

}  // namespace

namespace skyrise {

TpchQueryGenerator::TpchQueryGenerator(float scale_factor) : kScaleFactor(scale_factor) {}

std::string TpchQueryGenerator::BuildQuery(const BenchmarkItemId item_id) {
  // Preferring a fast random engine over one with high-quality randomness. Engines are not thread-safe. Since we are
  // fine with them not being synced across threads and object cost is not an issue, we simply use one generator per
  // calling thread.
  static thread_local std::minstd_rand random_engine{random_seed_++};

  // This is not nice, but initializing this statically would require external methods and make it harder to
  // follow in the end. It's not like this list (taken from TPC-H 4.2.2.13) will ever change...
  static const std::vector materials{"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};

  static const auto sizes =
      std::vector{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
                  26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50};

  static const auto country_codes =
      std::vector{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34};

  // Random distributions for all strings defined by the TPC-H benchmark. Each query in Chapter 2.4 has a
  // "Substition Parameters" section. For example, 2.4.1.3 states "DELTA is randomly selected within [60. 120]."
  // For other generation rules, check section 2.4.[query-id].3
  //
  // uniform_int_distributions should not be modified when they are used, but because we have no explicit thread safety
  // guarantee, we make this thread-local, too.

  std::uniform_int_distribution<> material_dist{0, static_cast<int>(materials.size() - 1)};
  std::uniform_int_distribution<> region_dist{0, regions.count - 1};
  std::uniform_int_distribution<> segment_dist{0, c_mseg_set.count - 1};
  std::uniform_int_distribution<> nation_dist{0, nations.count - 1};
  std::uniform_int_distribution<> type_dist{0, p_types_set.count - 1};
  std::uniform_int_distribution<> color_dist{0, colors.count - 1};
  std::uniform_int_distribution<> shipmode_dist{0, l_smode_set.count - 1};
  std::uniform_int_distribution<> brand_char_dist{1, 5};
  std::uniform_int_distribution<> container_dist{0, p_cntr_set.count - 1};

  // Will be filled with the parameters for this query and passed to the next method which builds the query string
  std::vector<std::string> parameters;

  switch (item_id) {
    // Writing `1-1` to make people aware that this is zero-indexed while TPC-H query names are not
    case 1 - 1: {
      std::uniform_int_distribution<> date_diff_dist{60, 120};
      const auto date = CalculateDate(boost::gregorian::date{1998, 12, 01}, 0, -date_diff_dist(random_engine));

      parameters.emplace_back("'"s + date + "'");
      break;
    }

    case 2 - 1: {
      std::uniform_int_distribution<> size_dist{1, 50};
      const auto size = size_dist(random_engine);
      const auto* const material = materials[material_dist(random_engine)];
      const auto* const region = regions.list[region_dist(random_engine)].text;

      parameters.emplace_back(std::to_string(size));
      parameters.emplace_back("'%"s + material + "'");
      parameters.emplace_back("'"s + region + "'");
      parameters.emplace_back("'"s + region + "'");
      break;
    }

    case 3 - 1: {
      const auto* const segment = c_mseg_set.list[segment_dist(random_engine)].text;
      std::uniform_int_distribution<> date_diff_dist{0, 30};
      const auto date = CalculateDate(boost::gregorian::date{1995, 03, 01}, 0, date_diff_dist(random_engine));

      parameters.emplace_back("'"s + segment + "'");
      parameters.emplace_back("'"s + date + "'");
      parameters.emplace_back("'"s + date + "'");
      break;
    }

    case 4 - 1: {
      std::uniform_int_distribution<> date_diff_dist{0, 4 * 12 + 9};
      const auto diff = date_diff_dist(random_engine);
      const auto begin_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff);
      const auto end_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff + 3);

      parameters.emplace_back("'"s + begin_date + "'");
      parameters.emplace_back("'"s + end_date + "'");
      break;
    }

    case 5 - 1: {
      const auto* const region = regions.list[region_dist(random_engine)].text;

      std::uniform_int_distribution<> date_diff_dist{0, 4};
      const auto diff = date_diff_dist(random_engine);
      const auto begin_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff * 12);
      const auto end_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, (diff + 1) * 12);

      parameters.emplace_back("'"s + region + "'");
      parameters.emplace_back("'"s + begin_date + "'");
      parameters.emplace_back("'"s + end_date + "'");
      break;
    }

    case 6 - 1: {
      std::uniform_int_distribution<> date_diff_dist{0, 4};
      const auto diff = date_diff_dist(random_engine);
      const auto begin_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff * 12);
      const auto end_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, (diff + 1) * 12);

      static std::uniform_int_distribution<> discount_dist{2, 9};
      const auto discount = 0.01f * static_cast<float>(discount_dist(random_engine));

      std::uniform_int_distribution<> quantity_dist{24, 25};
      const auto quantity = quantity_dist(random_engine);

      parameters.emplace_back("'"s + begin_date + "'");
      parameters.emplace_back("'"s + end_date + "'");
      parameters.emplace_back(std::to_string(discount));
      parameters.emplace_back(std::to_string(discount));
      parameters.emplace_back(std::to_string(quantity));
      break;
    }

    case 7 - 1: {
      const auto* const nation1 = nations.list[nation_dist(random_engine)].text;
      auto nation2 = std::string{};
      do {
        nation2 = nations.list[nation_dist(random_engine)].text;
      } while (nation1 == nation2);

      parameters.emplace_back("'"s + nation1 + "'");
      parameters.emplace_back("'"s + nation2 + "'");
      parameters.emplace_back("'"s + nation2 + "'");
      parameters.emplace_back("'"s + nation1 + "'");

      // Hard-coded in TPC-H, but used in JCC-H
      parameters.emplace_back("'1995-01-01'");
      parameters.emplace_back("'1996-12-31'");

      break;
    }

    case 8 - 1: {
      const auto nation_id = nation_dist(random_engine);
      const auto* const nation = nations.list[nation_id].text;

      // No idea why the field is called "weight", but it corresponds to the region of a given nation
      const auto* const region = regions.list[nations.list[nation_id].weight].text;

      const auto* const type = p_types_set.list[type_dist(random_engine)].text;

      parameters.emplace_back("'"s + nation + "'");
      parameters.emplace_back("'"s + region + "'");

      // Hard-coded in TPC-H, but used in JCC-H
      parameters.emplace_back("'1995-01-01'");
      parameters.emplace_back("'1996-12-31'");

      parameters.emplace_back("'"s + type + "'");

      break;
    }

    case 9 - 1: {
      const auto* const color = colors.list[color_dist(random_engine)].text;

      parameters.emplace_back("'%"s + color + "%'");
      break;
    }

    case 10 - 1: {
      std::uniform_int_distribution<> date_diff_dist{0, 23};
      const auto diff = date_diff_dist(random_engine);
      const auto begin_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff);
      const auto end_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, (diff + 3));

      parameters.emplace_back("'"s + begin_date + "'");
      parameters.emplace_back("'"s + end_date + "'");
      break;
    }

    case 11 - 1: {
      const auto* const nation = nations.list[nation_dist(random_engine)].text;
      const auto fraction = 0.0001 / (kScaleFactor > 0 ? kScaleFactor : 1);

      parameters.emplace_back("'"s + nation + "'");
      parameters.emplace_back(std::to_string(fraction));
      parameters.emplace_back("'"s + nation + "'");
      break;
    }

    case 12 - 1: {
      const auto* const shipmode1 = l_smode_set.list[shipmode_dist(random_engine)].text;
      std::string shipmode2;
      do {
        shipmode2 = l_smode_set.list[shipmode_dist(random_engine)].text;
      } while (shipmode1 == shipmode2);

      std::uniform_int_distribution<> date_diff_dist{0, 4};
      const auto diff = date_diff_dist(random_engine);
      const auto begin_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff * 12);
      const auto end_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, (diff + 1) * 12);

      parameters.emplace_back("'"s + shipmode1 + "'");
      parameters.emplace_back("'"s + shipmode2 + "'");
      parameters.emplace_back("'"s + begin_date + "'");
      parameters.emplace_back("'"s + end_date + "'");
      break;
    }

    case 13 - 1: {
      const auto words1 = std::vector{"special", "pending", "unusual", "express"};
      const auto words2 = std::vector{"packages", "requests", "accounts", "deposits"};

      std::uniform_int_distribution<> word_dist{0, 3};

      parameters.emplace_back("'%"s + words1[word_dist(random_engine)] + '%' + words2[word_dist(random_engine)] + "%'");
      break;
    }

    case 14 - 1: {
      std::uniform_int_distribution<> date_diff_dist{0, 5 * 12};
      const auto diff = date_diff_dist(random_engine);
      const auto begin_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff);
      const auto end_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff + 1);

      parameters.emplace_back("'"s + begin_date + "'");
      parameters.emplace_back("'"s + end_date + "'");
      break;
    }

    case 15 - 1: {
      auto query_15 = std::string{tpch_queries.at(15)};

      std::uniform_int_distribution<> date_diff_dist{0, 4 * 12 + 9};
      const auto diff = date_diff_dist(random_engine);
      const auto begin_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff);
      const auto end_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff + 3);

      // Hack: We cannot use prepared statements in TPC-H 15. Thus, we need to build the SQL string by hand.
      // By manually replacing the `?` from tpch_queries.cpp, we can keep all queries in a readable form there.
      // This is ugly, but at least we can assert that nobody tampered with the string over there.
      static constexpr auto BEGIN_DATE_OFFSET = 156;
      static constexpr auto END_DATE_OFFSET = 192;
      DebugAssert((std::string_view{&query_15[BEGIN_DATE_OFFSET], 10} == "1996-01-01" &&
                   std::string_view{&query_15[END_DATE_OFFSET], 10} == "1996-04-01"),
                  "TPC-H 15 string has been modified");
      query_15.replace(BEGIN_DATE_OFFSET, 10, begin_date);
      query_15.replace(END_DATE_OFFSET, 10, end_date);

      const auto view_id = std::atomic_fetch_add(&q15_view_id_, size_t{1});
      boost::replace_all(query_15, std::string("revenue_view"), std::string("revenue") + std::to_string(view_id));

      // Not using SubstitutePlaceholders here
      return query_15;
    }

    case 16 - 1: {
      const auto brand = brand_char_dist(random_engine) * 10 + brand_char_dist(random_engine);

      const auto full_type = std::string{p_types_set.list[type_dist(random_engine)].text};
      const auto partial_type = std::string(full_type, 0, full_type.find_last_of(' '));

      auto sizes_copy = sizes;
      std::shuffle(sizes_copy.begin(), sizes_copy.end(), random_engine);

      parameters.emplace_back("'Brand#"s + std::to_string(brand) + "'");
      parameters.emplace_back("'"s + partial_type + "%'");
      for (auto i = 0; i < 8; ++i) parameters.emplace_back(std::to_string(sizes_copy[i]));
      break;
    }

    case 17 - 1: {
      const auto brand = brand_char_dist(random_engine) * 10 + brand_char_dist(random_engine);
      const auto* const container = p_cntr_set.list[container_dist(random_engine)].text;

      parameters.emplace_back("'Brand#"s + std::to_string(brand) + "'");
      parameters.emplace_back("'"s + container + "'");
      break;
    }

    case 18 - 1: {
      std::uniform_int_distribution<> quantity_dist{312, 315};
      const auto quantity = quantity_dist(random_engine);

      parameters.emplace_back(std::to_string(quantity));
      break;
    }

    case 19 - 1: {
      std::uniform_int_distribution<> quantity1_dist{1, 10};
      std::uniform_int_distribution<> quantity2_dist{10, 20};
      std::uniform_int_distribution<> quantity3_dist{20, 30};
      const auto quantity1 = quantity1_dist(random_engine);
      const auto quantity2 = quantity2_dist(random_engine);
      const auto quantity3 = quantity3_dist(random_engine);
      const auto brand1 = brand_char_dist(random_engine) * 10 + brand_char_dist(random_engine);
      const auto brand2 = brand_char_dist(random_engine) * 10 + brand_char_dist(random_engine);
      const auto brand3 = brand_char_dist(random_engine) * 10 + brand_char_dist(random_engine);

      parameters.emplace_back("'Brand#" + std::to_string(brand1) + "'");
      parameters.emplace_back(std::to_string(quantity1));
      parameters.emplace_back(std::to_string(quantity1));
      parameters.emplace_back("'Brand#" + std::to_string(brand2) + "'");
      parameters.emplace_back(std::to_string(quantity2));
      parameters.emplace_back(std::to_string(quantity2));
      parameters.emplace_back("'Brand#" + std::to_string(brand3) + "'");
      parameters.emplace_back(std::to_string(quantity3));
      parameters.emplace_back(std::to_string(quantity3));

      break;
    }

    case 20 - 1: {
      const auto* const color = colors.list[color_dist(random_engine)].text;
      std::uniform_int_distribution<> date_diff_dist{0, 4};
      const auto diff = date_diff_dist(random_engine);
      const auto begin_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, diff * 12);
      const auto end_date = CalculateDate(boost::gregorian::date{1993, 01, 01}, (diff + 1) * 12);
      const auto* const nation = nations.list[nation_dist(random_engine)].text;

      parameters.emplace_back("'"s + color + "%'");
      parameters.emplace_back("'"s + begin_date + "'");
      parameters.emplace_back("'"s + end_date + "'");
      parameters.emplace_back("'"s + nation + "'");
      break;
    }

    case 21 - 1: {
      const auto* const nation = nations.list[nation_dist(random_engine)].text;

      parameters.emplace_back("'"s + nation + "'");
      break;
    }

    case 22 - 1: {
      auto country_codes_copy = country_codes;
      std::shuffle(country_codes_copy.begin(), country_codes_copy.end(), random_engine);

      // We need the same country code twice - have a look at the query
      for (auto i = 0; i < 7; ++i) parameters.emplace_back("'"s + std::to_string(country_codes_copy[i]) + "'");
      for (auto i = 0; i < 7; ++i) parameters.emplace_back("'"s + std::to_string(country_codes_copy[i]) + "'");
      break;
    }

    default:
      Fail("There are only 22 TPC-H queries");
  }

  return SubstitutePlaceholders(item_id, parameters);
}

std::string TpchQueryGenerator::BuildDeterministicQuery(const BenchmarkItemId item_id) {
  DebugAssert(item_id < 22, "There are only 22 TPC-H queries");

  if (item_id + 1 == 15) {
    // Generating TPC-H Query 15 by hand
    auto query_15 = std::string{tpch_queries.find(15)->second};

    // TPC-H query 15 uses "stream ids" to name the views. While not supported right now, we might want to execute
    // multiple instances of Q15 simultaneously and will need unique view names for that.
    static auto view_id = 0;
    boost::replace_all(query_15, std::string("revenueview"), std::string("revenue") + std::to_string(view_id++));
    return query_15;
  }

  // Stores how the parameters (the ? in the query) should be replaced. These values are examples for the queries. Most
  // of them use the verification parameters given in the TPC-H specification for the respective query. A few are
  // modified so that we get results even for a small scale factor.
  static std::vector<std::vector<std::string>> parameter_values = {
      {"'1998-09-02'"},
      {"15", "'%BRASS'", "'EUROPE'", "'EUROPE'"},
      {"'BUILDING'", "'1995-03-15'", "'1995-03-15'"},
      {"'1993-07-01'", "'1993-10-01'"},
      {"'ASIA'", "'1994-01-01'", "'1995-01-01'"},
      {"'1994-01-01'", "'1995-01-01'", ".06", ".06", "24"},
      {"'FRANCE'", "'GERMANY'", "'GERMANY'", "'FRANCE'", "'1995-01-01'", "'1996-12-31'"},
      {"'BRAZIL'", "'AMERICA'", "'1995-01-01'", "'1996-12-31'", "'ECONOMY ANODIZED STEEL'"},
      {"'%green%'"},
      {"'1993-10-01'", "'1994-01-01'"},
      {"'GERMANY'", "0.0001", "'GERMANY'"},
      {"'MAIL'", "'SHIP'", "'1994-01-01'", "'1995-01-01'"},
      {"'%special%requests%'"},
      {"'1995-09-01'", "'1995-10-01'"},
      {},  // Handled above
      {"'Brand#45'", "'MEDIUM POLISHED%'", "49", "14", "23", "45", "19", "3", "36", "9"},
      {"'Brand#23'", "'MED BOX'"},
      {"300"},
      {"'Brand#12'", "1", "1", "'Brand#23'", "10", "10", "'Brand#34'", "20", "20"},
      {"'forest%'", "'1994-01-01'", "'1995-01-01'", "'CANADA'"},
      {"'SAUDI ARABIA'"},
      {"'13'", "'31'", "'23'", "'29'", "'30'", "'18'", "'17'", "'13'", "'31'", "'23'", "'29'", "'30'", "'18'", "'17'"}};

  return SubstitutePlaceholders(item_id, parameter_values[item_id]);
}

}  // namespace skyrise