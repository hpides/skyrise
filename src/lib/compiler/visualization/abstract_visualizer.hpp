/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

// By defining BOOST_MULTI_INDEX_DISABLE_SERIALIZATION, a smaller number of Boost libraries is required.
#define BOOST_MULTI_INDEX_DISABLE_SERIALIZATION
#include <boost/algorithm/string.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graphviz.hpp>

namespace skyrise {

// All graphviz options, e.g. color, shape, format, can be looked up at
// http://www.graphviz.org/doc/info/attrs.html
// We do not want to create constants here because they would be rather restrictive compared to all possible options
// defined by graphviz.
struct GraphvizConfig {
  std::string renderer = "dot";
  std::string format = "png";
};

struct VizGraphInfo {
  std::string bg_color = "white";
  std::string font_name = "Helvetica";
  std::string font_color = "black";
  std::string rankdir = "BT";  // Bottom to top
  std::string ratio = "compress";
  std::string label = "";
  std::string label_location = "t";
  std::string label_justification = "l";
};

struct VizVertexInfo {
  uintptr_t id;
  std::string label;
  std::string tooltip;
  std::string color = "black";
  std::string font_color = "black";
  std::string shape = "rectangle";
  std::string margin = "0.1,0.1";
  double pen_width = 1.0;
};

struct VizEdgeInfo {
  std::string label;
  std::string label_tooltip;
  std::string color = "black";
  std::string font_color = "black";
  double pen_width = 1.0;
  std::string dir = "forward";
  std::string style = "solid";
  std::string arrowhead = "normal";
};

// Custom facet for creating a custom locale with thousands separator.
struct SeparateThousandsFacet : std::numpunct<char> {
  string_type do_grouping() const override { return "\3"; }  // groups of 3 digits
};

template <typename GraphBase>
class AbstractVisualizer {
  //                                  Edge list    Vertex list   Directed graph
  using Graph = boost::adjacency_list<boost::vecS, boost::vecS, boost::directedS,
                                      // Vertex info Edge info    Graph info
                                      VizVertexInfo, VizEdgeInfo, VizGraphInfo>;

  // No label in a node should be wider than this many characters. If it is longer, line breaks should be added.
  static const uint8_t MAX_LABEL_WIDTH = 50;

 public:
  enum class InputSide { Left, Right };

  AbstractVisualizer() : AbstractVisualizer(GraphvizConfig{}, VizGraphInfo{}, VizVertexInfo{}, VizEdgeInfo{}) {}

  AbstractVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                     VizEdgeInfo edge_info)
      : graphviz_config_(std::move(graphviz_config)),
        graph_info_(std::move(graph_info)),
        default_vertex_(std::move(vertex_info)),
        default_edge_(std::move(edge_info)) {
    // Add global Graph properties
    AddGraphProperty("rankdir", graph_info_.rankdir);
    AddGraphProperty("fontcolor", graph_info_.font_color);
    AddGraphProperty("fontname", graph_info_.font_name);
    AddGraphProperty("bgcolor", graph_info_.bg_color);
    AddGraphProperty("ratio", graph_info_.ratio);
    AddGraphProperty("label", graph_info_.label);
    AddGraphProperty("labelloc", graph_info_.label_location);
    AddGraphProperty("labeljust", graph_info_.label_justification);

    // Add vertex properties
    AddProperty("node_id", &VizVertexInfo::id);
    AddProperty("label", &VizVertexInfo::label);
    AddProperty("tooltip", &VizVertexInfo::tooltip);
    AddProperty("color", &VizVertexInfo::color);
    AddProperty("fontcolor", &VizVertexInfo::font_color);
    AddProperty("shape", &VizVertexInfo::shape);
    AddProperty("margin", &VizVertexInfo::margin);
    AddProperty("penwidth", &VizVertexInfo::pen_width);

    // Add edge properties
    AddProperty("color", &VizEdgeInfo::color);
    AddProperty("fontcolor", &VizEdgeInfo::font_color);
    AddProperty("label", &VizEdgeInfo::label);
    AddProperty("penwidth", &VizEdgeInfo::pen_width);
    AddProperty("style", &VizEdgeInfo::style);
    AddProperty("dir", &VizEdgeInfo::dir);
    AddProperty("arrowhead", &VizEdgeInfo::arrowhead);
    AddProperty("labeltooltip", &VizEdgeInfo::label_tooltip);
  }

  virtual ~AbstractVisualizer() = default;

  void Visualize(const GraphBase& graph_base, const std::string& img_filename) {
    BuildGraph(graph_base);

    char* tmpname = strdup("/tmp/skyrise_viz_XXXXXX");
    auto file_descriptor = mkstemp(tmpname);
    Assert(file_descriptor > 0, "mkstemp failed");

    // mkstemp returns a file descriptor. Unfortunately, we cannot directly create an ofstream from a file descriptor.
    close(file_descriptor);
    std::ofstream file(tmpname);

    // This unique_ptr serves as a scope guard that guarantees the deletion of the temp file once we return from this
    // method.
    const auto delete_temp_file = [&tmpname](auto ptr) {
      delete ptr;
      std::remove(tmpname);
    };
    const auto delete_guard = std::unique_ptr<char, decltype(delete_temp_file)>(new char, delete_temp_file);

    // The caller set the pen widths to either the number of rows (for edges) or the execution time in ns (for
    // vertices). As some plans have only operators that take microseconds and others take minutes, normalize this
    // so that the thickest pen has a width of max_normalized_width and the thinnest one has a width of 1. Using
    // a logarithm makes the operators that follow the most expensive one more visible. Not sure if this is what
    // statisticians would do, but it makes for beautiful images.
    const auto normalize_penwidths = [&](auto iter_pair) {
      const auto max_normalized_width = 8.0;
      const auto log_base = std::log(1.5);
      double max_unnormalized_width = 0.0;
      // False positive with gcc and tsan (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=92194)
      //#pragma GCC diagnostic push
      //#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
      for (auto iter = iter_pair.first; iter != iter_pair.second; ++iter) {
        max_unnormalized_width = std::max(max_unnormalized_width, std::log(graph_[*iter].pen_width) / log_base);
      }

      double offset = max_unnormalized_width - (max_normalized_width - 1.0);

      for (auto iter = iter_pair.first; iter != iter_pair.second; ++iter) {
        auto& pen_width = graph_[*iter].pen_width;
        if (max_unnormalized_width == 0.0) {
          // All widths are the same, set pen width to 1
          pen_width = 1.0;
        } else {
          // Set normalized pen width
          pen_width = 1.0 + std::max(0.0, std::log(pen_width) / log_base - offset);
        }
      }
      // #pragma GCC diagnostic pop
    };
    normalize_penwidths(boost::vertices(graph_));
    normalize_penwidths(boost::edges(graph_));

    // TODO(julianmenzler): dot_output_file for debugging -- remove if no longer needed
    //    std::ofstream dot_output_file (img_filename + ".txt");
    //    boost::write_graphviz_dp(dot_output_file, graph_, properties_);

    boost::write_graphviz_dp(file, graph_, properties_);

    auto renderer = graphviz_config_.renderer;
    auto format = graphviz_config_.format;

    auto cmd = renderer + " -T" + format + " \"" + tmpname + "\" > \"" + img_filename + "\"";
    auto ret = system(cmd.c_str());

    Assert(ret == 0, "Calling graphviz' " + renderer +
                         " failed. Have you installed graphviz "
                         "(apt-get install graphviz / brew install graphviz)?");
    // We do not want to make graphviz a requirement for Skyrise as visualization is just a gimmick
  }

 protected:
  virtual void BuildGraph(const GraphBase& graph_base) = 0;

  template <typename T>
  static uintptr_t GetId(const T& v) {
    return reinterpret_cast<uintptr_t>(&v);
  }

  template <typename T>
  static uintptr_t GetId(const std::shared_ptr<T>& v) {
    return reinterpret_cast<uintptr_t>(v.get());
  }

  enum class WrapLabel { On, Off };

  template <typename T>
  void AddVertex(const T& vertex, const std::string& label = "", const WrapLabel wrap_label = WrapLabel::On) {
    VizVertexInfo info = default_vertex_;
    info.id = GetId(vertex);
    info.label = label;
    AddVertex(vertex, info, wrap_label);
  }

  template <typename T>
  void AddVertex(const T& vertex, VizVertexInfo& vertex_info, const WrapLabel wrap_label = WrapLabel::On) {
    auto vertex_id = GetId(vertex);
    auto inserted = id_to_position_.insert({vertex_id, id_to_position_.size()}).second;
    if (!inserted) {
      // Vertex already exists, do nothing
      return;
    }

    vertex_info.id = vertex_id;
    if (wrap_label == WrapLabel::On) vertex_info.label = WrapLabel(vertex_info.label);
    boost::add_vertex(vertex_info, graph_);
  }

  template <typename T, typename K>
  void AddEdge(const T& from, const K& to) {
    AddEdge(from, to, default_edge_);
  }

  template <typename T, typename K>
  void AddEdge(const T& from, const K& to, const VizEdgeInfo& edge_info) {
    auto from_id = GetId(from);
    auto to_id = GetId(to);

    auto from_pos = id_to_position_.at(from_id);
    auto to_pos = id_to_position_.at(to_id);

    boost::add_edge(from_pos, to_pos, edge_info, graph_);
  }

  template <typename T>
  void AddGraphProperty(const std::string& property_name, const T& value) {
    // Use this to add a global property to the graph. This results in a config line in the graph file:
    // property_name=value;
    properties_.property(property_name, boost::make_constant_property<Graph*>(value));
  }

  template <typename T>
  void AddProperty(const std::string& property_name, const T& value) {
    // Use this to add a property that is read from each vertex/edge (depending on the value). This will result in:
    // <node_id> [..., property_name=value, ...];
    properties_.property(property_name, boost::get(value, graph_));
  }

  std::string WrapLabel(const std::string& label) {
    if (label.length() <= MAX_LABEL_WIDTH) return label;
    std::stringstream label_stream;

    // 1. Split label into lines
    std::vector<std::string> lines;
    boost::split(lines, label, boost::is_any_of("\n"));

    for (size_t line_idx = 0; line_idx < lines.size(); ++line_idx) {
      if (line_idx > 0) label_stream << '\n';
      const auto& line = lines.at(line_idx);
      if (line.length() <= MAX_LABEL_WIDTH) {
        label_stream << line;
        continue;
      }
      // 2. Split line into words, so we don't break a line in the middle of a word
      std::vector<std::string> line_words;
      boost::split(line_words, line, boost::is_any_of(" "));
      size_t line_length = 0;
      size_t word_idx = 0;
      while (true) {
        label_stream << line_words.at(word_idx);
        line_length += line_words.at(word_idx).length();

        // Exit on last word
        if (word_idx == line_words.size() - 1) break;

        line_length++;  // include whitespace
        size_t next_line_length = line_length + line_words.at(++word_idx).length();
        if (next_line_length < MAX_LABEL_WIDTH) {
          label_stream << ' ';
        } else {
          label_stream << '\n';
          line_length = 0;
        }
      }
    }

    return label_stream.str();
  }

  std::string RandomColor() {
    // Favor a hand picked list of nice-to-look-at colors over random generation for now.
    static std::vector<std::string> colors(
        {"#008A2A", "#005FAF", "#5F7E7E", "#9C2F2F", "#A0666C", "#9F9F00", "#9FC0CB", "#9F4C00", "#AF00AF"});

    random_color_index_ = (random_color_index_ + 1) % colors.size();
    return colors[random_color_index_];
  }

  Graph graph_;
  std::unordered_map<uintptr_t, uint16_t> id_to_position_;
  boost::dynamic_properties properties_;

  GraphvizConfig graphviz_config_;
  VizGraphInfo graph_info_;
  VizVertexInfo default_vertex_;
  VizEdgeInfo default_edge_;

  // Current index of color in RandomColor()
  size_t random_color_index_{0};
};

}  // namespace skyrise
