#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <rclcpp/rclcpp.hpp>

namespace let_ros
{

enum class Engine { Sequential, Barrier, OutOfOrder, Tag, Frontier, Static };

enum class Model { Streams, Calendar };

struct JobCosts
{
  std::map<std::string, std::vector<int64_t>> ns;
  void save(const std::string & path) const;
  static JobCosts load(const std::string & path);
};

enum class CostModel { Job, Callback };

struct Options
{
  Engine engine = Engine::OutOfOrder;
  Model model = Model::Streams;
  int64_t max_streams = 200000;
  int workers = 1;
  int64_t horizon_ns = 1000000000LL;
  std::map<std::string, int64_t> deadline_ns;
  int64_t default_deadline_ns = -1;
  double d_scale = 1.0;
  std::map<std::string, int64_t> link_delay_ns;
  int64_t default_link_delay_ns = 0;
  int64_t random_link_delay_max_ns = 0;
  uint64_t random_link_delay_seed = 0;
  bool timer_phase_zero = false;
  std::map<std::string, int64_t> timer_offset_ns;
  int min_ring = 2;
  bool pin_threads = false;
  bool verbose = false;
  bool initialize_components = false;
  std::vector<std::string> required_output_topics;
  std::string trace_path;
  std::map<std::string, std::map<std::string, std::set<std::string>>> publish_sets;
  std::string record_publish_sets_path;

  int64_t window_ns = 0;
  bool ring_inf = false;
  int ring_scale = 1;
  int64_t lead_ns = -1;
  int sched = 0;
  std::shared_ptr<JobCosts> costs;
  bool record_costs = false;
  CostModel cost_model = CostModel::Job;
  std::string costs_source;
};

struct Result
{
  std::string engine;
  std::string model;
  int64_t streams = 0;
  int64_t ring_slots = 0;
  int workers = 0;
  int64_t horizon_ns = 0;
  size_t tasks = 0;
  size_t channels = 0;
  int64_t jobs = 0;
  int64_t active_jobs = 0;
  int64_t timer_callbacks = 0;
  int64_t sub_callbacks = 0;
  int64_t messages = 0;
  int64_t window_ns = 0;
  int64_t windows = 0;
  bool ring_inf = false;
  int ring_scale = 1;
  double d_scale = 1.0;
  int64_t max_link_delay_ns = 0;
  int64_t delayed_channels = 0;
  int64_t payload_bytes = 0;
  int64_t nulls = 0;
  int64_t lead_ns = 0;
  int sched = 0;
  double est_s = 0.0;
  std::string cost_model;
  std::string costs_source;
  int cores = 0;
  bool pinned = false;
  double setup_s = 0.0;
  double run_s = 0.0;
  double node_setup_s = 0.0;
  double rss_before_mb = -1.0;
  double rss_peak_mb = -1.0;
  uint64_t hash = 0;
  std::string to_json() const;
};

class Simulator
{
public:
  explicit Simulator(Options options);
  ~Simulator();

  void add_node(rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node,
    std::shared_ptr<void> keep_alive = nullptr);
  void add_node(const rclcpp::Node::SharedPtr & node);

  Result run();

  void describe(std::ostream & os) const;

  struct Impl;

private:
  std::unique_ptr<Impl> impl_;
};

std::map<std::string, int64_t> load_deadlines(const std::string & path);
std::map<std::string, int64_t> load_link_delays(const std::string & path);
int64_t parse_duration_ns(const std::string & text);
std::map<std::string, int64_t> load_offsets(const std::string & path);
std::map<std::string, std::map<std::string, std::set<std::string>>>
load_publish_sets(const std::string & path);

}
