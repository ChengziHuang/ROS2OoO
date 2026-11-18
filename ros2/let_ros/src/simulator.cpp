#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

#include <rcl/publisher.h>
#include <rcl/timer.h>
#include <rmw/rmw.h>
#include <rmw/types.h>
#include <rclcpp/experimental/intra_process_manager.hpp>
#include <rclcpp/serialized_message.hpp>
#include <rcutils/error_handling.h>

#include "sim_impl.hpp"

namespace let_ros
{

namespace
{

const char * engine_name(Engine e)
{
  switch (e) {
    case Engine::Sequential: return "seq";
    case Engine::Barrier: return "barrier";
    case Engine::OutOfOrder: return "ooo";
    case Engine::Tag: return "tag";
    case Engine::Frontier: return "frontier";
    case Engine::Static: return "static";
  }
  return "?";
}

const char * model_name(Model m)
{
  return m == Model::Streams ? "streams" : "calendar";
}

}

std::string Result::to_json() const
{
  std::ostringstream o;
  o << "{\"engine\":\"" << engine << "\",\"model\":\"" << model << "\",\"workers\":" << workers
    << ",\"horizon_ns\":" << horizon_ns << ",\"tasks\":" << tasks
    << ",\"channels\":" << channels << ",\"streams\":" << streams << ",\"jobs\":" << jobs
    << ",\"active_jobs\":" << active_jobs << ",\"timer_callbacks\":" << timer_callbacks
    << ",\"sub_callbacks\":" << sub_callbacks
    << ",\"callbacks\":" << timer_callbacks + sub_callbacks << ",\"messages\":" << messages
    << ",\"ring_slots\":" << ring_slots
    << ",\"window_ns\":" << window_ns << ",\"windows\":" << windows
    << ",\"ring_inf\":" << (ring_inf ? "true" : "false") << ",\"ring_scale\":" << ring_scale
    << ",\"d_scale\":" << d_scale
    << ",\"max_link_delay_ns\":" << max_link_delay_ns << ",\"delayed_channels\":" << delayed_channels
    << ",\"payload_bytes\":" << payload_bytes << ",\"nulls\":" << nulls
    << ",\"lead_ns\":" << lead_ns << ",\"sched\":" << sched
    << ",\"est_s\":" << std::setprecision(6) << est_s;
  if (!cost_model.empty()) {
    o << ",\"cost_model\":\"" << cost_model << "\",\"costs_source\":\"" << costs_source << "\"";
  }
  o << ",\"cores\":" << cores << ",\"pinned\":" << (pinned ? "true" : "false")
    << ",\"setup_s\":" << std::setprecision(6) << setup_s
    << ",\"run_s\":" << std::setprecision(6) << run_s
    << ",\"node_setup_s\":" << node_setup_s
    << ",\"total_s\":" << node_setup_s + setup_s + run_s;
  if (rss_peak_mb >= 0) {
    o << ",\"rss_before_mb\":" << std::setprecision(6) << rss_before_mb
      << ",\"rss_peak_mb\":" << rss_peak_mb;
  }
  o << ",\"hash\":\"" << std::hex << std::setw(16) << std::setfill('0') << hash << "\"}";
  return o.str();
}

int64_t parse_duration_ns(const std::string & text)
{
  std::string s;
  for (char c : text) {
    if (!std::isspace(static_cast<unsigned char>(c))) {
      s += c;
    }
  }
  size_t pos = 0;
  double v = 0.0;
  try {
    v = std::stod(s, &pos);
  } catch (const std::exception &) {
    throw std::runtime_error("invalid duration '" + text + "'");
  }
  std::string unit = s.substr(pos);
  double scale = 1e6;
  if (unit == "s") {
    scale = 1e9;
  } else if (unit == "ms" || unit.empty()) {
    scale = 1e6;
  } else if (unit == "us") {
    scale = 1e3;
  } else if (unit == "ns") {
    scale = 1.0;
  } else {
    throw std::runtime_error("unknown time unit in '" + text + "'");
  }
  double ns = std::round(v * scale);
  if (!std::isfinite(ns) || std::fabs(ns) > 9.0e18) {
    throw std::runtime_error("duration out of range '" + text + "'");
  }
  return static_cast<int64_t>(ns);
}

void JobCosts::save(const std::string & path) const
{
  std::ofstream out(path);
  if (!out) {
    throw std::runtime_error("cannot write " + path);
  }
  out << "# <task> <jobs> <cost of each job in ns, in job order>\n";
  for (auto & kv : ns) {
    out << kv.first << " " << kv.second.size();
    for (int64_t c : kv.second) {
      out << " " << c;
    }
    out << "\n";
  }
}

JobCosts JobCosts::load(const std::string & path)
{
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("cannot open job cost file " + path);
  }
  JobCosts c;
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() || line[0] == '#') {
      continue;
    }
    std::istringstream ss(line);
    std::string task;
    size_t n = 0;
    if (!(ss >> task >> n)) {
      throw std::runtime_error("bad line in " + path);
    }
    std::vector<int64_t> v(n);
    for (size_t x = 0; x < n; ++x) {
      if (!(ss >> v[x]) || v[x] < 0) {
        throw std::runtime_error("bad cost of " + task + " in " + path);
      }
    }
    c.ns[task] = std::move(v);
  }
  return c;
}

std::map<std::string, int64_t> load_deadlines(const std::string & path)
{
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("cannot open deadline file " + path);
  }
  std::map<std::string, int64_t> out;
  std::string line;
  while (std::getline(in, line)) {
    auto hash = line.find('#');
    if (hash != std::string::npos) {
      line = line.substr(0, hash);
    }
    auto colon = line.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    std::string name = line.substr(0, colon);
    name.erase(0, name.find_first_not_of(" \t"));
    name.erase(name.find_last_not_of(" \t") + 1);
    if (name.empty()) {
      continue;
    }
    int64_t d = parse_duration_ns(line.substr(colon + 1));
    if (d <= 0) {
      throw std::runtime_error("deadline of " + name + " in " + path + " must be positive");
    }
    out[name] = d;
  }
  return out;
}

std::map<std::string, int64_t> load_link_delays(const std::string & path)
{
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("cannot open link delay file " + path);
  }
  std::map<std::string, int64_t> out;
  std::string line;
  size_t lineno = 0;
  while (std::getline(in, line)) {
    ++lineno;
    auto hash = line.find('#');
    if (hash != std::string::npos) {
      line = line.substr(0, hash);
    }
    if (line.find_first_not_of(" \t\r\n") == std::string::npos) {
      continue;
    }
    auto colon = line.rfind(':');
    if (colon == std::string::npos) {
      throw std::runtime_error("missing ':' in link delay file " + path +
              " at line " + std::to_string(lineno));
    }
    std::istringstream ss(line.substr(0, colon));
    std::vector<std::string> words;
    std::string w;
    while (ss >> w) {
      words.push_back(w);
    }
    if (words.empty() || words.size() > 2 || words[0].front() != '/') {
      throw std::runtime_error("link delay line '" + line + "' in " + path +
              " must be '<topic>: <delay>' or '<topic> <node>: <delay>' with an absolute topic");
    }
    std::string key = words.size() == 1 ? words[0] : words[0] + " " + words[1];
    int64_t d = parse_duration_ns(line.substr(colon + 1));
    if (d < 0) {
      throw std::runtime_error("link delay of " + key + " in " + path + " must not be negative");
    }
    out[key] = d;
  }
  return out;
}

std::map<std::string, int64_t> load_offsets(const std::string & path)
{
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("cannot open offset file " + path);
  }
  std::map<std::string, int64_t> out;
  std::string line;
  while (std::getline(in, line)) {
    auto hash = line.find('#');
    if (hash != std::string::npos) {
      line = line.substr(0, hash);
    }
    auto colon = line.rfind(':');
    if (colon == std::string::npos) {
      continue;
    }
    std::string prefix = line.substr(0, colon);
    prefix.erase(0, prefix.find_first_not_of(" \t"));
    prefix.erase(prefix.find_last_not_of(" \t") + 1);
    if (prefix.empty()) {
      continue;
    }
    int64_t d = parse_duration_ns(line.substr(colon + 1));
    if (d < 0) {
      throw std::runtime_error("offset of " + prefix + " in " + path + " must not be negative");
    }
    out[prefix] = d;
  }
  return out;
}

std::map<std::string, std::map<std::string, std::set<std::string>>>
load_publish_sets(const std::string & path)
{
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("cannot open publish set file " + path);
  }
  std::map<std::string, std::map<std::string, std::set<std::string>>> out;
  std::string line;
  while (std::getline(in, line)) {
    auto hash = line.find('#');
    if (hash != std::string::npos) {
      line = line.substr(0, hash);
    }
    std::istringstream ss(line);
    std::string node;
    std::string source;
    if (!(ss >> node >> source)) {
      continue;
    }
    auto & topics = out[node][source];
    std::string topic;
    while (ss >> topic) {
      if (topic != "-") {
        topics.insert(topic);
      }
    }
  }
  return out;
}

Simulator::Simulator(Options options)
: impl_(std::make_unique<Impl>())
{
  auto bad = [](const std::string & what) {
      throw std::invalid_argument("let_ros: " + what);
    };
  if (options.workers < 1) {
    bad("workers must be at least 1");
  }
  if (options.horizon_ns <= 0) {
    bad("the horizon must be positive");
  }
  if (options.max_streams < 1) {
    bad("max_streams must be at least 1");
  }
  if (options.min_ring < 1) {
    bad("min_ring must be at least 1");
  }
  if (options.ring_scale < 1) {
    bad("ring_scale must be at least 1");
  }
  for (auto & kv : options.deadline_ns) {
    if (kv.second <= 0) {
      bad("the deadline of " + kv.first + " must be positive");
    }
  }
  if (!(options.d_scale > 0.0) || !std::isfinite(options.d_scale)) {
    bad("d_scale must be positive");
  }
  for (auto & kv : options.link_delay_ns) {
    if (kv.second < 0) {
      bad("the link delay of " + kv.first + " must not be negative");
    }
  }
  if (options.default_link_delay_ns < 0 || options.random_link_delay_max_ns < 0) {
    bad("link delays must not be negative");
  }
  if (options.lead_ns == 0 || options.lead_ns < -1) {
    bad("lead must be positive, or -1 for no limit");
  }
  if (options.sched != 0 && options.sched != 1) {
    bad("sched must be 0 or 1");
  }
  for (auto & kv : options.timer_offset_ns) {
    if (kv.second < 0) {
      bad("the timer offset of " + kv.first + " must not be negative, so that no job is "
        "released before 0");
    }
  }
  impl_->opt = std::move(options);
}

Simulator::~Simulator()
{
  detail::clear_targets();
}

void Simulator::add_node(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node,
  std::shared_ptr<void> keep_alive)
{
  auto t = std::make_unique<Task>();
  t->name = node->get_name();
  t->fqn = node->get_fully_qualified_name();
  t->base = node;
  t->keep = std::move(keep_alive);
  impl_->tasks.push_back(std::move(t));
}

void Simulator::add_node(const rclcpp::Node::SharedPtr & node)
{
  add_node(node->get_node_base_interface(), node);
}

bool Simulator::Impl::needs_plan() const
{
  return opt.engine != Engine::Frontier || std::getenv("LET_ROS_SELFCHECK") != nullptr;
}

void Simulator::Impl::build()
{
  build_graph();
  if (jobs_built) {
    return;
  }
  jobs_built = true;
  if (opt.model == Model::Calendar) {
    generate_jobs();
  } else {
    build_streams();
  }
}

void Simulator::Impl::build_graph()
{
  if (graph_built) {
    return;
  }
  if (opt.initialize_components) {
    std::vector<rclcpp::node_interfaces::NodeBaseInterface::SharedPtr> nodes;
    for (const auto & t : tasks) {
      nodes.push_back(t->base);
    }
    detail::initialize_component_connections(nodes);
  }
  graph_built = true;

  for (size_t i = 0; i < tasks.size(); ++i) {
    Task & t = *tasks[i];
    t.base->for_each_callback_group(
      [&](rclcpp::CallbackGroup::SharedPtr group) {
        group->collect_all_ptrs(
          [&](const rclcpp::SubscriptionBase::SharedPtr & sub) {
            std::string topic = sub->get_topic_name();
            if (excluded_topic(topic)) {
              return;
            }
            if (sub->is_cft_enabled()) {
              throw std::runtime_error("content-filtered subscription to " + topic +
                " in " + t.fqn + " is outside the record model");
            }
            if (sub->get_intra_process_waitable()) {
              throw std::runtime_error(
                "intra-process communication is still on for the subscription to " +
                topic + " in node " + t.fqn + ". The executable must link src/ipc.cpp "
                "and export its symbols (ENABLE_EXPORTS).");
            }
            t.subs.push_back(SubInfo{sub, topic});
          },
          [&](const rclcpp::ServiceBase::SharedPtr &) {services_ignored++;},
          [&](const rclcpp::ClientBase::SharedPtr &) {services_ignored++;},
          [&](const rclcpp::TimerBase::SharedPtr & timer) {
            int64_t period = 0;
            if (rcl_timer_get_period(timer->get_timer_handle().get(), &period) != RCL_RET_OK) {
              rcutils_reset_error();
              throw std::runtime_error("cannot read timer period in node " + t.fqn);
            }
            if (period <= 0) {
              throw std::runtime_error(
                "timer with period 0 in node " + t.fqn + ". The simulator needs periodic "
                "timers (the throughput timing of reference_system runs its timers as fast as "
                "possible, use default or cpu)");
            }
            t.timers.push_back(TimerInfo{timer, period});
          },
          [&](const rclcpp::Waitable::SharedPtr &) {});
      });
  }

  std::unordered_map<const rcl_node_t *, int> node_index;
  for (size_t i = 0; i < tasks.size(); ++i) {
    node_index[tasks[i]->base->get_rcl_node_handle()] = static_cast<int>(i);
  }
  auto infos = detail::registered_publishers();
  std::sort(infos.begin(), infos.end(),
    [](const detail::PublisherInfo & a, const detail::PublisherInfo & b) {
      return a.seq < b.seq;
    });
  for (auto & info : infos) {
    auto it = node_index.find(info.node);
    if (it == node_index.end() || excluded_topic(info.topic)) {
      continue;
    }
    if (info.outside_messages != 0) {
      throw std::runtime_error("publisher of " + info.topic + " in " +
        tasks[static_cast<size_t>(it->second)]->fqn +
        " published outside a callback job (including node construction), which is unsupported");
    }
    Pub p;
    p.handle = info.handle;
    p.task = it->second;
    p.topic = info.topic;
    p.type_support = info.type_support;
    pubs.push_back(std::move(p));
  }
  std::stable_sort(pubs.begin(), pubs.end(), [](const Pub & a, const Pub & b) {
      return a.task < b.task || (a.task == b.task && a.topic < b.topic);
    });
  for (size_t k = 1; k < pubs.size(); ++k) {
    if (pubs[k - 1].task == pubs[k].task && pubs[k - 1].topic == pubs[k].topic) {
      throw std::runtime_error("the record model requires at most one publisher per node on " +
        pubs[k].topic + " in " + tasks[static_cast<size_t>(pubs[k].task)]->fqn);
    }
  }
  for (size_t k = 0; k < pubs.size(); ++k) {
    Task & owner = *tasks[pubs[k].task];
    pubs[k].target.index = static_cast<int>(owner.pubs.size());
    owner.pubs.push_back(static_cast<int>(k));
  }
  {
    std::map<const rclcpp::Context *,
      std::shared_ptr<rclcpp::experimental::IntraProcessManager>> ipms;
    for (auto & p : pubs) {
      auto ctx = tasks[static_cast<size_t>(p.task)]->base->get_context();
      auto & ipm = ipms[ctx.get()];
      if (!ipm) {
        ipm = ctx->get_sub_context<rclcpp::experimental::IntraProcessManager>();
      }
      rmw_gid_t gid;
      const rmw_publisher_t * rp = rcl_publisher_get_rmw_handle(p.handle);
      if (!rp || rmw_get_gid_for_publisher(rp, &gid) != RMW_RET_OK) {
        rcutils_reset_error();
        throw std::runtime_error("cannot read the publisher id of " + p.topic);
      }
      if (ipm->matches_any_publishers(&gid)) {
        throw std::runtime_error(
          "intra-process communication is still on for the publisher of " + p.topic +
          " in node " + tasks[static_cast<size_t>(p.task)]->fqn +
          ". The executable must link src/ipc.cpp and export its symbols "
          "(ENABLE_EXPORTS).");
      }
    }
  }
  apply_publish_sets();

  std::map<std::string, std::vector<int>> topic_pubs;
  for (size_t k = 0; k < pubs.size(); ++k) {
    topic_pubs[pubs[k].topic].push_back(static_cast<int>(k));
  }
  for (size_t j = 0; j < tasks.size(); ++j) {
    for (size_t s = 0; s < tasks[j]->subs.size(); ++s) {
      const std::string & topic = tasks[j]->subs[s].topic;
      auto it = topic_pubs.find(topic);
      if (it == topic_pubs.end()) {
        unfed_subs.push_back(tasks[j]->fqn + " <- " + topic);
        continue;
      }
      std::map<int, std::vector<int>> by_task;
      for (int pk : it->second) {
        const Pub & publisher = pubs[pk];
        if (detail::topic_endpoints_compatible(
            publisher.handle, publisher.type_support, *tasks[j]->subs[s].sub))
        {
          by_task[publisher.task].push_back(pk);
        }
      }
      if (by_task.empty()) {
        unfed_subs.push_back(tasks[j]->fqn + " <- " + topic +
          " (no type/QoS compatible publisher)");
      }
      for (auto & kv : by_task) {
        Channel c;
        c.prod = kv.first;
        c.cons = static_cast<int>(j);
        c.sub = static_cast<int>(s);
        c.pubs = kv.second;
        int id = static_cast<int>(channels.size());
        channels.push_back(std::move(c));
        tasks[kv.first]->out_ch.push_back(id);
        tasks[j]->in_ch.push_back(id);
      }
    }
  }
  for (auto & p : pubs) {
    p.target.subscribers = 0;
  }
  for (const auto & channel : channels) {
    for (int pid : channel.pubs) {
      ++pubs[static_cast<size_t>(pid)].target.subscribers;
    }
  }
  for (size_t i = 0; i < tasks.size(); ++i) {
    std::set<int> nb;
    nb.insert(static_cast<int>(i));
    for (int c : tasks[i]->out_ch) {
      nb.insert(channels[c].cons);
    }
    for (int c : tasks[i]->in_ch) {
      nb.insert(channels[c].prod);
    }
    tasks[i]->neighbors.assign(nb.begin(), nb.end());
  }

  compute_deadlines();
  compute_link_delays();
}

void Simulator::Impl::compute_link_delays()
{
  std::set<std::string> matched;
  for (auto & ch : channels) {
    const Task & t = *tasks[static_cast<size_t>(ch.cons)];
    const std::string & topic = t.subs[static_cast<size_t>(ch.sub)].topic;
    auto it = opt.link_delay_ns.end();
    for (const auto & key : {topic + " " + t.fqn, topic + " " + t.name, topic}) {
      auto candidate = opt.link_delay_ns.find(key);
      if (candidate != opt.link_delay_ns.end()) {
        matched.insert(candidate->first);
        if (it == opt.link_delay_ns.end()) {
          it = candidate;
        }
      }
    }
    if (it != opt.link_delay_ns.end()) {
      ch.delay = it->second;
    } else if (opt.random_link_delay_max_ns > 0) {
      uint64_t h = mix(0, opt.random_link_delay_seed);
      for (char c : topic + " " + t.fqn) {
        h = mix(h, static_cast<uint8_t>(c));
      }
      h = mix(h, 0x5bd1e995ULL);
      if (h % 4 == 0) {
        ch.delay = 0;
      } else {
        ch.delay = static_cast<int64_t>((h >> 2) % static_cast<uint64_t>(opt.random_link_delay_max_ns + 1));
      }
    } else {
      ch.delay = opt.default_link_delay_ns;
    }
  }
  for (auto & kv : opt.link_delay_ns) {
    if (!matched.count(kv.first)) {
      throw std::runtime_error("link delay given for " + kv.first +
              ", which matches no connection of the graph, a publisher and a subscription on a "
              "topic (use '<topic>' or '<topic> <node>')");
    }
  }
}

int64_t Simulator::Impl::max_out_delay(const Task & t) const
{
  int64_t m = 0;
  for (int c : t.out_ch) {
    m = std::max(m, channels[static_cast<size_t>(c)].delay);
  }
  return m;
}

void Simulator::Impl::apply_publish_sets()
{
  for (auto & tp : tasks) {
    Task & t = *tp;
    size_t n = t.pubs.size();
    t.allow_timer.assign(t.timers.size(), std::vector<char>(n, 1));
    t.allow_sub.assign(t.subs.size(), std::vector<char>(n, 1));
    t.used_timer.assign(t.timers.size(), std::vector<char>(n, 0));
    t.used_sub.assign(t.subs.size(), std::vector<char>(n, 0));
    auto it = opt.publish_sets.find(t.fqn);
    if (it == opt.publish_sets.end()) {
      it = opt.publish_sets.find(t.name);
    }
    if (it == opt.publish_sets.end()) {
      continue;
    }
    auto mask = [&](const std::set<std::string> & topics) {
        std::vector<char> m(n, 0);
        for (size_t x = 0; x < n; ++x) {
          m[x] = topics.count(pubs[static_cast<size_t>(t.pubs[x])].topic) ? 1 : 0;
        }
        return m;
      };
    for (auto & kv : it->second) {
      bool matched = false;
      if (kv.first == "timer") {
        for (auto & a : t.allow_timer) {
          a = mask(kv.second);
          matched = true;
        }
      } else {
        for (size_t x = 0; x < t.subs.size(); ++x) {
          if (t.subs[x].topic == kv.first) {
            t.allow_sub[x] = mask(kv.second);
            matched = true;
          }
        }
      }
      if (!matched) {
        throw std::runtime_error("publish set for " + t.fqn + ": no callback '" + kv.first + "'");
      }
    }
  }
}

void Simulator::Impl::write_publish_sets(const std::string & path) const
{
  std::ofstream out(path);
  if (!out) {
    throw std::runtime_error("cannot write " + path);
  }
  out << "# <node> <callback> <topics it published, or ->\n"
      << "# callback is 'timer' (all timers of the node) or a subscribed topic\n";
  auto topics = [&](const Task & t, const std::vector<char> & used) {
      std::string s;
      for (size_t x = 0; x < used.size(); ++x) {
        if (used[x]) {
          s += " " + pubs[static_cast<size_t>(t.pubs[x])].topic;
        }
      }
      return s.empty() ? std::string(" -") : s;
    };
  for (auto & tp : tasks) {
    const Task & t = *tp;
    if (!t.timers.empty()) {
      std::vector<char> any(t.pubs.size(), 0);
      for (auto & u : t.used_timer) {
        for (size_t x = 0; x < u.size(); ++x) {
          any[x] = any[x] || u[x];
        }
      }
      out << t.fqn << " timer" << topics(t, any) << "\n";
    }
    std::map<std::string, std::vector<char>> by_topic;
    for (size_t x = 0; x < t.subs.size(); ++x) {
      auto & any = by_topic[t.subs[x].topic];
      any.resize(t.pubs.size(), 0);
      for (size_t y = 0; y < any.size(); ++y) {
        any[y] = any[y] || t.used_sub[x][y];
      }
    }
    for (auto & kv : by_topic) {
      out << t.fqn << " " << kv.first << topics(t, kv.second) << "\n";
    }
  }
}

void Simulator::Impl::compute_deadlines()
{
  std::vector<bool> fixed(tasks.size(), false);
  std::set<std::string> matched;
  for (size_t i = 0; i < tasks.size(); ++i) {
    Task & t = *tasks[i];
    auto it = opt.deadline_ns.find(t.fqn);
    if (it == opt.deadline_ns.end()) {
      it = opt.deadline_ns.find(t.name);
    }
    if (it != opt.deadline_ns.end()) {
      matched.insert(it->first);
      t.D = it->second;
      fixed[i] = true;
    } else if (!t.timers.empty()) {
      int64_t d = t.timers[0].period;
      for (auto & ti : t.timers) {
        d = std::min(d, ti.period);
      }
      t.D = d;
      fixed[i] = true;
    }
  }
  for (auto & kv : opt.deadline_ns) {
    if (!matched.count(kv.first)) {
      throw std::runtime_error("deadline given for " + kv.first + ", which is no node of the graph");
    }
  }
  bool changed = true;
  while (changed) {
    changed = false;
    for (size_t i = 0; i < tasks.size(); ++i) {
      if (fixed[i]) {
        continue;
      }
      Task & t = *tasks[i];
      int64_t best = -1;
      for (int c : t.in_ch) {
        int64_t d = tasks[channels[c].prod]->D;
        if (d > 0 && (best < 0 || d < best)) {
          best = d;
        }
      }
      if (best > 0 && (t.D < 0 || best < t.D)) {
        t.D = best;
        changed = true;
      }
    }
  }
  std::string missing;
  for (auto & tp : tasks) {
    if (tp->D <= 0) {
      if (opt.default_deadline_ns > 0) {
        tp->D = opt.default_deadline_ns;
      } else if (!tp->subs.empty() || !tp->timers.empty()) {
        missing += " " + tp->fqn;
      } else {
        tp->D = 1;
      }
    }
  }
  if (!missing.empty()) {
    throw std::runtime_error(
            "no deadline for nodes without timers and without timed upstream:" + missing +
            " (give one in the deadline file or with --default-deadline)");
  }
  if (opt.d_scale != 1.0) {
    for (auto & tp : tasks) {
      tp->D = std::max<int64_t>(1, std::llround(static_cast<double>(tp->D) * opt.d_scale));
    }
  }
}

int64_t Simulator::Impl::timer_first_release(int task, int timer) const
{
  const Task & t = *tasks[static_cast<size_t>(task)];
  int64_t offset = 0;
  size_t best = 0;
  for (auto & kv : opt.timer_offset_ns) {
    if (t.fqn.rfind(kv.first, 0) == 0 && kv.first.size() >= best) {
      best = kv.first.size();
      offset = kv.second;
    }
  }
  return offset + (opt.timer_phase_zero ? 0 : t.timers[static_cast<size_t>(timer)].period);
}

int64_t Simulator::Impl::window_length() const
{
  int64_t window = -1;
  for (auto & tp : tasks) {
    bool has_jobs = opt.model == Model::Calendar ? !tp->jobs.empty() : tp->total > 0;
    if (has_jobs && (window < 0 || tp->D < window)) {
      window = tp->D;
    }
  }
  return std::max<int64_t>(window, 1);
}

void Simulator::Impl::fail(const std::string & what)
{
  {
    std::lock_guard<std::mutex> lock(err_mutex);
    if (error.empty()) {
      error = what;
    }
  }
  abort.store(true);
}

void Simulator::Impl::push(const Item & item)
{
  int w = tl_worker;
  if (w < 0) {
    w = static_cast<int>(rr.fetch_add(1) % queues.size());
  }
  wpushed[static_cast<size_t>(w)].v.fetch_add(1);
  Queue & qu = *queues[w];
  std::lock_guard<std::mutex> lock(qu.m);
  qu.q.push_back(item);
  qu.n.store(static_cast<int>(qu.q.size()), std::memory_order_relaxed);
}

bool Simulator::Impl::pop(int w, Item & item)
{
  const bool skip = opt.sched == 1;
  {
    Queue & own = *queues[w];
    if (!skip || own.n.load(std::memory_order_relaxed) > 0) {
      std::lock_guard<std::mutex> lock(own.m);
      if (!own.q.empty()) {
        item = own.q.back();
        own.q.pop_back();
        own.n.store(static_cast<int>(own.q.size()), std::memory_order_relaxed);
        return true;
      }
    }
  }
  int n = static_cast<int>(queues.size());
  for (int d = 1; d < n; ++d) {
    Queue & other = *queues[(w + d) % n];
    if (skip && other.n.load(std::memory_order_relaxed) == 0) {
      continue;
    }
    std::lock_guard<std::mutex> lock(other.m);
    if (!other.q.empty()) {
      item = other.q.front();
      other.q.pop_front();
      other.n.store(static_cast<int>(other.q.size()), std::memory_order_relaxed);
      return true;
    }
  }
  return false;
}

void Simulator::Impl::back_off(int idle) const
{
  if (idle > 64) {
    std::this_thread::yield();
    return;
  }
  const int spins = 1 << std::min(idle, 4);
  for (int s = 0; s < spins; ++s) {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield");
#endif
  }
}

int64_t Simulator::Impl::pushed_sum() const
{
  int64_t n = 0;
  for (size_t w = 0; w < queues.size(); ++w) {
    n += wpushed[w].v.load();
  }
  return n;
}

int64_t Simulator::Impl::completed_sum() const
{
  int64_t n = 0;
  for (size_t w = 0; w < queues.size(); ++w) {
    n += wdone[w].v.load();
  }
  return n;
}

bool Simulator::Impl::idle_check(int64_t total)
{
  const int64_t done = completed_sum();
  if (done >= total) {
    stop.store(true);
    return false;
  }
  if (pushed_sum() - done <= 0) {
    fail("deadlock: no job is ready after " + std::to_string(done) + " of " +
      std::to_string(total) + " jobs");
    return false;
  }
  return true;
}

bool Simulator::Impl::fire_timer(TimerInfo & ti, int64_t release)
{
  if (!ti.timer->call()) {
    return false;
  }
  if (tl_job) {
    tasks[static_cast<size_t>(tl_job->task)]->timer_calls++;
  }
  auto info = std::make_shared<rcl_timer_call_info_t>();
  info->expected_call_time = release;
  info->actual_call_time = release;
  std::shared_ptr<void> data = info;
  ti.timer->execute_callback(data);
  return true;
}

bool Simulator::Impl::deliver(
  Task & t, SubInfo & si, const Slot & s, int64_t release,
  int64_t source)
{
  if (s.count == 0) {
    return false;
  }
  t.sub_calls += s.count;
  rmw_message_info_t raw = rmw_get_zero_initialized_message_info();
  raw.source_timestamp = source;
  raw.received_timestamp = release;
  rclcpp::MessageInfo info(raw);
  for (int m = 0; m < s.count; ++m) {
    const std::vector<uint8_t> & bytes = s.msgs[m];
    if (si.sub->is_serialized()) {
      auto sm = std::make_shared<rclcpp::SerializedMessage>(bytes.size());
      auto & raw_sm = sm->get_rcl_serialized_message();
      std::memcpy(raw_sm.buffer, bytes.data(), bytes.size());
      raw_sm.buffer_length = bytes.size();
      si.sub->handle_serialized_message(sm, info);
    } else {
      std::shared_ptr<void> msg = si.sub->create_message();
      rmw_serialized_message_t view = rmw_get_zero_initialized_serialized_message();
      view.buffer = const_cast<uint8_t *>(bytes.data());
      view.buffer_length = bytes.size();
      view.buffer_capacity = bytes.size();
      view.allocator = rcutils_get_default_allocator();
      if (rmw_deserialize(&view, &si.sub->get_message_type_support_handle(), msg.get()) !=
        RMW_RET_OK)
      {
        throw std::runtime_error("cannot deserialize message on " + si.topic);
      }
      si.sub->handle_message(msg, info);
      si.sub->return_message(msg);
    }
  }
  return true;
}

void Simulator::Impl::account(Task & t, int pid, const Slot & s, int64_t release, int64_t k)
{
  Pub & p = pubs[static_cast<size_t>(pid)];
  p.messages += s.count;
  if (s.count == 0) {
    return;
  }
  p.hash = mix(p.hash, static_cast<uint64_t>(release));
  for (int m = 0; m < s.count; ++m) {
    p.hash = hash_bytes(p.hash, s.msgs[m]);
  }
  if (tracing) {
    std::ostringstream line;
    line << t.fqn << " " << p.topic << " job=" << k << " r=" << release << " n=" << s.count;
    for (int m = 0; m < s.count; ++m) {
      line << " " << std::hex << hash_bytes(0, s.msgs[m]);
      if (std::getenv("LET_ROS_DUMP")) {
        line << " [";
        for (size_t b = 0; b < s.msgs[m].size() && b < 96; ++b) {
          line << std::setw(2) << std::setfill('0') << static_cast<int>(s.msgs[m][b]);
        }
        line << "]" << std::setfill(' ');
      }
      line << std::dec;
    }
    t.trace.push_back(line.str());
  }
}

int64_t Simulator::Impl::payload_bytes() const
{
  int64_t n = 0;
  auto add = [&n](const Slot & s) {
      for (auto & m : s.msgs) {
        n += static_cast<int64_t>(m.capacity());
      }
    };
  if (opt.model == Model::Calendar) {
    for (auto & p : pubs) {
      for (auto & sl : p.target.ring) {
        add(sl);
      }
    }
  } else {
    for (auto & st : streams) {
      for (auto & r : st.rings) {
        for (auto & sl : r) {
          add(sl);
        }
      }
    }
  }
  return n;
}

int64_t Simulator::Impl::ring_slots() const
{
  int64_t n = 0;
  if (opt.model == Model::Calendar) {
    for (auto & p : pubs) {
      n += static_cast<int64_t>(p.target.ring.size());
    }
  } else {
    for (auto & s : streams) {
      for (auto & r : s.rings) {
        n += static_cast<int64_t>(r.size());
      }
    }
  }
  return n;
}

Result Simulator::run()
{
  Impl & im = *impl_;
  const Engine engine = im.opt.engine;
  const bool calendar = im.opt.model == Model::Calendar;
  const bool barrier = engine == Engine::Barrier || engine == Engine::Tag;
  if (engine == Engine::Static && !im.opt.costs) {
    throw std::invalid_argument(
            "let_ros: the static engine needs measured job costs (Options::costs), from a "
            "sequential run with record_costs or from a file (--costs FILE)");
  }
  Result res;
  res.engine = engine_name(engine);
  res.model = model_name(im.opt.model);
  res.workers = engine == Engine::Sequential ? 1 : im.opt.workers;
  res.horizon_ns = im.opt.horizon_ns;
  res.ring_inf = im.opt.ring_inf;
  res.ring_scale = im.opt.ring_scale;
  res.d_scale = im.opt.d_scale;
  res.lead_ns = engine == Engine::Frontier ? im.opt.lead_ns : 0;
  res.sched = engine == Engine::OutOfOrder || engine == Engine::Frontier ? im.opt.sched : 0;

  auto & he = detail::hook_errors();
  const auto outside_before = he.outside_publish.load();
  auto t0 = std::chrono::steady_clock::now();
  if (im.needs_plan()) {
    im.build();
  } else {
    im.build_graph();
  }
  int64_t window = 0;
  if (barrier) {
    const int64_t min_d = im.window_length();
    window = engine == Engine::Tag ? 1 : (im.opt.window_ns > 0 ? im.opt.window_ns : min_d);
    if (window > min_d) {
      throw std::invalid_argument(
              "let_ros: window " + std::to_string(window) + " ns is larger than the smallest D (" +
              std::to_string(min_d) + " ns)");
    }
  }
  im.windows = 0;
  if (engine == Engine::Frontier) {
    im.f_prepare();
  } else if (calendar) {
    im.allocate_rings(barrier, window);
    if (engine == Engine::Sequential) {
      im.prepare_sequential();
    } else if (barrier) {
      im.prepare_barrier(window);
    } else if (engine == Engine::Static) {
      im.st_prepare();
    }
  } else {
    im.allocate_stream_rings(barrier, window);
    if (engine == Engine::Static) {
      im.st_prepare();
    }
  }
  for (auto & tp : im.tasks) {
    for (auto & u : tp->used_timer) {
      std::fill(u.begin(), u.end(), 0);
    }
    for (auto & u : tp->used_sub) {
      std::fill(u.begin(), u.end(), 0);
    }
  }
  for (auto & tp : im.tasks) {
    tp->timer_calls = 0;
    tp->sub_calls = 0;
  }
  he.publish_set_violation.store(0);
  auto t1 = std::chrono::steady_clock::now();

  im.abort.store(false);
  im.error.clear();
  im.tracing = !im.opt.trace_path.empty();
  for (auto & tp : im.tasks) {
    tp->trace.clear();
  }
  if (engine == Engine::Frontier) {
    im.f_run();
  } else if (engine == Engine::Static) {
    im.st_run();
  } else if (calendar) {
    if (engine == Engine::Sequential) {
      im.run_sequential();
    } else if (barrier) {
      im.run_barrier();
    } else {
      im.run_ooo();
    }
  } else {
    if (engine == Engine::Sequential) {
      im.s_run_sequential();
    } else if (barrier) {
      im.s_run_barrier(window);
    } else {
      im.s_run_ooo();
    }
  }
  auto t2 = std::chrono::steady_clock::now();
  detail::clear_targets();
  if (!im.error.empty()) {
    throw std::runtime_error(im.error);
  }
  if (he.outside_publish.load() != outside_before) {
    throw std::runtime_error("publishing outside callback jobs is unsupported");
  }
  if (he.foreign_publish.load() || he.serialize_failed.load()) {
    throw std::runtime_error("publish errors: foreign=" + std::to_string(he.foreign_publish) +
            " serialize=" + std::to_string(he.serialize_failed));
  }
  if (int64_t v = he.publish_set_violation.load()) {
    const Task & t = *im.tasks[static_cast<size_t>(he.first_violation_task.load())];
    const Pub & p = im.pubs[static_cast<size_t>(t.pubs[static_cast<size_t>(
        he.first_violation_pub.load())])];
    throw std::runtime_error(
            std::to_string(v) + " publishes outside the publish sets, first by " + t.fqn +
            " on " + p.topic + " (record the sets again or widen them)");
  }
  for (const auto & topic : im.opt.required_output_topics) {
    int64_t count = 0;
    for (const auto & publisher : im.pubs) {
      if (publisher.topic == topic) {
        count += publisher.messages;
      }
    }
    if (count == 0) {
      throw std::runtime_error("required output topic " + topic +
        " produced no messages (check graph initialization, input and horizon)");
    }
  }
  if (!im.opt.record_publish_sets_path.empty()) {
    im.write_publish_sets(im.opt.record_publish_sets_path);
  }

  res.tasks = im.tasks.size();
  res.channels = im.channels.size();
  for (const auto & ch : im.channels) {
    res.max_link_delay_ns = std::max(res.max_link_delay_ns, ch.delay);
    res.delayed_channels += ch.delay > 0 ? 1 : 0;
  }
  res.streams = calendar || !im.jobs_built ? 0 : static_cast<int64_t>(im.streams.size());
  res.window_ns = window;
  res.windows = im.windows;
  if (engine == Engine::Frontier) {
    for (size_t i = 0; i < im.ftasks.size(); ++i) {
      for (auto & fs : im.ftasks[i]->slots) {
        res.ring_slots++;
        for (auto & m : fs->s.msgs) {
          res.payload_bytes += static_cast<int64_t>(m.capacity());
        }
      }
      res.nulls += im.f_nulls[i];
    }
  } else {
    res.ring_slots = im.ring_slots();
    res.payload_bytes = im.payload_bytes();
  }
  res.est_s = engine == Engine::Static ? im.st_est : 0.0;
  if (engine == Engine::Static) {
    res.cost_model = im.opt.cost_model == CostModel::Callback ? "callback" : "job";
    res.costs_source = im.opt.costs_source.empty() ? "given" : im.opt.costs_source;
  }
  if (engine == Engine::Sequential && im.opt.record_costs && im.opt.costs) {
    for (size_t i = 0; i < im.tasks.size(); ++i) {
      im.opt.costs->ns[im.tasks[i]->fqn] = im.seq_costs[i];
    }
  }
  if (engine == Engine::Frontier) {
    if (im.finished_tasks.load() != static_cast<int64_t>(im.tasks.size())) {
      throw std::runtime_error("internal error: the frontier engine stopped with tasks left");
    }
    for (size_t i = 0; i < im.tasks.size(); ++i) {
      const Task & t = *im.tasks[i];
      const FTask & F = *im.ftasks[i];
      bool unread = !F.inbox.empty();
      for (const FInput & in : F.ins) {
        unread = unread || !in.q.empty();
      }
      if (unread) {
        throw std::runtime_error("internal error: " + t.fqn + " has unread records");
      }
      if (im.jobs_built) {
        int64_t jobs = calendar ? static_cast<int64_t>(t.jobs.size()) : t.total;
        if (t.done.load() != jobs) {
          throw std::runtime_error("internal error: " + t.fqn + " ran " +
                  std::to_string(t.done.load()) + " jobs, the model has " + std::to_string(jobs));
        }
      }
    }
  }
  for (auto & tp : im.tasks) {
    int64_t jobs = engine == Engine::Frontier ? tp->done.load() :
      calendar ? static_cast<int64_t>(tp->jobs.size()) : tp->total;
    res.jobs += jobs;
    res.active_jobs += tp->active;
    res.timer_callbacks += tp->timer_calls;
    res.sub_callbacks += tp->sub_calls;
    if (tp->done.load() != jobs) {
      throw std::runtime_error("internal error: " + tp->fqn + " did not finish");
    }
  }
  std::vector<std::pair<std::string, uint64_t>> hs;
  for (auto & p : im.pubs) {
    res.messages += p.messages;
    hs.emplace_back(im.tasks[p.task]->fqn + "|" + p.topic, p.hash);
  }
  std::sort(hs.begin(), hs.end());
  uint64_t h = 0;
  for (auto & kv : hs) {
    for (char c : kv.first) {
      h = mix(h, static_cast<uint8_t>(c));
    }
    h = mix(h, kv.second);
  }
  res.hash = h;
  res.setup_s = std::chrono::duration<double>(t1 - t0).count();
  res.run_s = std::chrono::duration<double>(t2 - t1).count();
  res.cores = static_cast<int>(allowed_cores().size());
  res.pinned = im.opt.pin_threads;
  if (const char * dag = std::getenv("LET_ROS_DAG")) {
    if (calendar && engine != Engine::Frontier) {
      im.dump_dag(dag);
    } else {
      std::cerr << "let_ros: LET_ROS_DAG is not written by this run (" << res.engine << ", " <<
        res.model << " model). It needs --model calendar and an engine other than frontier.\n";
    }
  }
  if (im.tracing) {
    std::ofstream out(im.opt.trace_path);
    for (auto & tp : im.tasks) {
      for (auto & line : tp->trace) {
        out << line << "\n";
      }
    }
  }
  return res;
}

void Simulator::describe(std::ostream & os) const
{
  Impl & im = *impl_;
  const bool plan = im.needs_plan();
  if (plan) {
    im.build();
  } else {
    im.build_graph();
  }
  const bool calendar = im.opt.model == Model::Calendar;
  os << "tasks " << im.tasks.size() << ", channels " << im.channels.size();
  if (plan && !calendar) {
    os << ", streams " << im.streams.size() << (im.cyclic ? " (cyclic)" : "");
  }
  os << ", ignored services/clients " << im.services_ignored << "\n";
  if (!plan) {
    os << "  (frontier engine: no job plan is built, so no jobs or rings are listed)\n";
  }
  auto & ipc = detail::intra_process_counts();
  if (ipc.publishers.load() + ipc.subscriptions.load() > 0) {
    os << "  intra-process communication turned off for " << ipc.publishers.load() <<
      " publishers and " << ipc.subscriptions.load() << " subscriptions so far\n";
  }
  for (auto & tp : im.tasks) {
    os << "  " << tp->fqn << "  D=" << tp->D / 1e6 << "ms";
    if (!plan) {
    } else if (calendar) {
      os << "  jobs=" << tp->jobs.size() << "  ring(ooo)=" << im.ring_for(*tp, false, 1);
    } else {
      int64_t kmax = 0;
      for (int sid : tp->streams) {
        const Stream & s = im.streams[static_cast<size_t>(sid)];
        const int64_t span = tp->D + im.stream_child_delay(s);
        kmax = std::max<int64_t>(kmax, std::max<int64_t>(im.opt.min_ring, span / s.period + 1) *
            im.opt.ring_scale);
      }
      os << "  jobs=" << tp->total << "  streams=" << tp->streams.size() << "  K(ooo)<=" << kmax;
    }
    for (auto & ti : tp->timers) {
      os << "  timer " << ti.period / 1e6 << "ms";
    }
    for (int pid : tp->pubs) {
      os << "  pub " << im.pubs[pid].topic;
    }
    for (size_t x = 0; x < tp->subs.size(); ++x) {
      os << "  sub " << tp->subs[x].topic;
      int64_t c = 0;
      for (int ci : tp->in_ch) {
        if (im.channels[static_cast<size_t>(ci)].sub == static_cast<int>(x)) {
          c = std::max(c, im.channels[static_cast<size_t>(ci)].delay);
        }
      }
      if (c > 0) {
        os << " (C=" << c / 1e6 << "ms)";
      }
    }
    os << "\n";
  }
  for (auto & u : im.unfed_subs) {
    os << "  no publisher in the simulation: " << u << "\n";
  }
}

}
