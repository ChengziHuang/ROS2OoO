#pragma once

#include <cmath>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#ifdef __GLIBC__
#include <execinfo.h>
#include <malloc.h>
#endif
#include <sys/syscall.h>
#include <unistd.h>

#include <rclcpp/rclcpp.hpp>
#include <rcutils/error_handling.h>

#include "let_ros/simulator.hpp"

namespace let_ros
{

using NodeList = std::vector<std::pair<
      rclcpp::node_interfaces::NodeBaseInterface::SharedPtr, std::shared_ptr<void>>>;
using NodeFactory = std::function<NodeList()>;

struct DriverArgs
{
  Options base;
  std::string record_costs_path;
  std::vector<Engine> engines{Engine::OutOfOrder};
  std::vector<int> workers{1};
  std::vector<int64_t> leads;
  std::vector<int> scheds;
  std::vector<int> ring_scales;
  int repeat = 1;
  bool describe = false;
  bool check = false;
  bool interleave = false;
  std::vector<std::string> rest;
};

inline std::vector<std::string> split(const std::string & s, char sep)
{
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, sep)) {
    if (!item.empty()) {
      out.push_back(item);
    }
  }
  return out;
}

inline int64_t parse_int(
  const std::string & flag, const std::string & text, int64_t min,
  int64_t max = std::numeric_limits<int64_t>::max())
{
  size_t pos = 0;
  long long v = 0;
  try {
    v = std::stoll(text, &pos);
  } catch (const std::exception &) {
    pos = 0;
  }
  if (text.empty() || pos != text.size()) {
    throw std::runtime_error("invalid value '" + text + "' for " + flag);
  }
  if (v < min || v > max) {
    throw std::runtime_error(
            flag + " must be between " + std::to_string(min) + " and " + std::to_string(max) +
            ", got " + text);
  }
  return v;
}

inline int64_t parse_positive_duration(const std::string & flag, const std::string & text)
{
  int64_t v = parse_duration_ns(text);
  if (v <= 0) {
    throw std::runtime_error(flag + " must be positive, got " + text);
  }
  return v;
}

inline void print_usage(const char * prog, const std::string & extra)
{
  std::cerr <<
    "usage: " << prog << " [options]" << extra << "\n"
    "  --engine seq,barrier,ooo   engines to run (default ooo), also the baselines\n"
    "                             tag, frontier, static (see the README)\n"
    "  --workers 1,2,4            worker counts of the parallel engines (default 1)\n"
    "  --repeat N                 runs per configuration (default 1)\n"
    "  --interleave               repeat the whole list of configurations N times, in\n"
    "                             forward and reverse order on alternate repeats (ABBA), so\n"
    "                             that a drift of the machine hits all of them alike\n"
    "  --horizon 10s              simulate jobs released in [0, horizon)\n"
    "  --deadlines FILE           lines 'node: 50ms' that override D\n"
    "  --default-deadline 10ms    D for nodes without timer and timed upstream\n"
    "  --timer-phase zero|period  first timer release at 0 or at the period (default)\n"
    "  --pin                      pin each worker to its own allowed physical core\n"
    "  --describe                 print the task graph before running\n"
    "  --model streams|calendar   closed-form job dependencies (default) or a job\n"
    "                             list computed before the run (reference)\n"
    "  --max-streams N            fail if the streams model needs more (default 200000)\n"
    "  --check                    fail unless all runs give the same hash\n"
    "  --initialize-components    initialize lazy subscriptions before freezing the graph\n"
    "  --require-output-topic T   fail if topic T publishes no messages (repeatable)\n"
    "  --trace FILE               write one line per published record\n"
    "  --publish-sets FILE        topics each callback may publish (fewer jobs)\n"
    "  --record-publish-sets FILE write the topics each callback published\n"
    "  --window min|DURATION      barrier window (default min, the smallest D)\n"
    "  --ring auto|inf            ring slots from the formula (default) or one per job\n"
    "  --ring-scale N             the ring slots of the formula times N, at most one\n"
    "                             per job (default 1, ablation of the ring size); a\n"
    "                             list such as 1,4 runs ooo once per value, the other\n"
    "                             engines use the first\n"
    "  --lead inf|DURATION        frontier: how far a task may run ahead of its readers\n"
    "                             (default inf); a list such as inf,200ms runs frontier\n"
    "                             once per value\n"
    "  --sched old|new            scheduler of ooo and frontier (default old); new runs\n"
    "                             the next job of a task on the same worker and lets\n"
    "                             idle workers back off; a list such as old,new runs\n"
    "                             both\n"
    "  --costs FILE               static: job costs (default: measured by a seq\n"
    "                             calibration run that is not reported)\n"
    "  --record-costs FILE        write the job costs of a seq calibration run\n"
    "  --cost-model job|callback  static: cost of each job, or mean per callback\n"
    "  --d-scale F                multiply every D by F (the D sweep)\n"
    "  --link-delays FILE         lines 'topic: 5ms' or 'topic node: 5ms', the transport\n"
    "                             delay C of the connections: a record written at w\n"
    "                             reaches the subscription at w + C\n"
    "  --link-delay DURATION      C of the connections that no line matches (default 0)\n"
    "  --random-link-delays MAX[:SEED]  testing: C of the connections that no line\n"
    "                             matches, drawn from [0, MAX] by a hash of the topic,\n"
    "                             the node and the seed, about a quarter of them 0\n"
    "  --offsets FILE             lines 'prefix: 37ms', timer offsets of the nodes whose\n"
    "                             fully qualified name starts with the prefix\n";
}

inline DriverArgs parse_driver_args(int argc, char ** argv)
{
  DriverArgs a;
  const int64_t int_max = std::numeric_limits<int>::max();
  for (int i = 1; i < argc; ++i) {
    std::string s = argv[i];
    auto next = [&]() -> std::string {
        if (i + 1 >= argc) {
          throw std::runtime_error("missing value after " + s);
        }
        return argv[++i];
      };
    if (s == "--engine") {
      a.engines.clear();
      for (auto & e : split(next(), ',')) {
        if (e == "seq") {
          a.engines.push_back(Engine::Sequential);
        } else if (e == "barrier") {
          a.engines.push_back(Engine::Barrier);
        } else if (e == "ooo") {
          a.engines.push_back(Engine::OutOfOrder);
        } else if (e == "tag") {
          a.engines.push_back(Engine::Tag);
        } else if (e == "frontier") {
          a.engines.push_back(Engine::Frontier);
        } else if (e == "static") {
          a.engines.push_back(Engine::Static);
        } else {
          throw std::runtime_error("unknown engine " + e);
        }
      }
    } else if (s == "--workers") {
      a.workers.clear();
      for (auto & w : split(next(), ',')) {
        a.workers.push_back(static_cast<int>(parse_int(s, w, 1, int_max)));
      }
    } else if (s == "--repeat") {
      a.repeat = static_cast<int>(parse_int(s, next(), 1, int_max));
    } else if (s == "--horizon") {
      a.base.horizon_ns = parse_positive_duration(s, next());
    } else if (s == "--deadlines") {
      a.base.deadline_ns = load_deadlines(next());
    } else if (s == "--default-deadline") {
      a.base.default_deadline_ns = parse_positive_duration(s, next());
    } else if (s == "--timer-phase") {
      std::string v = next();
      if (v != "zero" && v != "period") {
        throw std::runtime_error("--timer-phase must be zero or period, got " + v);
      }
      a.base.timer_phase_zero = v == "zero";
    } else if (s == "--pin") {
      a.base.pin_threads = true;
    } else if (s == "--describe") {
      a.describe = true;
    } else if (s == "--check") {
      a.check = true;
    } else if (s == "--interleave") {
      a.interleave = true;
    } else if (s == "--initialize-components") {
      a.base.initialize_components = true;
    } else if (s == "--require-output-topic") {
      std::string topic = next();
      if (topic.empty() || topic.front() != '/') {
        throw std::invalid_argument("--require-output-topic needs an absolute ROS topic name");
      }
      a.base.required_output_topics.push_back(std::move(topic));
    } else if (s == "--trace") {
      a.base.trace_path = next();
    } else if (s == "--model") {
      std::string v = next();
      if (v == "streams") {
        a.base.model = Model::Streams;
      } else if (v == "calendar") {
        a.base.model = Model::Calendar;
      } else {
        throw std::runtime_error("unknown model " + v);
      }
    } else if (s == "--max-streams") {
      a.base.max_streams = parse_int(s, next(), 1);
    } else if (s == "--publish-sets") {
      a.base.publish_sets = load_publish_sets(next());
    } else if (s == "--record-publish-sets") {
      a.base.record_publish_sets_path = next();
    } else if (s == "--window") {
      std::string v = next();
      a.base.window_ns = v == "min" ? 0 : parse_positive_duration(s, v);
    } else if (s == "--ring") {
      std::string v = next();
      if (v != "auto" && v != "inf") {
        throw std::runtime_error("--ring must be auto or inf, got " + v);
      }
      a.base.ring_inf = v == "inf";
    } else if (s == "--ring-scale") {
      a.ring_scales.clear();
      for (auto & v : split(next(), ',')) {
        a.ring_scales.push_back(static_cast<int>(parse_int(s, v, 1, 1000000)));
      }
      if (a.ring_scales.empty()) {
        throw std::runtime_error("--ring-scale needs a number");
      }
      a.base.ring_scale = a.ring_scales.front();
    } else if (s == "--lead") {
      a.leads.clear();
      for (auto & v : split(next(), ',')) {
        a.leads.push_back(v == "inf" ? -1 : parse_positive_duration(s, v));
      }
      if (a.leads.empty()) {
        throw std::runtime_error("--lead needs inf or a duration");
      }
      a.base.lead_ns = a.leads.front();
    } else if (s == "--sched") {
      a.scheds.clear();
      for (auto & v : split(next(), ',')) {
        if (v != "old" && v != "new") {
          throw std::runtime_error("--sched must be old or new, got " + v);
        }
        a.scheds.push_back(v == "new" ? 1 : 0);
      }
      if (a.scheds.empty()) {
        throw std::runtime_error("--sched needs old or new");
      }
      a.base.sched = a.scheds.front();
    } else if (s == "--costs") {
      a.base.costs = std::make_shared<JobCosts>(JobCosts::load(next()));
    } else if (s == "--record-costs") {
      a.record_costs_path = next();
    } else if (s == "--cost-model") {
      std::string v = next();
      if (v != "job" && v != "callback") {
        throw std::runtime_error("--cost-model must be job or callback, got " + v);
      }
      a.base.cost_model = v == "job" ? CostModel::Job : CostModel::Callback;
    } else if (s == "--d-scale") {
      std::string v = next();
      double f = 0.0;
      size_t pos = 0;
      try {
        f = std::stod(v, &pos);
      } catch (const std::exception &) {
        pos = 0;
      }
      if (pos != v.size() || !(f > 0.0) || !std::isfinite(f)) {
        throw std::runtime_error("--d-scale must be a positive number, got " + v);
      }
      a.base.d_scale = f;
    } else if (s == "--link-delays") {
      for (auto & kv : load_link_delays(next())) {
        a.base.link_delay_ns[kv.first] = kv.second;
      }
    } else if (s == "--link-delay") {
      std::string v = next();
      int64_t d = parse_duration_ns(v);
      if (d < 0) {
        throw std::runtime_error("--link-delay must not be negative, got " + v);
      }
      a.base.default_link_delay_ns = d;
    } else if (s == "--random-link-delays") {
      std::string v = next();
      auto colon = v.find(':');
      a.base.random_link_delay_max_ns = parse_positive_duration(s, v.substr(0, colon));
      a.base.random_link_delay_seed = colon == std::string::npos ? 0 :
        static_cast<uint64_t>(parse_int(s, v.substr(colon + 1), 0));
    } else if (s == "--offsets") {
      for (auto & kv : load_offsets(next())) {
        a.base.timer_offset_ns[kv.first] = kv.second;
      }
    } else if (s == "--ros-args") {
      break;
    } else {
      a.rest.push_back(s);
    }
  }
  if (a.engines.empty()) {
    throw std::runtime_error("--engine needs at least one of seq, barrier, ooo");
  }
  if (a.workers.empty()) {
    throw std::runtime_error("--workers needs at least one worker count");
  }
  return a;
}

inline void crash_handler(int sig, siginfo_t * si, void *)
{
  char buf[200];
  int n = std::snprintf(buf, sizeof buf,
      "\n*** let_ros crashed: signal %d at address %p in thread %ld\n",
      sig, si ? si->si_addr : nullptr, static_cast<long>(syscall(SYS_gettid)));
  if (n > 0 && write(2, buf, static_cast<size_t>(n)) < 0) {
    n = 0;
  }
#ifdef __GLIBC__
  void * frames[100];
  backtrace_symbols_fd(frames, backtrace(frames, 100), 2);
#endif
  const char end[] = "*** end of backtrace\n";
  if (write(2, end, sizeof end - 1) < 0) {
    n = 0;
  }
  std::signal(sig, SIG_DFL);
  std::raise(sig);
}

inline void install_crash_handler()
{
#ifdef __GLIBC__
  void * warm[2];
  backtrace(warm, 2);
#endif
  static char altstack[1 << 16];
  stack_t ss{};
  ss.ss_sp = altstack;
  ss.ss_size = sizeof altstack;
  sigaltstack(&ss, nullptr);
  struct sigaction sa{};
  sa.sa_sigaction = crash_handler;
  sa.sa_flags = SA_SIGINFO | SA_ONSTACK | SA_RESETHAND;
  sigemptyset(&sa.sa_mask);
  for (int sig : {SIGSEGV, SIGBUS, SIGFPE, SIGILL, SIGABRT}) {
    sigaction(sig, &sa, nullptr);
  }
}

[[noreturn]] inline void finish(int rc)
{
  std::cout.flush();
  std::cerr.flush();
  std::fflush(nullptr);
  std::_Exit(rc);
}

inline long proc_status_kb(const std::string & key)
{
  std::ifstream in("/proc/self/status");
  std::string line;
  while (std::getline(in, line)) {
    if (line.compare(0, key.size(), key) == 0) {
      return std::strtol(line.c_str() + key.size(), nullptr, 10);
    }
  }
  return -1;
}

inline bool reset_peak_rss()
{
#ifdef __GLIBC__
  malloc_trim(0);
#endif
  std::ofstream out("/proc/self/clear_refs");
  out << "5";
  out.close();
  return !out.fail() && proc_status_kb("VmHWM:") >= 0;
}

inline int run_driver(const DriverArgs & a, const NodeFactory & factory)
{
  bool first = true;
  bool have_hash = false;
  uint64_t hash = 0;
  int runs = 0;
  int compared = 0;
  int mismatches = 0;
  auto run_once = [&](const Options & opt) {
      const auto node_start = std::chrono::steady_clock::now();
      NodeList nodes = factory();
      const auto node_end = std::chrono::steady_clock::now();
      const bool rss = reset_peak_rss();
      const long before = rss ? proc_status_kb("VmRSS:") : -1;
      Result res;
      {
        Simulator sim(opt);
        for (auto & n : nodes) {
          sim.add_node(n.first, n.second);
        }
        res = sim.run();
        if (a.describe && first) {
          sim.describe(std::cerr);
        }
        first = false;
      }
      res.node_setup_s = std::chrono::duration<double>(node_end - node_start).count();
      if (rss) {
        res.rss_before_mb = static_cast<double>(before) / 1024.0;
        res.rss_peak_mb = static_cast<double>(proc_status_kb("VmHWM:")) / 1024.0;
      }
      nodes.clear();
      rcutils_reset_error();
      compared++;
      if (!have_hash) {
        hash = res.hash;
        have_hash = true;
      } else if (res.hash != hash) {
        mismatches++;
      }
      return res;
    };
  std::shared_ptr<JobCosts> costs = a.base.costs;
  std::string source = costs ? "file" : "";
  bool static_needs = false;
  for (Engine e : a.engines) {
    static_needs = static_needs || (e == Engine::Static && !costs);
  }
  bool calibrated = false;
  if (static_needs || !a.record_costs_path.empty()) {
    auto measured = std::make_shared<JobCosts>();
    Options opt = a.base;
    opt.engine = Engine::Sequential;
    opt.workers = 1;
    opt.costs = measured;
    opt.record_costs = true;
    Result res = run_once(opt);
    calibrated = true;
    std::cerr << "calibration: a seq run measured the job costs in " << res.run_s <<
      " s, it is not reported\n";
    if (!a.record_costs_path.empty()) {
      measured->save(a.record_costs_path);
    }
    if (!costs) {
      costs = measured;
      source = "calibration";
    }
  }
  struct Config
  {
    Engine engine;
    int workers;
    int64_t lead_ns;
    int sched;
    int ring_scale;
  };
  std::vector<Config> configs;
  for (Engine e : a.engines) {
    std::vector<int> ws = a.workers;
    if (e == Engine::Sequential) {
      ws = {1};
    }
    std::vector<int64_t> leads{a.base.lead_ns};
    if (e == Engine::Frontier && !a.leads.empty()) {
      leads = a.leads;
    }
    std::vector<int> scheds{a.base.sched};
    if ((e == Engine::OutOfOrder || e == Engine::Frontier) && !a.scheds.empty()) {
      scheds = a.scheds;
    }
    std::vector<int> scales{a.base.ring_scale};
    if (e == Engine::OutOfOrder && !a.ring_scales.empty()) {
      scales = a.ring_scales;
    }
    for (int sc : scheds) {
      for (int rs : scales) {
        for (int64_t lead : leads) {
          for (int w : ws) {
            configs.push_back({e, w, lead, sc, rs});
          }
        }
      }
    }
  }
  auto run_config = [&](const Config & c) {
      Options opt = a.base;
      opt.engine = c.engine;
      opt.workers = c.workers;
      opt.lead_ns = c.lead_ns;
      opt.sched = c.sched;
      opt.ring_scale = c.ring_scale;
      opt.costs = costs;
      opt.costs_source = source;
      opt.record_costs = false;
      Result res = run_once(opt);
      std::cout << res.to_json() << std::endl;
      runs++;
    };
  if (a.interleave) {
    for (int r = 0; r < a.repeat; ++r) {
      for (size_t k = 0; k < configs.size(); ++k) {
        run_config(configs[r % 2 == 0 ? k : configs.size() - 1 - k]);
      }
    }
  } else {
    for (const auto & c : configs) {
      for (int r = 0; r < a.repeat; ++r) {
        run_config(c);
      }
    }
  }
  if (runs == 0) {
    std::cerr << (a.check ? "CHECK FAILED" : "error") << ": no run was made\n";
    return 1;
  }
  if (a.check) {
    const std::string what = std::to_string(runs) + (runs == 1 ? " run" : " runs") +
      (calibrated ? " and the calibration run" : "");
    if (mismatches) {
      std::cerr << "CHECK FAILED: " << mismatches << " of " << compared <<
        " runs differ from the first run (" << what << ")\n";
      return 1;
    }
    if (compared == 1) {
      std::cerr << "CHECK OK: only one run, nothing to compare\n";
    } else {
      std::cerr << "CHECK OK: " << what << " give the same hash\n";
    }
  }
  return 0;
}

inline void init_ros(int argc, char ** argv)
{
  install_crash_handler();
  setenv("ROS_AUTOMATIC_DISCOVERY_RANGE", "OFF", 1);
  unsetenv("ROS_STATIC_PEERS");
  std::vector<const char *> args;
  args.push_back(argc > 0 ? argv[0] : "let_ros");
  bool has_ros_args = false;
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--ros-args") {
      has_ros_args = true;
      for (int j = i; j < argc; ++j) {
        args.push_back(argv[j]);
      }
      break;
    }
  }
  if (!has_ros_args) {
    args.push_back("--ros-args");
  }
  args.push_back("--disable-rosout-logs");
  args.push_back("--disable-external-lib-logs");
  rclcpp::init(static_cast<int>(args.size()), args.data());
}

}
