#pragma once

#include <pthread.h>
#include <sched.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <rclcpp/rclcpp.hpp>

#include "let_ros/simulator.hpp"
#include "internal.hpp"

namespace let_ros
{

using detail::JobContext;
using detail::PubTarget;
using detail::Slot;
using detail::tl_job;

inline bool excluded_topic(const std::string & t)
{
  return t == "/parameter_events" || t == "/rosout" || t == "/clock";
}

inline uint64_t mix(uint64_t h, uint64_t v)
{
  h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
  return h;
}

inline uint64_t hash_bytes(uint64_t h, const std::vector<uint8_t> & b)
{
  uint64_t x = 1469598103934665603ULL;
  size_t n = b.size();
  size_t i = 0;
  for (; i + 8 <= n; i += 8) {
    uint64_t w;
    std::memcpy(&w, b.data() + i, 8);
    x = (x ^ w) * 1099511628211ULL;
  }
  for (; i < n; ++i) {
    x = (x ^ b[i]) * 1099511628211ULL;
  }
  return mix(h, mix(x, n));
}

inline int read_int_file(const std::string & path)
{
  std::ifstream in(path);
  int v = -1;
  if (in) {
    in >> v;
  }
  return v;
}

inline const std::vector<int> & allowed_cores()
{
  static const std::vector<int> cores = [] {
      std::vector<int> out;
      cpu_set_t set;
      CPU_ZERO(&set);
      if (sched_getaffinity(0, sizeof(set), &set) != 0) {
        return out;
      }
      std::set<std::pair<int, int>> seen;
      for (int c = 0; c < CPU_SETSIZE; ++c) {
        if (!CPU_ISSET(c, &set)) {
          continue;
        }
        std::string base = "/sys/devices/system/cpu/cpu" + std::to_string(c) + "/topology/";
        int pkg = read_int_file(base + "physical_package_id");
        int core = read_int_file(base + "core_id");
        if (pkg < 0 || core < 0) {
          out.push_back(c);
        } else if (seen.insert({pkg, core}).second) {
          out.push_back(c);
        }
      }
      return out;
    }();
  return cores;
}

inline std::atomic<bool> pin_warned{false};

inline void pin_to_core(int w)
{
  const auto & cores = allowed_cores();
  if (cores.empty()) {
    return;
  }
  if (w >= static_cast<int>(cores.size()) && !pin_warned.exchange(true)) {
    std::cerr << "let_ros: warning: " << w + 1 << " or more workers but only " << cores.size()
              << " allowed physical cores, workers share cores\n";
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cores[static_cast<size_t>(w) % cores.size()], &set);
  if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set) != 0 && !pin_warned.exchange(true)) {
    std::cerr << "let_ros: warning: could not pin a worker thread\n";
  }
}

class SpinBarrier
{
public:
  explicit SpinBarrier(int n)
  : n_(n), left_(n) {}
  void wait()
  {
    bool sense = !sense_.load(std::memory_order_relaxed);
    if (left_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      left_.store(n_, std::memory_order_relaxed);
      sense_.store(sense, std::memory_order_release);
    } else {
      int spins = 0;
      while (sense_.load(std::memory_order_acquire) != sense) {
        if (++spins > 256) {
          std::this_thread::yield();
        }
      }
    }
  }

private:
  int n_;
  std::atomic<int> left_;
  std::atomic<bool> sense_{false};
};

inline thread_local int tl_worker = -1;

inline int64_t ceil_div(int64_t a, int64_t b)
{
  return a <= 0 ? 0 : (a + b - 1) / b;
}

struct TimerInfo
{
  rclcpp::TimerBase::SharedPtr timer;
  int64_t period = 0;
};

struct SubInfo
{
  rclcpp::SubscriptionBase::SharedPtr sub;
  std::string topic;
};

struct Pub
{
  const rcl_publisher_t * handle = nullptr;
  int task = -1;
  std::string topic;
  const rosidl_message_type_support_t * type_support = nullptr;
  PubTarget target;
  uint64_t hash = 0;
  int64_t messages = 0;
};

struct Job
{
  int64_t release = 0;
  int32_t timer = -1;
  int32_t channel = -1;
  int64_t pjob = 0;
  bool active = false;
  bool wrote = false;
};

struct Channel
{
  int prod = -1;
  int cons = -1;
  int sub = -1;
  std::vector<int> pubs;
  std::vector<int64_t> cons_job;
  int64_t delay = 0;
};

struct Stream
{
  int task = -1;
  int timer = -1;
  int channel = -1;
  int parent = -1;
  int depth = 0;
  int64_t offset = 0;
  int64_t period = 0;
  int64_t count = 0;
  int K = 2;
  int rank = 0;
  std::vector<int> children;
  const std::vector<char> * allow = nullptr;
  std::vector<char> * used = nullptr;
  std::vector<std::vector<Slot>> rings;

  int64_t release(int64_t m) const {return offset + m * period;}
};

struct alignas(64) Task
{
  std::string name;
  std::string fqn;
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr base;
  std::shared_ptr<void> keep;
  std::vector<TimerInfo> timers;
  std::vector<SubInfo> subs;
  std::vector<int> pubs;
  std::vector<int> out_ch;
  std::vector<int> in_ch;
  std::vector<int> neighbors;
  int64_t D = -1;
  int64_t active = 0;
  int64_t timer_calls = 0;
  int64_t sub_calls = 0;
  std::vector<std::string> trace;
  std::vector<std::vector<char>> allow_timer, allow_sub, used_timer, used_sub;

  int ring = 2;
  std::vector<Job> jobs;

  std::vector<int> streams;
  int64_t total = 0;
  std::vector<int64_t> next_m;
  std::vector<std::pair<int64_t, int>> next_heap;
  std::vector<Slot *> cur_slots;

  alignas(64) std::atomic<int64_t> done{0};
  std::atomic<int64_t> prog{-1};
  alignas(64) std::atomic<int64_t> claim{0};
  alignas(64) std::atomic<int> cand_pos{-1};
  std::atomic<int64_t> cand_m{0};
};

struct Item
{
  int task = -1;
  int pos = -1;
  int64_t m = 0;
  int64_t k = 0;
};

struct alignas(64) PaddedCount
{
  std::atomic<int64_t> v{0};
};

struct FSlot
{
  Slot s;
  std::atomic<int> refs{0};
  int owner = -1;
};

struct FRecord
{
  int64_t w = 0;
  int64_t pjob = 0;
  int n = 0;
  std::array<FSlot *, 2> inl{{nullptr, nullptr}};
  std::vector<FSlot *> more;
  FSlot * at(int x) const {return x < 2 ? inl[static_cast<size_t>(x)] : more[static_cast<size_t>(x - 2)];}
  void put(int x, FSlot * p)
  {
    if (x < 2) {
      inl[static_cast<size_t>(x)] = p;
    } else {
      more.push_back(p);
    }
  }
};

struct FInput
{
  int ch = -1;
  int prod = -1;
  int sub = -1;
  int64_t last_w = 0;
  std::deque<FRecord> q;
};

struct alignas(64) FTask
{
  std::mutex inbox_m;
  std::vector<std::pair<int, FRecord>> inbox;
  std::mutex pool_m;
  std::vector<FSlot *> free_slots;
  std::vector<std::unique_ptr<FSlot>> slots;
  std::vector<FSlot *> cur_fslot;
  std::vector<int64_t> lb_timer;
  std::vector<int64_t> lb_sub;
  std::vector<FInput> ins;
  std::vector<std::vector<int>> sub_inputs;
  std::vector<int64_t> bound;
  std::vector<int64_t> next_timer;
  std::vector<std::vector<int>> out_timer;
  std::vector<std::vector<int>> out_sub;
  std::vector<int> outs;
  std::vector<std::vector<int>> writer_timers;
  std::vector<std::vector<int>> writer_subs;
  std::vector<int64_t> cert_own;
  std::vector<int> producers;
  std::vector<int> consumers;
  std::vector<Slot *> cur;
  std::vector<int> sent;
  int64_t k = 0;
  int64_t front_own = 0;
  bool finished = false;
  alignas(64) std::atomic<int64_t> front{0};
  alignas(64) std::atomic<int> state{0};
};

struct SJob
{
  int task = -1;
  int pos = -1;
  int64_t m = 0;
  int64_t k = 0;
  int cb = 0;
};

struct Simulator::Impl
{
  Options opt;
  bool tracing = false;
  std::vector<std::unique_ptr<Task>> tasks;
  std::vector<Pub> pubs;
  std::vector<Channel> channels;
  std::vector<Stream> streams;
  int64_t key_mul = 1;
  bool use_keys = true;
  bool graph_built = false;
  bool jobs_built = false;
  bool cyclic = false;
  int64_t services_ignored = 0;
  std::vector<std::string> unfed_subs;

  struct alignas(64) Queue
  {
    std::mutex m;
    std::deque<Item> q;
    std::atomic<int> n{0};
  };
  std::vector<std::unique_ptr<Queue>> queues;
  std::unique_ptr<PaddedCount[]> wpushed;
  std::unique_ptr<PaddedCount[]> wdone;
  int64_t pushed_sum() const;
  int64_t completed_sum() const;
  alignas(64) std::atomic<bool> stop{false};
  alignas(64) std::atomic<int> idle_workers{0};
  bool idle_check(int64_t total);
  alignas(64) std::atomic<uint64_t> rr{0};
  alignas(64) std::atomic<bool> abort{false};
  alignas(64) std::mutex err_mutex;
  std::string error;
  int64_t windows = 0;

  std::vector<std::unique_ptr<FTask>> ftasks;
  std::vector<int> ch_input;
  std::unique_ptr<PaddedCount[]> chan_cert;
  std::vector<int64_t> f_nulls;
  alignas(64) std::atomic<int64_t> finished_tasks{0};

  std::vector<SJob> st_jobs;
  std::vector<int64_t> st_dep_off;
  std::vector<std::pair<int, int64_t>> st_deps;
  std::vector<std::vector<int64_t>> st_lists;
  double st_est = 0.0;

  std::vector<std::vector<int64_t>> seq_costs;

  void build();
  void build_graph();
  bool needs_plan() const;
  void compute_deadlines();
  void compute_link_delays();
  int64_t max_out_delay(const Task & t) const;
  void apply_publish_sets();
  void write_publish_sets(const std::string & path) const;
  int64_t timer_first_release(int task, int timer) const;
  int64_t window_length() const;
  void fail(const std::string & what);
  void push(const Item & item);
  bool pop(int w, Item & item);
  void back_off(int idle) const;
  bool fire_timer(TimerInfo & ti, int64_t release);
  bool deliver(Task & t, SubInfo & si, const Slot & s, int64_t release, int64_t source);
  void account(Task & t, int pid, const Slot & s, int64_t release, int64_t k);
  int64_t ring_slots() const;

  void generate_jobs();
  int ring_for(const Task & t, bool barrier, int64_t window) const;
  void allocate_rings(bool barrier, int64_t window);
  bool run_job(int i, int64_t k);
  bool ready(const Task & t, int64_t k) const;
  void try_enqueue(int i);
  bool try_claim(int i, Item & out);
  struct Ref
  {
    int64_t r;
    int i;
    int64_t k;
  };
  struct Range
  {
    int i;
    int64_t lo;
    int64_t hi;
  };
  std::vector<Ref> seq_order;
  std::vector<std::vector<Range>> win_list;
  void prepare_sequential();
  void prepare_barrier(int64_t window);
  void run_sequential();
  void run_barrier();
  void run_ooo();
  void dump_dag(const char * path) const;

  void build_streams();
  bool stream_before(int a, int b) const;
  int64_t stream_child_delay(const Stream & s) const;
  void allocate_stream_rings(bool barrier, int64_t window);
  int64_t s_index(int sid, int64_t m) const;
  int64_t s_key(const Stream & S, int64_t m) const {return S.release(m) * key_mul + S.rank;}
  std::pair<int, int64_t> s_next(const Task & t) const;
  bool s_ready(int i, int pos, int64_t m) const;
  bool s_run_job(int i, int pos, int64_t m, int64_t k);
  void s_complete(int i, int pos, int64_t m, int64_t k);
  void s_try_enqueue(int i);
  bool s_try_claim(int i, Item & out);
  void s_run_sequential();
  void s_run_barrier(int64_t window);
  void s_run_ooo();
  std::vector<std::pair<int, int64_t>> s_job_list(const Task & t) const;

  void f_prepare();
  void f_run();
  void f_schedule(int i);
  void f_process(int i);
  void f_step(int i);
  bool f_run_job(int i, int kind, int a, int input, int64_t release);
  void f_publish(int i, const std::vector<int> * wrote);
  FSlot * f_slot(int i);
  void f_release(FSlot * fs);
  bool f_idle_check(bool & marked);
  void st_prepare();
  void st_run();
  int64_t payload_bytes() const;
};

}
