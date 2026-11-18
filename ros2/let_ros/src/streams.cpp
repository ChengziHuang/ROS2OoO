#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <limits>
#include <queue>
#include <stdexcept>
#include <unordered_map>

#include "sim_impl.hpp"

namespace let_ros
{

bool Simulator::Impl::stream_before(int a, int b) const
{
  while (a != b) {
    const Stream & A = streams[static_cast<size_t>(a)];
    const Stream & B = streams[static_cast<size_t>(b)];
    int ka = A.timer >= 0 ? 0 : 1;
    int kb = B.timer >= 0 ? 0 : 1;
    if (ka != kb) {
      return ka < kb;
    }
    if (ka == 0) {
      return A.timer < B.timer;
    }
    const Channel & ca = channels[static_cast<size_t>(A.channel)];
    const Channel & cb = channels[static_cast<size_t>(B.channel)];
    if (ca.sub != cb.sub) {
      return ca.sub < cb.sub;
    }
    if (ca.prod != cb.prod) {
      return ca.prod < cb.prod;
    }
    a = A.parent;
    b = B.parent;
  }
  return false;
}

void Simulator::Impl::build_streams()
{
  streams.clear();
  cyclic = false;
  const int64_t H = opt.horizon_ns;
  size_t callbacks = 0;
  for (auto & tp : tasks) {
    callbacks += tp->timers.size() + tp->subs.size();
  }
  for (size_t i = 0; i < tasks.size(); ++i) {
    Task & t = *tasks[i];
    for (size_t x = 0; x < t.timers.size(); ++x) {
      int64_t first = timer_first_release(static_cast<int>(i), static_cast<int>(x));
      if (first >= H) {
        continue;
      }
      Stream s;
      s.task = static_cast<int>(i);
      s.timer = static_cast<int>(x);
      s.offset = first;
      s.period = t.timers[x].period;
      s.allow = &t.allow_timer[x];
      s.used = &t.used_timer[x];
      streams.push_back(std::move(s));
    }
  }
  auto check_limit = [this]() {
      if (static_cast<int64_t>(streams.size()) > opt.max_streams) {
        throw std::runtime_error(
                "more than " + std::to_string(opt.max_streams) +
                " activation streams (--max-streams). If the activation graph has a cycle in"
                " which every callback may publish, declare publish sets (--publish-sets) or"
                " use --model calendar.");
      }
    };
  check_limit();
  for (size_t sid = 0; sid < streams.size(); ++sid) {
    const int task = streams[sid].task;
    const int64_t write_offset = streams[sid].offset + tasks[task]->D;
    if (write_offset >= H) {
      continue;
    }
    const std::vector<char> & allow = *streams[sid].allow;
    const int64_t period = streams[sid].period;
    const int depth = streams[sid].depth;
    for (int c : tasks[task]->out_ch) {
      const Channel & ch = channels[static_cast<size_t>(c)];
      bool may_publish = false;
      for (int pid : ch.pubs) {
        may_publish = may_publish || allow[static_cast<size_t>(pubs[pid].target.index)];
      }
      if (!may_publish) {
        continue;
      }
      const int64_t child_offset = write_offset + ch.delay;
      if (child_offset >= H) {
        continue;
      }
      Task & ct = *tasks[ch.cons];
      Stream s;
      s.task = ch.cons;
      s.channel = c;
      s.parent = static_cast<int>(sid);
      s.depth = depth + 1;
      s.offset = child_offset;
      s.period = period;
      s.allow = &ct.allow_sub[static_cast<size_t>(ch.sub)];
      s.used = &ct.used_sub[static_cast<size_t>(ch.sub)];
      if (static_cast<size_t>(s.depth) > callbacks) {
        cyclic = true;
      }
      streams.push_back(std::move(s));
      check_limit();
    }
  }
  for (auto & s : streams) {
    s.count = ceil_div(H - s.offset, s.period);
  }
  for (size_t sid = 0; sid < streams.size(); ++sid) {
    if (streams[sid].parent >= 0) {
      streams[static_cast<size_t>(streams[sid].parent)].children.push_back(static_cast<int>(sid));
    }
  }
  for (auto & tp : tasks) {
    tp->streams.clear();
    tp->total = 0;
  }
  for (size_t sid = 0; sid < streams.size(); ++sid) {
    Task & t = *tasks[streams[sid].task];
    t.streams.push_back(static_cast<int>(sid));
    t.total += streams[sid].count;
  }
  for (auto & tp : tasks) {
    std::sort(tp->streams.begin(), tp->streams.end(),
      [this](int a, int b) {return stream_before(a, b);});
    for (size_t p = 0; p < tp->streams.size(); ++p) {
      streams[static_cast<size_t>(tp->streams[p])].rank = static_cast<int>(p);
    }
  }
  static std::atomic<bool> warned{false};
  if (cyclic && !warned.exchange(true)) {
    std::cerr << "let_ros: warning: the activation graph has a cycle, so the number of "
              << "streams (" << streams.size() << ") grows with the horizon. "
              << "Publish sets that leave the cycle avoid this.\n";
  }
}

int64_t Simulator::Impl::stream_child_delay(const Stream & s) const
{
  int64_t m = 0;
  for (int c : s.children) {
    const Stream & C = streams[static_cast<size_t>(c)];
    m = std::max(m, channels[static_cast<size_t>(C.channel)].delay);
  }
  return m;
}

void Simulator::Impl::allocate_stream_rings(bool barrier, int64_t window)
{
  std::unordered_map<const rcl_publisher_t *, PubTarget *> targets;
  for (auto & p : pubs) {
    p.target.task = p.task;
    p.target.type_support = p.type_support;
    p.target.ring.clear();
    p.hash = 0;
    p.messages = 0;
    targets[p.handle] = &p.target;
  }
  for (auto & s : streams) {
    const int64_t span = tasks[s.task]->D + stream_child_delay(s);
    int64_t K = barrier ? ceil_div(span + window, s.period) : span / s.period + 1;
    K = std::max<int64_t>(K, opt.min_ring);
    if (opt.ring_scale > 1) {
      K = std::max(K, std::min(K * opt.ring_scale, std::max<int64_t>(1, s.count)));
    }
    if (opt.ring_inf) {
      K = std::max<int64_t>(1, s.count);
    }
    s.K = static_cast<int>(K);
    const std::vector<char> & allow = *s.allow;
    s.rings.assign(allow.size(), {});
    for (size_t x = 0; x < allow.size(); ++x) {
      if (allow[x]) {
        s.rings[x].assign(static_cast<size_t>(K), Slot{});
      }
    }
  }
  key_mul = 1;
  for (auto & tp : tasks) {
    key_mul = std::max<int64_t>(key_mul, static_cast<int64_t>(tp->streams.size()));
  }
  use_keys = opt.horizon_ns <= (std::numeric_limits<int64_t>::max() - key_mul) / key_mul;
  for (auto & tp : tasks) {
    Task & t = *tp;
    t.done.store(0);
    t.prog.store(-1);
    t.claim.store(0);
    t.active = 0;
    t.next_m.assign(t.streams.size(), 0);
    t.next_heap.clear();
    for (size_t p = 0; p < t.streams.size(); ++p) {
      const Stream & S = streams[static_cast<size_t>(t.streams[p])];
      if (S.count > 0) {
        t.next_heap.emplace_back(S.offset, static_cast<int>(p));
      }
    }
    std::make_heap(t.next_heap.begin(), t.next_heap.end(), std::greater<>());
    t.cur_slots.assign(t.pubs.size(), nullptr);
    auto next = s_next(t);
    t.cand_pos.store(next.first);
    t.cand_m.store(next.second);
  }
  detail::set_targets(std::move(targets));
}

int64_t Simulator::Impl::s_index(int sid, int64_t m) const
{
  const Stream & S = streams[static_cast<size_t>(sid)];
  const Task & t = *tasks[S.task];
  const int64_t r = S.release(m);
  int64_t index = 0;
  for (int id : t.streams) {
    const Stream & T = streams[static_cast<size_t>(id)];
    int64_t c = std::min(T.count, ceil_div(r - T.offset, T.period));
    if (T.rank <= S.rank && r >= T.offset) {
      int64_t d = r - T.offset;
      if (d % T.period == 0 && d / T.period < T.count) {
        c += 1;
      }
    }
    index += c;
  }
  return index;
}

std::vector<std::pair<int, int64_t>> Simulator::Impl::s_job_list(const Task & t) const
{
  std::vector<std::pair<int, int64_t>> out;
  out.reserve(static_cast<size_t>(t.total));
  std::vector<int64_t> next(t.streams.size(), 0);
  while (true) {
    int best = -1;
    int64_t best_r = 0;
    for (size_t p = 0; p < t.streams.size(); ++p) {
      const Stream & S = streams[static_cast<size_t>(t.streams[p])];
      if (next[p] >= S.count) {
        continue;
      }
      int64_t r = S.release(next[p]);
      if (best < 0 || r < best_r) {
        best = static_cast<int>(p);
        best_r = r;
      }
    }
    if (best < 0) {
      break;
    }
    out.emplace_back(best, next[static_cast<size_t>(best)]++);
  }
  return out;
}

std::pair<int, int64_t> Simulator::Impl::s_next(const Task & t) const
{
  if (t.next_heap.empty()) {
    return {-1, 0};
  }
  const int best = t.next_heap.front().second;
  return {best, t.next_m[static_cast<size_t>(best)]};
}

bool Simulator::Impl::s_ready(int i, int pos, int64_t m) const
{
  const Task & t = *tasks[i];
  const int sid = t.streams[static_cast<size_t>(pos)];
  const Stream & S = streams[static_cast<size_t>(sid)];
  if (use_keys) {
    if (S.parent >= 0) {
      const Stream & P = streams[static_cast<size_t>(S.parent)];
      if (tasks[P.task]->prog.load() < s_key(P, m)) {
        return false;
      }
    }
    if (m >= S.K) {
      const int64_t old = m - S.K;
      for (int c : S.children) {
        const Stream & C = streams[static_cast<size_t>(c)];
        if (old < C.count && tasks[C.task]->prog.load() < s_key(C, old)) {
          return false;
        }
      }
    }
    return true;
  }
  if (S.parent >= 0) {
    const Stream & P = streams[static_cast<size_t>(S.parent)];
    if (tasks[P.task]->done.load() < s_index(S.parent, m)) {
      return false;
    }
  }
  if (m >= S.K) {
    const int64_t old = m - S.K;
    for (int c : S.children) {
      const Stream & C = streams[static_cast<size_t>(c)];
      if (tasks[C.task]->done.load() < s_index(c, old)) {
        return false;
      }
    }
  }
  return true;
}

bool Simulator::Impl::s_run_job(int i, int pos, int64_t m, int64_t k)
{
  Task & t = *tasks[i];
  Stream & S = streams[static_cast<size_t>(t.streams[static_cast<size_t>(pos)])];
  const int64_t r = S.release(m);
  for (size_t x = 0; x < t.pubs.size(); ++x) {
    if (S.rings[x].empty()) {
      t.cur_slots[x] = nullptr;
    } else {
      Slot & sl = S.rings[x][static_cast<size_t>(m % S.K)];
      sl.job = m;
      sl.count = 0;
      t.cur_slots[x] = &sl;
    }
  }
  JobContext ctx{i, k, r, S.allow, S.used, t.cur_slots.data()};
  tl_job = &ctx;
  bool active = false;
  try {
    if (S.timer >= 0) {
      active = fire_timer(t.timers[static_cast<size_t>(S.timer)], r);
    } else {
      const Channel & ch = channels[static_cast<size_t>(S.channel)];
      const Stream & P = streams[static_cast<size_t>(S.parent)];
      SubInfo & si = t.subs[static_cast<size_t>(ch.sub)];
      for (int pid : ch.pubs) {
        const auto & ring = P.rings[static_cast<size_t>(pubs[pid].target.index)];
        if (ring.empty()) {
          continue;
        }
        const Slot & sl = ring[static_cast<size_t>(m % P.K)];
        if (sl.job != m) {
          throw std::runtime_error(
                  "internal error: record " + std::to_string(m) + " of a stream of " +
                  tasks[P.task]->fqn + " was overwritten before " + t.fqn + " read it");
        }
        active = deliver(t, si, sl, r, r - ch.delay) || active;
      }
    }
  } catch (const std::exception & e) {
    tl_job = nullptr;
    fail("job " + std::to_string(k) + " of " + t.fqn + ": " + e.what());
    return false;
  }
  tl_job = nullptr;
  if (active) {
    t.active++;
  }
  for (size_t x = 0; x < t.pubs.size(); ++x) {
    if (t.cur_slots[x]) {
      account(t, t.pubs[x], *t.cur_slots[x], r, k);
    }
  }
  return true;
}

void Simulator::Impl::s_complete(int i, int pos, int64_t m, int64_t k)
{
  Task & t = *tasks[i];
  const Stream & S = streams[static_cast<size_t>(t.streams[static_cast<size_t>(pos)])];
  t.next_m[static_cast<size_t>(pos)] = m + 1;
  std::pop_heap(t.next_heap.begin(), t.next_heap.end(), std::greater<>());
  if (m + 1 < S.count) {
    t.next_heap.back().first = S.release(m + 1);
    std::push_heap(t.next_heap.begin(), t.next_heap.end(), std::greater<>());
  } else {
    t.next_heap.pop_back();
  }
  auto next = s_next(t);
  t.cand_pos.store(next.first, std::memory_order_relaxed);
  t.cand_m.store(next.second, std::memory_order_relaxed);
  if (use_keys) {
    t.prog.store(s_key(S, m));
  }
  t.done.store(k);
}

void Simulator::Impl::s_try_enqueue(int i)
{
  Task & t = *tasks[i];
  const int64_t d = t.done.load();
  if (d >= t.total || t.claim.load() != d) {
    return;
  }
  const int pos = t.cand_pos.load(std::memory_order_relaxed);
  const int64_t m = t.cand_m.load(std::memory_order_relaxed);
  if (pos < 0 || pos >= static_cast<int>(t.streams.size()) || m < 0 ||
    m >= streams[static_cast<size_t>(t.streams[static_cast<size_t>(pos)])].count)
  {
    return;
  }
  if (!s_ready(i, pos, m)) {
    return;
  }
  int64_t expect = d;
  if (t.claim.compare_exchange_strong(expect, d + 1)) {
    push(Item{i, pos, m, d + 1});
  }
}

bool Simulator::Impl::s_try_claim(int i, Item & out)
{
  Task & t = *tasks[i];
  const int64_t d = t.done.load();
  if (d >= t.total || t.claim.load() != d) {
    return false;
  }
  const int pos = t.cand_pos.load(std::memory_order_relaxed);
  const int64_t m = t.cand_m.load(std::memory_order_relaxed);
  if (pos < 0 || pos >= static_cast<int>(t.streams.size()) || m < 0 ||
    m >= streams[static_cast<size_t>(t.streams[static_cast<size_t>(pos)])].count)
  {
    return false;
  }
  if (!s_ready(i, pos, m)) {
    return false;
  }
  int64_t expect = d;
  if (!t.claim.compare_exchange_strong(expect, d + 1)) {
    return false;
  }
  wpushed[static_cast<size_t>(tl_worker)].v.fetch_add(1);
  out = Item{i, pos, m, d + 1};
  return true;
}

void Simulator::Impl::s_run_sequential()
{
  if (opt.pin_threads) {
    pin_to_core(0);
  }
  const bool selfcheck = std::getenv("LET_ROS_SELFCHECK") != nullptr;
  const bool costs = opt.record_costs;
  if (costs) {
    seq_costs.assign(tasks.size(), {});
    for (size_t i = 0; i < tasks.size(); ++i) {
      seq_costs[i].reserve(static_cast<size_t>(tasks[i]->total));
    }
  }
  struct E
  {
    int64_t r;
    int i;
    int pos;
    int64_t m;
  };
  auto later = [](const E & a, const E & b) {
      if (a.r != b.r) {return a.r > b.r;}
      if (a.i != b.i) {return a.i > b.i;}
      return a.pos > b.pos;
    };
  std::priority_queue<E, std::vector<E>, decltype(later)> heap(later);
  auto push_next = [&](int i) {
      const Task & t = *tasks[i];
      int pos = t.cand_pos.load(std::memory_order_relaxed);
      if (pos >= 0) {
        int64_t m = t.cand_m.load(std::memory_order_relaxed);
        heap.push(E{streams[static_cast<size_t>(t.streams[static_cast<size_t>(pos)])].release(m),
            i, pos, m});
      }
    };
  for (size_t i = 0; i < tasks.size(); ++i) {
    push_next(static_cast<int>(i));
  }
  while (!heap.empty()) {
    E e = heap.top();
    heap.pop();
    Task & t = *tasks[e.i];
    int64_t k = t.done.load() + 1;
    if (selfcheck) {
      int sid = t.streams[static_cast<size_t>(e.pos)];
      if (s_index(sid, e.m) != k || !s_ready(e.i, e.pos, e.m)) {
        fail("internal error: closed-form index or readiness of job " + std::to_string(k) +
          " of " + t.fqn + " does not match the sequential order");
        return;
      }
    }
    if (costs) {
      auto a = std::chrono::steady_clock::now();
      if (!s_run_job(e.i, e.pos, e.m, k)) {
        return;
      }
      auto b = std::chrono::steady_clock::now();
      seq_costs[static_cast<size_t>(e.i)].push_back(
        std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count());
    } else if (!s_run_job(e.i, e.pos, e.m, k)) {
      return;
    }
    s_complete(e.i, e.pos, e.m, k);
    push_next(e.i);
  }
}

void Simulator::Impl::s_run_barrier(int64_t window)
{
  int P = std::max(1, opt.workers);
  SpinBarrier barrier(P);
  std::atomic<size_t> next{0};
  std::vector<int> list;
  int64_t end = 0;
  bool finished = false;
  windows = 0;
  auto worker = [&](int w) {
      if (opt.pin_threads) {
        pin_to_core(w);
      }
      while (true) {
        if (w == 0) {
          int64_t first = -1;
          for (auto & tp : tasks) {
            int pos = tp->cand_pos.load(std::memory_order_relaxed);
            if (pos >= 0) {
              int64_t r = streams[static_cast<size_t>(tp->streams[static_cast<size_t>(pos)])]
                .release(tp->cand_m.load(std::memory_order_relaxed));
              first = first < 0 ? r : std::min(first, r);
            }
          }
          finished = first < 0 || abort.load();
          if (!finished) {
            end = (first / window + 1) * window;
            list.clear();
            for (size_t i = 0; i < tasks.size(); ++i) {
              const Task & t = *tasks[i];
              int pos = t.cand_pos.load(std::memory_order_relaxed);
              if (pos >= 0 &&
                streams[static_cast<size_t>(t.streams[static_cast<size_t>(pos)])]
                .release(t.cand_m.load(std::memory_order_relaxed)) < end)
              {
                list.push_back(static_cast<int>(i));
              }
            }
            next.store(0);
            windows++;
          }
        }
        barrier.wait();
        if (finished) {
          break;
        }
        while (!abort.load(std::memory_order_relaxed)) {
          size_t n = next.fetch_add(1);
          if (n >= list.size()) {
            break;
          }
          int i = list[n];
          Task & t = *tasks[i];
          while (true) {
            int pos = t.cand_pos.load(std::memory_order_relaxed);
            if (pos < 0) {
              break;
            }
            int64_t m = t.cand_m.load(std::memory_order_relaxed);
            if (streams[static_cast<size_t>(t.streams[static_cast<size_t>(pos)])].release(m) >=
              end)
            {
              break;
            }
            int64_t k = t.done.load() + 1;
            if (!s_run_job(i, pos, m, k)) {
              break;
            }
            s_complete(i, pos, m, k);
          }
        }
        barrier.wait();
      }
    };
  std::vector<std::thread> threads;
  for (int w = 1; w < P; ++w) {
    threads.emplace_back(worker, w);
  }
  worker(0);
  for (auto & th : threads) {
    th.join();
  }
}

void Simulator::Impl::s_run_ooo()
{
  int P = std::max(1, opt.workers);
  queues.clear();
  for (int w = 0; w < P; ++w) {
    queues.push_back(std::make_unique<Queue>());
  }
  int64_t total = 0;
  for (auto & tp : tasks) {
    total += tp->total;
  }
  wpushed = std::make_unique<PaddedCount[]>(static_cast<size_t>(P));
  wdone = std::make_unique<PaddedCount[]>(static_cast<size_t>(P));
  stop.store(false);
  for (size_t i = 0; i < tasks.size(); ++i) {
    s_try_enqueue(static_cast<int>(i));
  }
  auto worker = [&](int w) {
      tl_worker = w;
      if (opt.pin_threads) {
        pin_to_core(w);
      }
      Item job;
      Item next;
      bool have = false;
      const bool keep = opt.sched == 1;
      int idle = 0;
      std::atomic<int64_t> & mine = wdone[static_cast<size_t>(w)].v;
      while (!stop.load(std::memory_order_relaxed) && !abort.load(std::memory_order_relaxed)) {
        if (!have && !pop(w, job)) {
          if (!keep) {
            if (!idle_check(total)) {
              break;
            }
            if (++idle > 64) {
              std::this_thread::yield();
            }
            continue;
          }
          if (idle < (1 << 30)) {
            ++idle;
          }
          if (((idle & (idle - 1)) == 0 || (idle & 63) == 0) && !idle_check(total)) {
            break;
          }
          back_off(idle);
          continue;
        }
        have = false;
        idle = 0;
        if (!s_run_job(job.task, job.pos, job.m, job.k)) {
          break;
        }
        s_complete(job.task, job.pos, job.m, job.k);
        if (keep) {
          have = s_try_claim(job.task, next);
          for (int nb : tasks[job.task]->neighbors) {
            if (nb != job.task) {
              s_try_enqueue(nb);
            }
          }
        } else {
          for (int nb : tasks[job.task]->neighbors) {
            s_try_enqueue(nb);
          }
        }
        mine.store(mine.load(std::memory_order_relaxed) + 1, std::memory_order_release);
        if (have) {
          job = next;
        }
      }
      tl_worker = -1;
    };
  std::vector<std::thread> threads;
  for (int w = 1; w < P; ++w) {
    threads.emplace_back(worker, w);
  }
  worker(0);
  for (auto & th : threads) {
    th.join();
  }
}

}
