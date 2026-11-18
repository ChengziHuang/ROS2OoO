#include <algorithm>
#include <chrono>
#include <queue>
#include <stdexcept>
#include <unordered_map>

#include <rcl/timer.h>

#include "sim_impl.hpp"

namespace let_ros
{

void Simulator::Impl::generate_jobs()
{
  struct Ev
  {
    int64_t t;
    int task;
    int kind;
    int a;
    int b;
    int64_t c;
    int ch;
  };
  auto later = [](const Ev & x, const Ev & y) {
      if (x.t != y.t) {return x.t > y.t;}
      if (x.task != y.task) {return x.task > y.task;}
      if (x.kind != y.kind) {return x.kind > y.kind;}
      if (x.a != y.a) {return x.a > y.a;}
      if (x.b != y.b) {return x.b > y.b;}
      return x.c > y.c;
    };
  std::priority_queue<Ev, std::vector<Ev>, decltype(later)> heap(later);
  const int64_t H = opt.horizon_ns;
  for (size_t i = 0; i < tasks.size(); ++i) {
    for (size_t x = 0; x < tasks[i]->timers.size(); ++x) {
      int64_t first = timer_first_release(static_cast<int>(i), static_cast<int>(x));
      if (first < H) {
        heap.push(Ev{first, static_cast<int>(i), 0, static_cast<int>(x), -1, 0, -1});
      }
    }
  }
  for (auto & c : channels) {
    c.cons_job.assign(1, 0);
  }
  while (!heap.empty()) {
    Ev e = heap.top();
    heap.pop();
    Task & t = *tasks[e.task];
    int64_t k = static_cast<int64_t>(t.jobs.size()) + 1;
    Job j;
    j.release = e.t;
    if (e.kind == 0) {
      j.timer = e.a;
      int64_t next = e.t + t.timers[e.a].period;
      if (next < H) {
        heap.push(Ev{next, e.task, 0, e.a, -1, 0, -1});
      }
    } else {
      j.channel = e.ch;
      j.pjob = e.c;
      channels[e.ch].cons_job[e.c] = k;
    }
    t.jobs.push_back(j);
    const std::vector<char> & allow = e.kind == 0 ?
      t.allow_timer[static_cast<size_t>(e.a)] :
      t.allow_sub[static_cast<size_t>(channels[e.ch].sub)];
    for (int c : t.out_ch) {
      channels[c].cons_job.push_back(0);
      bool may_publish = false;
      for (int pid : channels[c].pubs) {
        may_publish = may_publish || allow[static_cast<size_t>(pubs[pid].target.index)];
      }
      int64_t w = e.t + t.D + channels[c].delay;
      if (may_publish && w < H) {
        heap.push(Ev{w, channels[c].cons, 1, channels[c].sub, e.task, k, c});
      }
    }
  }
}

int Simulator::Impl::ring_for(const Task & t, bool barrier, int64_t window) const
{
  const int64_t span = t.D + max_out_delay(t);
  int64_t need = 0;
  size_t m0 = 0;
  for (size_t k = 0; k < t.jobs.size(); ++k) {
    int64_t r = t.jobs[k].release;
    while (m0 < k) {
      int64_t w = t.jobs[m0].release + span;
      bool ok = barrier ? (w / window < r / window) : (w < r);
      if (ok) {
        ++m0;
      } else {
        break;
      }
    }
    need = std::max<int64_t>(need, static_cast<int64_t>(k - m0));
  }
  int64_t R = std::max<int64_t>(opt.min_ring, need + 1);
  if (opt.ring_scale > 1) {
    R = std::max(R, std::min(R * opt.ring_scale, static_cast<int64_t>(t.jobs.size()) + 1));
  }
  return static_cast<int>(R);
}

void Simulator::Impl::allocate_rings(bool barrier, int64_t window)
{
  std::unordered_map<const rcl_publisher_t *, PubTarget *> targets;
  for (auto & tp : tasks) {
    tp->ring = opt.ring_inf ? static_cast<int>(tp->jobs.size()) + 1 : ring_for(*tp, barrier, window);
    tp->done.store(0);
    tp->claim.store(0);
    tp->active = 0;
  }
  for (auto & p : pubs) {
    p.target.task = p.task;
    p.target.type_support = p.type_support;
    p.target.ring.assign(static_cast<size_t>(tasks[p.task]->ring), Slot{});
    p.target.ring[0].job = 0;
    p.hash = 0;
    p.messages = 0;
    targets[p.handle] = &p.target;
  }
  detail::set_targets(std::move(targets));
}

bool Simulator::Impl::run_job(int i, int64_t k)
{
  Task & t = *tasks[i];
  const Job & J = t.jobs[static_cast<size_t>(k - 1)];
  for (int pid : t.pubs) {
    Slot & s = pubs[pid].target.ring[static_cast<size_t>(k % t.ring)];
    s.job = k;
    s.count = 0;
  }
  JobContext ctx{i, k, J.release, nullptr, nullptr};
  if (J.timer >= 0) {
    ctx.allowed = &t.allow_timer[static_cast<size_t>(J.timer)];
    ctx.used = &t.used_timer[static_cast<size_t>(J.timer)];
  } else {
    size_t sub = static_cast<size_t>(channels[J.channel].sub);
    ctx.allowed = &t.allow_sub[sub];
    ctx.used = &t.used_sub[sub];
  }
  tl_job = &ctx;
  bool active = false;
  try {
    if (J.timer >= 0) {
      active = fire_timer(t.timers[J.timer], J.release);
    } else {
      const Channel & c = channels[J.channel];
      const Task & p = *tasks[c.prod];
      SubInfo & si = t.subs[c.sub];
      for (int pid : c.pubs) {
        const Slot & s = pubs[pid].target.ring[static_cast<size_t>(J.pjob % p.ring)];
        if (s.job != J.pjob) {
          throw std::runtime_error(
                  "internal error: record " + std::to_string(J.pjob) + " of " + p.fqn +
                  " was overwritten before " + t.fqn + " read it");
        }
        active = deliver(t, si, s, J.release, J.release - c.delay) || active;
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
  Job & JW = t.jobs[static_cast<size_t>(k - 1)];
  JW.active = active;
  JW.wrote = false;
  for (int pid : t.pubs) {
    const Slot & s = pubs[pid].target.ring[static_cast<size_t>(k % t.ring)];
    JW.wrote = JW.wrote || s.count > 0;
    account(t, pid, s, J.release, k);
  }
  return true;
}

bool Simulator::Impl::ready(const Task & t, int64_t k) const
{
  const Job & J = t.jobs[static_cast<size_t>(k - 1)];
  if (J.timer < 0) {
    const Channel & c = channels[J.channel];
    if (tasks[c.prod]->done.load() < J.pjob) {
      return false;
    }
  }
  if (k > t.ring) {
    int64_t old = k - t.ring;
    for (int ci : t.out_ch) {
      const Channel & c = channels[ci];
      int64_t m = c.cons_job[static_cast<size_t>(old)];
      if (m > 0 && tasks[c.cons]->done.load() < m) {
        return false;
      }
    }
  }
  return true;
}

void Simulator::Impl::try_enqueue(int i)
{
  Task & t = *tasks[i];
  int64_t k = t.done.load() + 1;
  if (k > static_cast<int64_t>(t.jobs.size())) {
    return;
  }
  int64_t expect = k - 1;
  if (t.claim.load() != expect || !ready(t, k)) {
    return;
  }
  if (t.claim.compare_exchange_strong(expect, k)) {
    push(Item{i, -1, 0, k});
  }
}

bool Simulator::Impl::try_claim(int i, Item & out)
{
  Task & t = *tasks[i];
  int64_t k = t.done.load() + 1;
  if (k > static_cast<int64_t>(t.jobs.size())) {
    return false;
  }
  int64_t expect = k - 1;
  if (t.claim.load() != expect || !ready(t, k)) {
    return false;
  }
  if (!t.claim.compare_exchange_strong(expect, k)) {
    return false;
  }
  wpushed[static_cast<size_t>(tl_worker)].v.fetch_add(1);
  out = Item{i, -1, 0, k};
  return true;
}

void Simulator::Impl::prepare_sequential()
{
  seq_order.clear();
  for (size_t i = 0; i < tasks.size(); ++i) {
    for (size_t k = 0; k < tasks[i]->jobs.size(); ++k) {
      seq_order.push_back(Ref{tasks[i]->jobs[k].release, static_cast<int>(i),
          static_cast<int64_t>(k + 1)});
    }
  }
  std::sort(seq_order.begin(), seq_order.end(), [](const Ref & a, const Ref & b) {
      if (a.r != b.r) {return a.r < b.r;}
      if (a.i != b.i) {return a.i < b.i;}
      return a.k < b.k;
    });
}

void Simulator::Impl::run_sequential()
{
  if (opt.pin_threads) {
    pin_to_core(0);
  }
  const bool costs = opt.record_costs;
  if (costs) {
    seq_costs.assign(tasks.size(), {});
    for (size_t i = 0; i < tasks.size(); ++i) {
      seq_costs[i].reserve(tasks[i]->jobs.size());
    }
  }
  for (auto & ref : seq_order) {
    if (costs) {
      auto a = std::chrono::steady_clock::now();
      if (!run_job(ref.i, ref.k)) {
        return;
      }
      auto b = std::chrono::steady_clock::now();
      seq_costs[static_cast<size_t>(ref.i)].push_back(
        std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count());
    } else if (!run_job(ref.i, ref.k)) {
      return;
    }
    tasks[ref.i]->done.store(ref.k);
  }
}

void Simulator::Impl::prepare_barrier(int64_t window)
{
  std::map<int64_t, std::vector<Range>> by_window;
  for (size_t i = 0; i < tasks.size(); ++i) {
    auto & jobs = tasks[i]->jobs;
    size_t a = 0;
    while (a < jobs.size()) {
      int64_t win = jobs[a].release / window;
      size_t b = a;
      while (b < jobs.size() && jobs[b].release / window == win) {
        ++b;
      }
      by_window[win].push_back(Range{static_cast<int>(i), static_cast<int64_t>(a + 1),
          static_cast<int64_t>(b + 1)});
      a = b;
    }
  }
  win_list.clear();
  win_list.reserve(by_window.size());
  for (auto & kv : by_window) {
    win_list.push_back(std::move(kv.second));
  }
  windows = static_cast<int64_t>(win_list.size());
}

void Simulator::Impl::run_barrier()
{
  const auto & list = win_list;
  int P = std::max(1, opt.workers);
  SpinBarrier barrier(P);
  std::atomic<size_t> next{0};
  auto worker = [&](int w) {
      if (opt.pin_threads) {
        pin_to_core(w);
      }
      for (size_t x = 0; x < list.size(); ++x) {
        auto & ranges = list[x];
        while (!abort.load(std::memory_order_relaxed)) {
          size_t n = next.fetch_add(1);
          if (n >= ranges.size()) {
            break;
          }
          const Range & rg = ranges[n];
          for (int64_t k = rg.lo; k < rg.hi; ++k) {
            if (!run_job(rg.i, k)) {
              break;
            }
            tasks[rg.i]->done.store(k);
          }
        }
        barrier.wait();
        if (abort.load()) {
          break;
        }
        if (w == 0) {
          next.store(0);
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

void Simulator::Impl::run_ooo()
{
  int P = std::max(1, opt.workers);
  queues.clear();
  for (int w = 0; w < P; ++w) {
    queues.push_back(std::make_unique<Queue>());
  }
  int64_t total = 0;
  for (auto & tp : tasks) {
    total += static_cast<int64_t>(tp->jobs.size());
  }
  wpushed = std::make_unique<PaddedCount[]>(static_cast<size_t>(P));
  wdone = std::make_unique<PaddedCount[]>(static_cast<size_t>(P));
  stop.store(false);
  for (size_t i = 0; i < tasks.size(); ++i) {
    try_enqueue(static_cast<int>(i));
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
        Task & t = *tasks[job.task];
        if (!run_job(job.task, job.k)) {
          break;
        }
        t.done.store(job.k);
        if (keep) {
          have = try_claim(job.task, next);
          for (int nb : t.neighbors) {
            if (nb != job.task) {
              try_enqueue(nb);
            }
          }
        } else {
          for (int nb : t.neighbors) {
            try_enqueue(nb);
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

void Simulator::Impl::dump_dag(const char * path) const
{
  std::ofstream out(path);
  for (size_t i = 0; i < tasks.size(); ++i) {
    for (size_t k = 0; k < tasks[i]->jobs.size(); ++k) {
      const Job & J = tasks[i]->jobs[k];
      int pt = J.timer >= 0 ? -1 : channels[static_cast<size_t>(J.channel)].prod;
      out << i << " " << k + 1 << " " << J.release << " " << J.active << " " << pt << " "
          << J.pjob << " " << J.wrote << " " << tasks[i]->name << " " << tasks[i]->fqn << "\n";
    }
  }
}

}
