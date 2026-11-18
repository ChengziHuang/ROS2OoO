#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <queue>
#include <stdexcept>
#include <unordered_map>

#include "sim_impl.hpp"

namespace let_ros
{

namespace
{

constexpr int64_t kNever = std::numeric_limits<int64_t>::max() / 4;

struct FEvent
{
  int64_t t = kNever;
  int kind = 2;
  int a = 0;
  int b = 0;
  int64_t c = 0;
  int input = -1;
};

bool before(const FEvent & x, const FEvent & y)
{
  if (x.t != y.t) {return x.t < y.t;}
  if (x.kind != y.kind) {return x.kind < y.kind;}
  if (x.a != y.a) {return x.a < y.a;}
  if (x.b != y.b) {return x.b < y.b;}
  return x.c < y.c;
}

std::vector<std::vector<std::array<int64_t, 5>>> f_expect;

}

void Simulator::Impl::f_prepare()
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
  ftasks.clear();
  for (size_t i = 0; i < tasks.size(); ++i) {
    ftasks.push_back(std::make_unique<FTask>());
  }
  ch_input.assign(channels.size(), -1);
  for (size_t i = 0; i < tasks.size(); ++i) {
    Task & t = *tasks[i];
    FTask & F = *ftasks[i];
    t.done.store(0);
    t.claim.store(0);
    t.active = 0;
    for (size_t x = 0; x < t.timers.size(); ++x) {
      F.next_timer.push_back(timer_first_release(static_cast<int>(i), static_cast<int>(x)));
    }
    std::set<int> prods;
    std::set<int> cons;
    for (int c : t.in_ch) {
      ch_input[static_cast<size_t>(c)] = static_cast<int>(F.ins.size());
      FInput in;
      in.ch = c;
      in.prod = channels[static_cast<size_t>(c)].prod;
      in.sub = channels[static_cast<size_t>(c)].sub;
      F.ins.push_back(std::move(in));
      prods.insert(channels[static_cast<size_t>(c)].prod);
    }
    for (int c : t.out_ch) {
      cons.insert(channels[static_cast<size_t>(c)].cons);
    }
    F.producers.assign(prods.begin(), prods.end());
    F.consumers.assign(cons.begin(), cons.end());
    auto writes = [&](const std::vector<char> & allow) {
        std::vector<int> out;
        for (int c : t.out_ch) {
          bool may = false;
          for (int pid : channels[static_cast<size_t>(c)].pubs) {
            may = may || allow[static_cast<size_t>(pubs[static_cast<size_t>(pid)].target.index)];
          }
          if (may) {
            out.push_back(c);
          }
        }
        return out;
      };
    for (auto & a : t.allow_timer) {
      F.out_timer.push_back(writes(a));
    }
    for (auto & a : t.allow_sub) {
      F.out_sub.push_back(writes(a));
    }
    F.sub_inputs.assign(t.subs.size(), {});
    for (size_t x = 0; x < F.ins.size(); ++x) {
      F.sub_inputs[static_cast<size_t>(F.ins[x].sub)].push_back(static_cast<int>(x));
    }
    F.bound.assign(F.ins.size(), 0);
    F.outs = t.out_ch;
    F.writer_timers.assign(F.outs.size(), {});
    F.writer_subs.assign(F.outs.size(), {});
    for (size_t o = 0; o < F.outs.size(); ++o) {
      for (size_t x = 0; x < F.out_timer.size(); ++x) {
        const auto & w = F.out_timer[x];
        if (std::find(w.begin(), w.end(), F.outs[o]) != w.end()) {
          F.writer_timers[o].push_back(static_cast<int>(x));
        }
      }
      for (size_t y = 0; y < F.out_sub.size(); ++y) {
        const auto & w = F.out_sub[y];
        if (std::find(w.begin(), w.end(), F.outs[o]) != w.end()) {
          F.writer_subs[o].push_back(static_cast<int>(y));
        }
      }
    }
    F.cert_own.assign(F.outs.size(), 0);
    F.cur.assign(t.pubs.size(), nullptr);
    F.cur_fslot.assign(t.pubs.size(), nullptr);
    F.lb_timer.assign(t.timers.size(), kNever);
    F.lb_sub.assign(t.subs.size(), kNever);
  }
  chan_cert = std::make_unique<PaddedCount[]>(channels.size());
  f_nulls.assign(tasks.size(), 0);
  finished_tasks.store(0);
  f_expect.clear();
  if (std::getenv("LET_ROS_SELFCHECK")) {
    f_expect.resize(tasks.size());
    for (size_t i = 0; i < tasks.size(); ++i) {
      const Task & t = *tasks[i];
      if (opt.model == Model::Calendar) {
        for (const Job & J : t.jobs) {
          if (J.timer >= 0) {
            f_expect[i].push_back({J.release, 0, J.timer, 0, 0});
          } else {
            const Channel & ch = channels[static_cast<size_t>(J.channel)];
            f_expect[i].push_back({J.release, 1, ch.sub, ch.prod, J.pjob});
          }
        }
      } else {
        for (auto & pm : s_job_list(t)) {
          const Stream & S = streams[static_cast<size_t>(t.streams[static_cast<size_t>(pm.first)])];
          if (S.timer >= 0) {
            f_expect[i].push_back({S.release(pm.second), 0, S.timer, 0, 0});
          } else {
            const Channel & ch = channels[static_cast<size_t>(S.channel)];
            f_expect[i].push_back({S.release(pm.second), 1, ch.sub, ch.prod,
                s_index(S.parent, pm.second)});
          }
        }
      }
    }
  }
  detail::set_targets(std::move(targets));
}

FSlot * Simulator::Impl::f_slot(int i)
{
  FTask & F = *ftasks[static_cast<size_t>(i)];
  {
    std::lock_guard<std::mutex> lock(F.pool_m);
    if (!F.free_slots.empty()) {
      FSlot * fs = F.free_slots.back();
      F.free_slots.pop_back();
      return fs;
    }
  }
  F.slots.push_back(std::make_unique<FSlot>());
  F.slots.back()->owner = i;
  return F.slots.back().get();
}

void Simulator::Impl::f_release(FSlot * fs)
{
  if (fs->refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    FTask & F = *ftasks[static_cast<size_t>(fs->owner)];
    std::lock_guard<std::mutex> lock(F.pool_m);
    F.free_slots.push_back(fs);
  }
}

void Simulator::Impl::f_schedule(int i)
{
  FTask & F = *ftasks[static_cast<size_t>(i)];
  int s = F.state.load();
  while (true) {
    if (s == 0) {
      if (F.state.compare_exchange_weak(s, 1)) {
        push(Item{i, -1, 0, 0});
        return;
      }
    } else if (s == 1) {
      if (F.state.compare_exchange_weak(s, 2)) {
        return;
      }
    } else {
      return;
    }
  }
}

void Simulator::Impl::f_process(int i)
{
  FTask & F = *ftasks[static_cast<size_t>(i)];
  while (true) {
    F.state.store(1);
    f_step(i);
    int expect = 1;
    if (F.state.compare_exchange_strong(expect, 0)) {
      return;
    }
  }
}

void Simulator::Impl::f_publish(int i, const std::vector<int> * wrote)
{
  Task & t = *tasks[static_cast<size_t>(i)];
  FTask & F = *ftasks[static_cast<size_t>(i)];
  const int64_t H = opt.horizon_ns;
  int64_t front = kNever;
  for (size_t x = 0; x < F.next_timer.size(); ++x) {
    F.lb_timer[x] = F.next_timer[x] < H ? F.next_timer[x] : kNever;
    front = std::min(front, F.lb_timer[x]);
  }
  for (size_t y = 0; y < F.sub_inputs.size(); ++y) {
    int64_t b = kNever;
    for (int x : F.sub_inputs[y]) {
      const FInput & in = F.ins[static_cast<size_t>(x)];
      b = std::min(b, std::max(F.bound[static_cast<size_t>(x)], in.last_w));
      if (!in.q.empty()) {
        b = std::min(b, in.q.front().w);
      }
    }
    F.lb_sub[y] = b;
    front = std::min(front, b);
  }
  if (front > F.front_own) {
    F.front_own = front;
    F.front.store(front);
    if (opt.lead_ns >= 0) {
      for (int p : F.producers) {
        if (p != i) {
          f_schedule(p);
        }
      }
    }
  }
  for (size_t o = 0; o < F.outs.size(); ++o) {
    int64_t f = kNever;
    for (int x : F.writer_timers[o]) {
      f = std::min(f, F.lb_timer[static_cast<size_t>(x)]);
    }
    for (int y : F.writer_subs[o]) {
      f = std::min(f, F.lb_sub[static_cast<size_t>(y)]);
    }
    const int c = F.outs[o];
    const int64_t cert = f >= H ? kNever : f + t.D + channels[static_cast<size_t>(c)].delay;
    if (cert > F.cert_own[o]) {
      F.cert_own[o] = cert;
      chan_cert[static_cast<size_t>(c)].v.store(cert);
      if (!wrote || std::find(wrote->begin(), wrote->end(), c) == wrote->end()) {
        f_nulls[static_cast<size_t>(i)]++;
      }
      f_schedule(channels[static_cast<size_t>(c)].cons);
    }
  }
  if (front >= H && !F.finished) {
    F.finished = true;
    finished_tasks.fetch_add(1);
  }
}

bool Simulator::Impl::f_run_job(int i, int kind, int a, int input, int64_t release)
{
  Task & t = *tasks[static_cast<size_t>(i)];
  FTask & F = *ftasks[static_cast<size_t>(i)];
  const int64_t k = ++F.k;
  if (!f_expect.empty()) {
    const auto & ex = f_expect[static_cast<size_t>(i)];
    int64_t b = 0;
    int64_t c = 0;
    if (kind == 1) {
      b = F.ins[static_cast<size_t>(input)].prod;
      c = F.ins[static_cast<size_t>(input)].q.front().pjob;
    }
    std::array<int64_t, 5> got{release, kind, a, b, c};
    if (k > static_cast<int64_t>(ex.size()) || ex[static_cast<size_t>(k - 1)] != got) {
      fail("internal error: job " + std::to_string(k) + " of " + t.fqn +
        " in the frontier engine is not the job of the closed form");
      return false;
    }
  }
  const std::vector<char> & allow = kind == 0 ? t.allow_timer[static_cast<size_t>(a)] :
    t.allow_sub[static_cast<size_t>(a)];
  std::vector<char> & used = kind == 0 ? t.used_timer[static_cast<size_t>(a)] :
    t.used_sub[static_cast<size_t>(a)];
  for (size_t x = 0; x < t.pubs.size(); ++x) {
    if (allow[x]) {
      FSlot * fs = f_slot(i);
      fs->s.job = k;
      fs->s.count = 0;
      F.cur_fslot[x] = fs;
      F.cur[x] = &fs->s;
    } else {
      F.cur_fslot[x] = nullptr;
      F.cur[x] = nullptr;
    }
  }
  JobContext ctx{i, k, release, &allow, &used, F.cur.data()};
  tl_job = &ctx;
  bool active = false;
  try {
    if (kind == 0) {
      active = fire_timer(t.timers[static_cast<size_t>(a)], release);
      F.next_timer[static_cast<size_t>(a)] += t.timers[static_cast<size_t>(a)].period;
    } else {
      FInput & in = F.ins[static_cast<size_t>(input)];
      FRecord rec = std::move(in.q.front());
      in.q.pop_front();
      in.last_w = rec.w;
      const Channel & ch = channels[static_cast<size_t>(in.ch)];
      SubInfo & si = t.subs[static_cast<size_t>(ch.sub)];
      for (int x = 0; x < rec.n; ++x) {
        if (FSlot * d = rec.at(x)) {
          active = deliver(t, si, d->s, release, release - ch.delay) || active;
        }
      }
      for (int x = 0; x < rec.n; ++x) {
        if (FSlot * d = rec.at(x)) {
          f_release(d);
        }
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
    if (F.cur_fslot[x]) {
      account(t, t.pubs[x], F.cur_fslot[x]->s, release, k);
    }
  }
  const int64_t w = release + t.D;
  const std::vector<int> & chans = kind == 0 ? F.out_timer[static_cast<size_t>(a)] :
    F.out_sub[static_cast<size_t>(a)];
  auto sends = [&](int c) {
      return w + channels[static_cast<size_t>(c)].delay < opt.horizon_ns;
    };
  for (size_t x = 0; x < t.pubs.size(); ++x) {
    if (FSlot * fs = F.cur_fslot[x]) {
      int refs = 0;
      for (int c : chans) {
        if (!sends(c)) {
          continue;
        }
        for (int pid : channels[static_cast<size_t>(c)].pubs) {
          refs += pubs[static_cast<size_t>(pid)].target.index == static_cast<int>(x) ? 1 : 0;
        }
      }
      fs->refs.store(refs, std::memory_order_relaxed);
      if (refs == 0) {
        std::lock_guard<std::mutex> lock(F.pool_m);
        F.free_slots.push_back(fs);
        F.cur_fslot[x] = nullptr;
      }
    }
  }
  for (int c : chans) {
    if (sends(c)) {
      const Channel & ch = channels[static_cast<size_t>(c)];
      FRecord rec;
      rec.w = w + ch.delay;
      rec.pjob = k;
      rec.n = static_cast<int>(ch.pubs.size());
      for (int x = 0; x < rec.n; ++x) {
        rec.put(x, F.cur_fslot[static_cast<size_t>(
            pubs[static_cast<size_t>(ch.pubs[static_cast<size_t>(x)])].target.index)]);
      }
      FTask & C = *ftasks[static_cast<size_t>(ch.cons)];
      std::lock_guard<std::mutex> lock(C.inbox_m);
      C.inbox.emplace_back(ch_input[static_cast<size_t>(c)], std::move(rec));
    }
  }
  t.done.store(k);
  return true;
}

void Simulator::Impl::f_step(int i)
{
  FTask & F = *ftasks[static_cast<size_t>(i)];
  const int64_t H = opt.horizon_ns;
  while (!abort.load(std::memory_order_relaxed)) {
    for (size_t x = 0; x < F.ins.size(); ++x) {
      F.bound[x] = chan_cert[static_cast<size_t>(F.ins[x].ch)].v.load();
    }
    {
      std::lock_guard<std::mutex> lock(F.inbox_m);
      for (auto & e : F.inbox) {
        F.ins[static_cast<size_t>(e.first)].q.push_back(std::move(e.second));
      }
      F.inbox.clear();
    }
    FEvent e;
    for (size_t x = 0; x < F.next_timer.size(); ++x) {
      if (F.next_timer[x] < H) {
        FEvent c;
        c.t = F.next_timer[x];
        c.kind = 0;
        c.a = static_cast<int>(x);
        if (before(c, e)) {
          e = c;
        }
      }
    }
    for (size_t x = 0; x < F.ins.size(); ++x) {
      const FInput & in = F.ins[x];
      if (!in.q.empty()) {
        FEvent c;
        c.t = in.q.front().w;
        c.kind = 1;
        c.a = in.sub;
        c.b = in.prod;
        c.c = in.q.front().pjob;
        c.input = static_cast<int>(x);
        if (before(c, e)) {
          e = c;
        }
      }
    }
    bool safe = e.t < H;
    for (size_t x = 0; safe && x < F.ins.size(); ++x) {
      if (static_cast<int>(x) == e.input) {
        continue;
      }
      const FInput & in = F.ins[x];
      bool earlier = e.kind == 1 && (in.sub < e.a || (in.sub == e.a && in.prod < e.b));
      safe = earlier ? F.bound[x] > e.t : F.bound[x] >= e.t;
    }
    if (safe && opt.lead_ns >= 0) {
      const std::vector<int> & chans = e.kind == 0 ? F.out_timer[static_cast<size_t>(e.a)] :
        F.out_sub[static_cast<size_t>(e.a)];
      for (int c : chans) {
        int r = channels[static_cast<size_t>(c)].cons;
        if (r != i && e.t >= ftasks[static_cast<size_t>(r)]->front.load() + opt.lead_ns) {
          safe = false;
          break;
        }
      }
    }
    f_publish(i, nullptr);
    if (!safe) {
      return;
    }
    const std::vector<int> & chans = e.kind == 0 ? F.out_timer[static_cast<size_t>(e.a)] :
      F.out_sub[static_cast<size_t>(e.a)];
    if (!f_run_job(i, e.kind, e.a, e.input, e.t)) {
      return;
    }
    const int64_t w = e.t + tasks[static_cast<size_t>(i)]->D;
    F.sent.clear();
    for (int c : chans) {
      if (w + channels[static_cast<size_t>(c)].delay < H) {
        F.sent.push_back(c);
      }
    }
    f_publish(i, &F.sent);
  }
}

bool Simulator::Impl::f_idle_check(bool & marked)
{
  if (!marked) {
    marked = true;
    idle_workers.fetch_add(1);
  }
  const int64_t n = static_cast<int64_t>(tasks.size());
  if (idle_workers.load() == std::max(1, opt.workers) && finished_tasks.load() < n) {
    for (int w = 0; w < static_cast<int>(queues.size()); ++w) {
      std::lock_guard<std::mutex> lock(queues[static_cast<size_t>(w)]->m);
      if (!queues[static_cast<size_t>(w)]->q.empty()) {
        return true;
      }
    }
    if (idle_workers.load() == std::max(1, opt.workers) && finished_tasks.load() < n) {
      fail("deadlock in the frontier engine: " + std::to_string(finished_tasks.load()) + " of " +
        std::to_string(n) + " tasks finished");
      return false;
    }
  }
  return true;
}

void Simulator::Impl::f_run()
{
  const int P = std::max(1, opt.workers);
  queues.clear();
  for (int w = 0; w < P; ++w) {
    queues.push_back(std::make_unique<Queue>());
  }
  wpushed = std::make_unique<PaddedCount[]>(static_cast<size_t>(P));
  idle_workers.store(0);
  const int64_t n = static_cast<int64_t>(tasks.size());
  for (size_t i = 0; i < tasks.size(); ++i) {
    f_schedule(static_cast<int>(i));
  }
  auto worker = [&](int w) {
      tl_worker = w;
      if (opt.pin_threads) {
        pin_to_core(w);
      }
      Item item;
      int idle = 0;
      bool marked = false;
      while (finished_tasks.load(std::memory_order_relaxed) < n &&
        !abort.load(std::memory_order_relaxed))
      {
        if (marked) {
          marked = false;
          idle_workers.fetch_sub(1);
        }
        if (!pop(w, item)) {
          if (!f_idle_check(marked)) {
            break;
          }
          if (opt.sched == 1) {
            if (idle < (1 << 30)) {
              ++idle;
            }
            back_off(idle);
          } else if (++idle > 64) {
            std::this_thread::yield();
          }
          continue;
        }
        idle = 0;
        f_process(item.task);
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
  for (auto & F : ftasks) {
    bool left = !F->inbox.empty();
    for (auto & in : F->ins) {
      left = left || !in.q.empty();
    }
    if (left && error.empty()) {
      fail("internal error: the frontier engine left records unread");
    }
  }
}

void Simulator::Impl::st_prepare()
{
  const bool calendar = opt.model == Model::Calendar;
  auto njobs = [&](size_t i) {
      return calendar ? static_cast<int64_t>(tasks[i]->jobs.size()) : tasks[i]->total;
    };
  std::vector<int64_t> base(tasks.size() + 1, 0);
  for (size_t i = 0; i < tasks.size(); ++i) {
    base[i + 1] = base[i] + njobs(i);
  }
  const int64_t J = base[tasks.size()];
  st_jobs.assign(static_cast<size_t>(J), SJob{});
  std::vector<int64_t> release(static_cast<size_t>(J), 0);
  st_dep_off.assign(static_cast<size_t>(J) + 1, 0);
  st_deps.clear();
  st_deps.reserve(static_cast<size_t>(J) * 2);
  for (size_t i = 0; i < tasks.size(); ++i) {
    const Task & t = *tasks[i];
    const int nt = static_cast<int>(t.timers.size());
    std::vector<std::pair<int, int64_t>> list;
    if (!calendar) {
      list = s_job_list(t);
      if (static_cast<int64_t>(list.size()) != t.total) {
        throw std::runtime_error("internal error: job list of " + t.fqn);
      }
    }
    for (int64_t x = 0; x < njobs(i); ++x) {
      const int64_t g = base[i] + x;
      SJob & j = st_jobs[static_cast<size_t>(g)];
      j.task = static_cast<int>(i);
      j.k = x + 1;
      if (j.k > 1) {
        st_deps.emplace_back(j.task, j.k - 1);
      }
      if (calendar) {
        const Job & J0 = t.jobs[static_cast<size_t>(x)];
        release[static_cast<size_t>(g)] = J0.release;
        j.cb = J0.timer >= 0 ? J0.timer : nt + channels[static_cast<size_t>(J0.channel)].sub;
        if (J0.timer < 0) {
          st_deps.emplace_back(channels[static_cast<size_t>(J0.channel)].prod, J0.pjob);
        }
        if (j.k > t.ring) {
          const int64_t old = j.k - t.ring;
          for (int c : t.out_ch) {
            const Channel & ch = channels[static_cast<size_t>(c)];
            int64_t mjob = ch.cons_job[static_cast<size_t>(old)];
            if (mjob > 0) {
              st_deps.emplace_back(ch.cons, mjob);
            }
          }
        }
      } else {
        j.pos = list[static_cast<size_t>(x)].first;
        j.m = list[static_cast<size_t>(x)].second;
        const int sid = t.streams[static_cast<size_t>(j.pos)];
        const Stream & S = streams[static_cast<size_t>(sid)];
        release[static_cast<size_t>(g)] = S.release(j.m);
        j.cb = S.timer >= 0 ? S.timer : nt + channels[static_cast<size_t>(S.channel)].sub;
        if (S.parent >= 0) {
          st_deps.emplace_back(streams[static_cast<size_t>(S.parent)].task, s_index(S.parent, j.m));
        }
        if (j.m >= S.K) {
          for (int c : S.children) {
            int64_t idx = s_index(c, j.m - S.K);
            if (idx > 0) {
              st_deps.emplace_back(streams[static_cast<size_t>(c)].task, idx);
            }
          }
        }
      }
      st_dep_off[static_cast<size_t>(g) + 1] = static_cast<int64_t>(st_deps.size());
    }
  }
  std::vector<int64_t> cost(static_cast<size_t>(J), 0);
  for (size_t i = 0; i < tasks.size(); ++i) {
    const Task & t = *tasks[i];
    const int64_t n = njobs(i);
    if (n == 0) {
      continue;
    }
    auto it = opt.costs->ns.find(t.fqn);
    if (it == opt.costs->ns.end() || static_cast<int64_t>(it->second.size()) != n) {
      throw std::runtime_error(
              "static engine: no measured costs for the " + std::to_string(n) +
              " jobs of " + t.fqn + " (the costs must come from a run of the same nodes, "
              "horizon and options)");
    }
    const std::vector<int64_t> & measured = it->second;
    if (opt.cost_model == CostModel::Job) {
      for (int64_t x = 0; x < n; ++x) {
        cost[static_cast<size_t>(base[i] + x)] = measured[static_cast<size_t>(x)];
      }
    } else {
      std::vector<double> sum(t.timers.size() + t.subs.size(), 0.0);
      std::vector<int64_t> cnt(t.timers.size() + t.subs.size(), 0);
      for (int64_t x = 0; x < n; ++x) {
        const SJob & j = st_jobs[static_cast<size_t>(base[i] + x)];
        sum[static_cast<size_t>(j.cb)] += static_cast<double>(measured[static_cast<size_t>(x)]);
        cnt[static_cast<size_t>(j.cb)]++;
      }
      for (int64_t x = 0; x < n; ++x) {
        const SJob & j = st_jobs[static_cast<size_t>(base[i] + x)];
        cost[static_cast<size_t>(base[i] + x)] =
          static_cast<int64_t>(sum[static_cast<size_t>(j.cb)] / cnt[static_cast<size_t>(j.cb)]);
      }
    }
  }
  std::vector<int64_t> order(static_cast<size_t>(J));
  for (int64_t g = 0; g < J; ++g) {
    order[static_cast<size_t>(g)] = g;
  }
  std::sort(order.begin(), order.end(), [&](int64_t a, int64_t b) {
      if (release[static_cast<size_t>(a)] != release[static_cast<size_t>(b)]) {
        return release[static_cast<size_t>(a)] < release[static_cast<size_t>(b)];
      }
      return a < b;
    });
  auto gid = [&](const std::pair<int, int64_t> & d) {
      return base[static_cast<size_t>(d.first)] + d.second - 1;
    };
  std::vector<int64_t> level(static_cast<size_t>(J), 0);
  std::vector<int64_t> below(static_cast<size_t>(J), 0);
  for (int64_t x = J - 1; x >= 0; --x) {
    int64_t g = order[static_cast<size_t>(x)];
    level[static_cast<size_t>(g)] = cost[static_cast<size_t>(g)] + below[static_cast<size_t>(g)];
    for (int64_t d = st_dep_off[static_cast<size_t>(g)]; d < st_dep_off[static_cast<size_t>(g) + 1];
      ++d)
    {
      int64_t p = gid(st_deps[static_cast<size_t>(d)]);
      below[static_cast<size_t>(p)] = std::max(below[static_cast<size_t>(p)],
          level[static_cast<size_t>(g)]);
    }
  }
  std::vector<int64_t> succ_off(static_cast<size_t>(J) + 1, 0);
  std::vector<int64_t> indeg(static_cast<size_t>(J), 0);
  for (int64_t g = 0; g < J; ++g) {
    for (int64_t d = st_dep_off[static_cast<size_t>(g)]; d < st_dep_off[static_cast<size_t>(g) + 1];
      ++d)
    {
      succ_off[static_cast<size_t>(gid(st_deps[static_cast<size_t>(d)])) + 1]++;
      indeg[static_cast<size_t>(g)]++;
    }
  }
  for (int64_t g = 0; g < J; ++g) {
    succ_off[static_cast<size_t>(g) + 1] += succ_off[static_cast<size_t>(g)];
  }
  std::vector<int64_t> succ(static_cast<size_t>(succ_off[static_cast<size_t>(J)]));
  {
    std::vector<int64_t> fill(succ_off.begin(), succ_off.end() - 1);
    for (int64_t g = 0; g < J; ++g) {
      for (int64_t d = st_dep_off[static_cast<size_t>(g)];
        d < st_dep_off[static_cast<size_t>(g) + 1]; ++d)
      {
        succ[static_cast<size_t>(fill[static_cast<size_t>(gid(st_deps[static_cast<size_t>(d)]))]++)] =
          g;
      }
    }
  }
  const int P = std::max(1, opt.workers);
  std::vector<int64_t> ready_at(static_cast<size_t>(J), 0);
  auto by_ready = [&](int64_t a, int64_t b) {
      if (ready_at[static_cast<size_t>(a)] != ready_at[static_cast<size_t>(b)]) {
        return ready_at[static_cast<size_t>(a)] > ready_at[static_cast<size_t>(b)];
      }
      return a > b;
    };
  auto by_level = [&](int64_t a, int64_t b) {
      if (level[static_cast<size_t>(a)] != level[static_cast<size_t>(b)]) {
        return level[static_cast<size_t>(a)] < level[static_cast<size_t>(b)];
      }
      return a > b;
    };
  std::priority_queue<int64_t, std::vector<int64_t>, decltype(by_ready)> pending(by_ready);
  std::priority_queue<int64_t, std::vector<int64_t>, decltype(by_level)> ready(by_level);
  using Free = std::pair<int64_t, int>;
  std::priority_queue<Free, std::vector<Free>, std::greater<Free>> free_at;
  for (int w = 0; w < P; ++w) {
    free_at.push({0, w});
  }
  for (int64_t g = 0; g < J; ++g) {
    if (indeg[static_cast<size_t>(g)] == 0) {
      pending.push(g);
    }
  }
  st_lists.assign(static_cast<size_t>(P), {});
  int64_t makespan = 0;
  for (int64_t done = 0; done < J; ++done) {
    auto [tw, w] = free_at.top();
    free_at.pop();
    while (!pending.empty() && ready_at[static_cast<size_t>(pending.top())] <= tw) {
      ready.push(pending.top());
      pending.pop();
    }
    if (ready.empty()) {
      if (pending.empty()) {
        throw std::runtime_error("internal error: the job graph of the static engine has a cycle");
      }
      tw = ready_at[static_cast<size_t>(pending.top())];
      while (!pending.empty() && ready_at[static_cast<size_t>(pending.top())] <= tw) {
        ready.push(pending.top());
        pending.pop();
      }
    }
    int64_t g = ready.top();
    ready.pop();
    int64_t start = std::max(tw, ready_at[static_cast<size_t>(g)]);
    int64_t finish = start + cost[static_cast<size_t>(g)];
    makespan = std::max(makespan, finish);
    st_lists[static_cast<size_t>(w)].push_back(g);
    free_at.push({finish, w});
    for (int64_t s = succ_off[static_cast<size_t>(g)]; s < succ_off[static_cast<size_t>(g) + 1]; ++s) {
      int64_t q = succ[static_cast<size_t>(s)];
      ready_at[static_cast<size_t>(q)] = std::max(ready_at[static_cast<size_t>(q)], finish);
      if (--indeg[static_cast<size_t>(q)] == 0) {
        pending.push(q);
      }
    }
  }
  st_est = static_cast<double>(makespan) * 1e-9;
}

void Simulator::Impl::st_run()
{
  const int P = std::max(1, opt.workers);
  const bool calendar = opt.model == Model::Calendar;
  SpinBarrier start(P);
  auto worker = [&](int w) {
      tl_worker = w;
      if (opt.pin_threads) {
        pin_to_core(w);
      }
      start.wait();
      for (int64_t g : st_lists[static_cast<size_t>(w)]) {
        if (abort.load(std::memory_order_relaxed)) {
          break;
        }
        for (int64_t d = st_dep_off[static_cast<size_t>(g)];
          d < st_dep_off[static_cast<size_t>(g) + 1]; ++d)
        {
          const auto & dep = st_deps[static_cast<size_t>(d)];
          const std::atomic<int64_t> & done = tasks[static_cast<size_t>(dep.first)]->done;
          int spins = 0;
          while (done.load() < dep.second) {
            if (abort.load(std::memory_order_relaxed)) {
              tl_worker = -1;
              return;
            }
            if (++spins > 256) {
              std::this_thread::yield();
            }
          }
        }
        const SJob & j = st_jobs[static_cast<size_t>(g)];
        const bool ok = calendar ? run_job(j.task, j.k) : s_run_job(j.task, j.pos, j.m, j.k);
        if (!ok) {
          break;
        }
        tasks[static_cast<size_t>(j.task)]->done.store(j.k);
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
