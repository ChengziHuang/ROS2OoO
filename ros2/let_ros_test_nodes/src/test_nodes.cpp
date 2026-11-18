#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <rclcpp/rclcpp.hpp>
#include <rclcpp_components/register_node_macro.hpp>
#include <std_msgs/msg/u_int64_multi_array.hpp>

namespace let_ros_test
{

using Msg = std_msgs::msg::UInt64MultiArray;
using namespace std::chrono_literals;

inline uint64_t mix(uint64_t h, uint64_t v)
{
  h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
  return h * 0xff51afd7ed558ccdULL;
}

inline uint64_t work(uint64_t x, int64_t n)
{
  for (int64_t i = 0; i < n; ++i) {
    x = mix(x, static_cast<uint64_t>(i));
  }
  return x;
}

inline uint64_t digest(const Msg & m)
{
  uint64_t h = m.data.size();
  for (auto v : m.data) {
    h = mix(h, v);
  }
  return h;
}

class Base : public rclcpp::Node
{
public:
  Base(const std::string & name, const rclcpp::NodeOptions & options)
  : Node(name, options)
  {
    work_ = declare_parameter<int64_t>("work", 0);
    jitter_pct_ = declare_parameter<int64_t>("jitter_pct", 0);
    if (jitter_pct_ < 0 || jitter_pct_ > 100) {
      throw std::invalid_argument("jitter_pct must be in [0, 100]");
    }
    heavy_pm_ = declare_parameter<int64_t>("heavy_pm", 0);
    heavy_x_ = declare_parameter<int64_t>("heavy_x", 1);
    if (heavy_pm_ < 0 || heavy_pm_ > 1000) {
      throw std::invalid_argument("heavy_pm must be in [0, 1000]");
    }
    if (heavy_x_ < 1) {
      throw std::invalid_argument("heavy_x must be at least 1");
    }
    state_ = static_cast<uint64_t>(declare_parameter<int64_t>("seed", 1));
  }

protected:
  uint64_t stamp() {return static_cast<uint64_t>(now().nanoseconds());}
  int64_t rounds() const
  {
    int64_t r = work_;
    if (jitter_pct_ != 0 && work_ != 0) {
      double u = static_cast<double>(mix(state_, seq_) >> 11) * (1.0 / 9007199254740992.0);
      double f = 1.0 + static_cast<double>(jitter_pct_) / 100.0 * (2.0 * u - 1.0);
      r = static_cast<int64_t>(static_cast<double>(work_) * f);
    }
    if (heavy_pm_ != 0 && heavy_x_ > 1 && r != 0) {
      uint64_t h = mix(mix(state_, seq_), 0xb5297a4d3d2f1c8bULL);
      if ((h >> 11) % 1000 < static_cast<uint64_t>(heavy_pm_)) {
        r *= heavy_x_;
      }
    }
    return r;
  }
  std::chrono::nanoseconds period_param(int64_t default_ms)
  {
    const int64_t ms = declare_parameter<int64_t>("period_ms", default_ms);
    const int64_t us = declare_parameter<int64_t>("period_us", 0);
    const int64_t ns = declare_parameter<int64_t>("period_ns", 0);
    if (ns > 0) {
      return std::chrono::nanoseconds(ns);
    }
    if (us > 0) {
      return std::chrono::nanoseconds(std::chrono::microseconds(us));
    }
    if (ms <= 0) {
      throw std::invalid_argument("period_ms must be positive");
    }
    return std::chrono::nanoseconds(std::chrono::milliseconds(ms));
  }
  Msg make(uint64_t extra_len)
  {
    Msg m;
    state_ = work(mix(state_, stamp()), rounds());
    m.data.push_back(seq_++);
    m.data.push_back(stamp());
    m.data.push_back(state_);
    for (uint64_t i = 0; i < extra_len; ++i) {
      m.data.push_back(mix(state_, i));
    }
    return m;
  }
  int64_t work_ = 0;
  int64_t jitter_pct_ = 0;
  int64_t heavy_pm_ = 0;
  int64_t heavy_x_ = 1;
  uint64_t state_ = 1;
  uint64_t seq_ = 0;
};

inline rclcpp::IntraProcessSetting ipc_setting(bool on)
{
  return on ? rclcpp::IntraProcessSetting::Enable : rclcpp::IntraProcessSetting::NodeDefault;
}

class Source : public Base
{
public:
  explicit Source(const rclcpp::NodeOptions & options)
  : Base("source", options)
  {
    auto period = period_param(10);
    lazy_ = declare_parameter<bool>("lazy", false);
    payload_ = static_cast<uint64_t>(declare_parameter<int64_t>("payload", 0));
    fail_at_ns_ = declare_parameter<int64_t>("fail_at_ms", -1) * 1000000;
    rclcpp::PublisherOptions popts;
    popts.use_intra_process_comm = ipc_setting(declare_parameter<bool>("ipc", false));
    pub_ = create_publisher<Msg>(declare_parameter<std::string>("topic", "a"), 10, popts);
    timer_ = create_wall_timer(period, [this] {
          if (fail_at_ns_ >= 0 && now().nanoseconds() >= fail_at_ns_) {
            throw std::runtime_error("test failure requested by fail_at_ms");
          }
          if (lazy_ && pub_->get_subscription_count() == 0) {
            return;
          }
          pub_->publish(make(seq_ % 4 + payload_));
        });
  }

private:
  bool lazy_ = false;
  uint64_t payload_ = 0;
  int64_t fail_at_ns_ = -1;
  rclcpp::Publisher<Msg>::SharedPtr pub_;
  rclcpp::TimerBase::SharedPtr timer_;
};

class TimerStamp : public Base
{
public:
  explicit TimerStamp(const rclcpp::NodeOptions & options)
  : Base("timer_stamp", options)
  {
    auto period = std::chrono::milliseconds(declare_parameter<int64_t>("period_ms", 15));
    pub_ = create_publisher<Msg>(declare_parameter<std::string>("topic", "ts"), 10);
    timer_ = create_wall_timer(period, [this](const rclcpp::TimerInfo & info) {
          const int64_t t = now().nanoseconds();
          if (info.expected_call_time.nanoseconds() != t || info.actual_call_time.nanoseconds() != t) {
            throw std::runtime_error("TimerInfo differs from now()");
          }
          Msg m = make(0);
          m.data.push_back(static_cast<uint64_t>(info.expected_call_time.nanoseconds()));
          m.data.push_back(static_cast<uint64_t>(info.actual_call_time.nanoseconds()));
          pub_->publish(m);
        });
  }

private:
  rclcpp::Publisher<Msg>::SharedPtr pub_;
  rclcpp::TimerBase::SharedPtr timer_;
};

class MessageStamp : public Base
{
public:
  explicit MessageStamp(const rclcpp::NodeOptions & options)
  : Base("message_stamp", options)
  {
    const auto source_delay = declare_parameter<int64_t>("source_delay_ns", 10000000);
    const auto transport_delay = declare_parameter<int64_t>("transport_delay_ns", 0);
    pub_ = create_publisher<Msg>(declare_parameter<std::string>("out", "stamped"), 10);
    sub_ = create_subscription<Msg>(declare_parameter<std::string>("in", "samples"), 10,
        [this, source_delay, transport_delay](const Msg::SharedPtr m,
          const rclcpp::MessageInfo & info) {
          const auto & raw = info.get_rmw_message_info();
          if (m->data.size() < 2 ||
            raw.source_timestamp != static_cast<int64_t>(m->data[1]) + source_delay ||
            raw.received_timestamp != raw.source_timestamp + transport_delay ||
            now().nanoseconds() != raw.received_timestamp)
          {
            throw std::runtime_error("transport MessageInfo differs from the logical contract");
          }
          Msg out = *m;
          out.data.push_back(static_cast<uint64_t>(raw.source_timestamp));
          out.data.push_back(static_cast<uint64_t>(raw.received_timestamp));
          pub_->publish(out);
        });
  }

private:
  rclcpp::Publisher<Msg>::SharedPtr pub_;
  rclcpp::Subscription<Msg>::SharedPtr sub_;
};

class Relay : public Base
{
public:
  explicit Relay(const rclcpp::NodeOptions & options)
  : Base("relay", options)
  {
    pub_ = create_publisher<Msg>(declare_parameter<std::string>("out", "b"), 10);
    rclcpp::SubscriptionOptions sopts;
    sopts.use_intra_process_comm = ipc_setting(declare_parameter<bool>("ipc", false));
    sub_ = create_subscription<Msg>(declare_parameter<std::string>("in", "a"), 10,
        [this](const Msg::SharedPtr m) {
          state_ = mix(state_, digest(*m));
          pub_->publish(make(m->data.size() % 3));
        }, sopts);
  }

private:
  rclcpp::Publisher<Msg>::SharedPtr pub_;
  rclcpp::Subscription<Msg>::SharedPtr sub_;
};

class Fuse : public Base
{
public:
  explicit Fuse(const rclcpp::NodeOptions & options)
  : Base("fuse", options)
  {
    pub_ = create_publisher<Msg>(declare_parameter<std::string>("out", "f"), 10);
    for (int i = 0; i < 2; ++i) {
      std::string topic = declare_parameter<std::string>("in" + std::to_string(i), "a");
      subs_.push_back(create_subscription<Msg>(topic, 10,
          [this, i](const Msg::SharedPtr m) {
            cache_[i] = m;
            if (cache_[0] && cache_[1]) {
              state_ = mix(mix(state_, digest(*cache_[0])), digest(*cache_[1]));
              pub_->publish(make(1));
              cache_[0].reset();
              cache_[1].reset();
            }
          }));
    }
  }

private:
  rclcpp::Publisher<Msg>::SharedPtr pub_;
  std::vector<rclcpp::Subscription<Msg>::SharedPtr> subs_;
  Msg::SharedPtr cache_[2];
};

class Sampler : public Base
{
public:
  explicit Sampler(const rclcpp::NodeOptions & options)
  : Base("sampler", options)
  {
    auto period = period_param(20);
    auto inputs = declare_parameter<std::vector<std::string>>("inputs",
        std::vector<std::string>{"a"});
    pub_ = create_publisher<Msg>(declare_parameter<std::string>("out", "s"), 10);
    cache_.resize(inputs.size(), 0);
    for (size_t i = 0; i < inputs.size(); ++i) {
      subs_.push_back(create_subscription<Msg>(inputs[i], 10,
          [this, i](const Msg::SharedPtr m) {cache_[i] = mix(cache_[i], digest(*m));}));
    }
    timer_ = create_wall_timer(period, [this] {
          for (auto c : cache_) {
            state_ = mix(state_, c);
          }
          pub_->publish(make(cache_.size()));
        });
  }

private:
  rclcpp::Publisher<Msg>::SharedPtr pub_;
  std::vector<rclcpp::Subscription<Msg>::SharedPtr> subs_;
  rclcpp::TimerBase::SharedPtr timer_;
  std::vector<uint64_t> cache_;
};

class Gate : public Base
{
public:
  explicit Gate(const rclcpp::NodeOptions & options)
  : Base("gate", options)
  {
    mod_ = static_cast<uint64_t>(declare_parameter<int64_t>("mod", 3));
    pub_ = create_publisher<Msg>(declare_parameter<std::string>("out", "g"), 10);
    sub_ = create_subscription<Msg>(declare_parameter<std::string>("in", "a"), 10,
        [this](const Msg::SharedPtr m) {
          state_ = mix(state_, digest(*m));
          if (state_ % mod_ == 0) {
            pub_->publish(make(0));
          }
        });
  }

private:
  uint64_t mod_ = 3;
  rclcpp::Publisher<Msg>::SharedPtr pub_;
  rclcpp::Subscription<Msg>::SharedPtr sub_;
};

class Burst : public Base
{
public:
  explicit Burst(const rclcpp::NodeOptions & options)
  : Base("burst", options)
  {
    pub_ = create_publisher<Msg>(declare_parameter<std::string>("out", "u"), 10);
    sub_ = create_subscription<Msg>(declare_parameter<std::string>("in", "a"), 10,
        [this](const Msg::SharedPtr m) {
          state_ = mix(state_, digest(*m));
          pub_->publish(make(0));
          pub_->publish(make(2));
        });
  }

private:
  rclcpp::Publisher<Msg>::SharedPtr pub_;
  rclcpp::Subscription<Msg>::SharedPtr sub_;
};

class TwoTimers : public Base
{
public:
  explicit TwoTimers(const rclcpp::NodeOptions & options)
  : Base("two_timers", options)
  {
    auto pa = std::chrono::milliseconds(declare_parameter<int64_t>("period_a_ms", 10));
    auto pb = std::chrono::milliseconds(declare_parameter<int64_t>("period_b_ms", 25));
    pub_ = create_publisher<Msg>(declare_parameter<std::string>("out", "t"), 10);
    ta_ = create_wall_timer(pa, [this] {state_ = mix(state_, 1); pub_->publish(make(1));});
    tb_ = create_timer(pb, [this] {state_ = mix(state_, 2); pub_->publish(make(2));});
  }

private:
  rclcpp::Publisher<Msg>::SharedPtr pub_;
  rclcpp::TimerBase::SharedPtr ta_;
  rclcpp::TimerBase::SharedPtr tb_;
};

class LazyRelay : public Base
{
public:
  explicit LazyRelay(const rclcpp::NodeOptions & options)
  : Base("lazy_relay", options)
  {
    input_ = declare_parameter<std::string>("in", "a");
    rclcpp::PublisherOptions pub_options;
    pub_options.event_callbacks.matched_callback = [this](rclcpp::MatchedInfo &) {
        if (!sub_ && pub_->get_subscription_count() > 0) {
          sub_ = create_subscription<Msg>(input_, 10, [this](const Msg::SharedPtr msg) {
              state_ = mix(state_, digest(*msg));
              pub_->publish(make(0));
            });
        }
      };
    pub_ = create_publisher<Msg>(declare_parameter<std::string>("out", "b"), 10, pub_options);
  }

private:
  std::string input_;
  rclcpp::Publisher<Msg>::SharedPtr pub_;
  rclcpp::Subscription<Msg>::SharedPtr sub_;
};

class Generic : public Base
{
public:
  explicit Generic(const rclcpp::NodeOptions & options)
  : Base("generic", options)
  {
    using Ints = std::vector<int64_t>;
    using Strings = std::vector<std::string>;
    const Ints timers = declare_parameter<Ints>("timers_ns", Ints{});
    const Strings outputs = declare_parameter<Strings>("outputs", Strings{});
    const Ints timer_pub = declare_parameter<Ints>("timer_pub", Ints{});
    const Strings inputs = declare_parameter<Strings>("inputs", Strings{});
    const Ints sub_mode = declare_parameter<Ints>("sub_mode", Ints{});
    const Ints sub_pub = declare_parameter<Ints>("sub_pub", Ints{});
    gate_mod_ = declare_parameter<int64_t>("gate_mod", 3);
    burst_ = declare_parameter<int64_t>("burst", 1);
    payload_ = declare_parameter<int64_t>("payload", 0);
    cache_work_ = declare_parameter<int64_t>("cache_work", 0);
    if (outputs.size() > 62) {
      throw std::invalid_argument("at most 62 outputs");
    }
    const int64_t all = (int64_t{1} << outputs.size()) - 1;
    auto mask_of = [all](const Ints & masks, size_t i, size_t n, const char * what) {
        if (masks.empty()) {
          return all;
        }
        if (masks.size() != n) {
          throw std::invalid_argument(std::string(what) + " needs one mask per entry");
        }
        if (masks[i] < 0 || (masks[i] & ~all) != 0) {
          throw std::invalid_argument(std::string(what) + " names an output that does not exist");
        }
        return masks[i];
      };
    if (gate_mod_ < 1 || burst_ < 1 || payload_ < 0 || cache_work_ < 0) {
      throw std::invalid_argument("need gate_mod >= 1, burst >= 1, payload >= 0, cache_work >= 0");
    }
    if (!sub_mode.empty() && sub_mode.size() != inputs.size()) {
      throw std::invalid_argument("sub_mode needs one mode per input");
    }
    for (auto & t : outputs) {
      pubs_.push_back(create_publisher<Msg>(t, 10));
    }
    cache_.assign(inputs.size(), 0);
    arrived_.assign(inputs.size(), 0);
    for (size_t i = 0; i < timers.size(); ++i) {
      if (timers[i] <= 0) {
        throw std::invalid_argument("timer periods must be positive");
      }
      const int64_t mask = mask_of(timer_pub, i, timers.size(), "timer_pub");
      timers_.push_back(create_wall_timer(std::chrono::nanoseconds(timers[i]), [this, i, mask] {
          state_ = mix(state_, 0x5100 + i);
          for (auto c : cache_) {
            state_ = mix(state_, c);
          }
          emit(mask, cache_.size());
        }));
    }
    for (size_t j = 0; j < inputs.size(); ++j) {
      const int64_t mode = sub_mode.empty() ? 0 : sub_mode[j];
      if (mode < 0 || mode > 3) {
        throw std::invalid_argument("sub_mode must be 0 (cache), 1 (relay), 2 (join) or 3 (gate)");
      }
      if (mode == 2) {
        joins_.push_back(j);
      }
      const int64_t mask = mask_of(sub_pub, j, inputs.size(), "sub_pub");
      subs_.push_back(create_subscription<Msg>(inputs[j], 10,
          [this, j, mode, mask](const Msg::SharedPtr m) {
            const uint64_t d = digest(*m);
            cache_[j] = mix(cache_[j], d);
            state_ = mix(state_, d);
            if (mode == 1) {
              emit(mask, m->data.size() % 3);
            } else if (mode == 2) {
              arrived_[j] = 1;
              bool all_new = true;
              for (size_t k : joins_) {
                all_new = all_new && arrived_[k];
              }
              if (all_new) {
                for (size_t k : joins_) {
                  arrived_[k] = 0;
                }
                emit(mask, joins_.size());
              } else {
                emit(0, 0);
              }
            } else if (mode == 3 && state_ % static_cast<uint64_t>(gate_mod_) == 0) {
              emit(mask, 0);
            } else {
              emit(0, 0);
            }
          }));
    }
  }

private:
  void emit(int64_t mask, uint64_t extra)
  {
    if (mask == 0) {
      if (cache_work_ > 0) {
        state_ = work(state_, cache_work_);
      }
      return;
    }
    for (int64_t b = 0; b < burst_; ++b) {
      for (size_t k = 0; k < pubs_.size(); ++k) {
        if ((mask >> k) & 1) {
          pubs_[k]->publish(make(extra + static_cast<uint64_t>(payload_)));
        }
      }
    }
  }
  int64_t gate_mod_ = 3;
  int64_t burst_ = 1;
  int64_t payload_ = 0;
  int64_t cache_work_ = 0;
  std::vector<rclcpp::Publisher<Msg>::SharedPtr> pubs_;
  std::vector<rclcpp::Subscription<Msg>::SharedPtr> subs_;
  std::vector<rclcpp::TimerBase::SharedPtr> timers_;
  std::vector<uint64_t> cache_;
  std::vector<char> arrived_;
  std::vector<size_t> joins_;
};

}

RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::Source)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::Relay)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::Fuse)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::Sampler)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::Gate)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::Burst)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::TwoTimers)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::TimerStamp)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::MessageStamp)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::LazyRelay)
RCLCPP_COMPONENTS_REGISTER_NODE(let_ros_test::Generic)
