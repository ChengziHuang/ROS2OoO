#ifndef LET_ROS__LEAN_REFERENCE_NODES_HPP_
#define LET_ROS__LEAN_REFERENCE_NODES_HPP_

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rclcpp/rclcpp.hpp"
#include "reference_system/msg_types.hpp"
#include "reference_system/nodes/settings.hpp"
#include "reference_system/number_cruncher.hpp"
#include "reference_system/sample_management.hpp"

namespace let_ros
{
namespace lean_nodes
{

inline rclcpp::NodeOptions node_options()
{
  rclcpp::NodeOptions opts;
  opts.start_parameter_services(false);
  opts.start_parameter_event_publisher(false);
  return opts;
}

class Command : public rclcpp::Node
{
public:
  explicit Command(const ::nodes::CommandSettings & settings)
  : Node(settings.node_name, node_options())
  {
    subscription_ = this->create_subscription<message_t>(
      settings.input_topic, 10,
      [this](const message_t::SharedPtr msg) {input_callback(msg);});
  }

private:
  void input_callback(const message_t::SharedPtr input_message)
  {
    uint32_t missed_samples =
      get_missed_samples_and_update_seq_nr(input_message, sequence_number_);
    print_sample_path(this->get_name(), missed_samples, input_message);
  }

private:
  rclcpp::Subscription<message_t>::SharedPtr subscription_;
  uint32_t sequence_number_ = 0;
};

class Cyclic : public rclcpp::Node
{
public:
  explicit Cyclic(const ::nodes::CyclicSettings & settings)
  : Node(settings.node_name, node_options()),
    number_crunch_limit_(settings.number_crunch_limit)
  {
    uint64_t input_number = 0U;
    for (const auto & input_topic : settings.inputs) {
      subscriptions_.emplace_back(
        subscription_t{
            this->create_subscription<message_t>(
              input_topic, 1,
              [this, input_number](const message_t::SharedPtr msg) {
                input_callback(input_number, msg);
              }),
            0, message_t::SharedPtr()});
      ++input_number;
    }
    publisher_ = this->create_publisher<message_t>(settings.output_topic, 1);
    timer_ = this->create_wall_timer(
      settings.cycle_time,
      [this] {timer_callback();});
  }

private:
  void input_callback(
    const uint64_t input_number,
    const message_t::SharedPtr input_message)
  {
    subscriptions_[input_number].cache = input_message;
  }

  void timer_callback()
  {
    uint64_t timestamp = now_as_int();
    auto number_cruncher_result = number_cruncher(number_crunch_limit_);

    auto output_message = publisher_->borrow_loaned_message();
    output_message.get().size = 0;

    uint32_t missed_samples = 0;
    for (auto & s : subscriptions_) {
      if (!s.cache) {
        continue;
      }

      missed_samples +=
        get_missed_samples_and_update_seq_nr(s.cache, s.sequence_number);

      merge_history_into_sample(output_message.get(), s.cache);
      s.cache.reset();
    }
    set_sample(
      this->get_name(), sequence_number_++, missed_samples, timestamp,
      output_message.get());

    output_message.get().data[0] = number_cruncher_result;
    publisher_->publish(std::move(output_message));
  }

private:
  rclcpp::Publisher<message_t>::SharedPtr publisher_;
  rclcpp::TimerBase::SharedPtr timer_;

  struct subscription_t
  {
    rclcpp::Subscription<message_t>::SharedPtr subscription;
    uint32_t sequence_number = 0;
    message_t::SharedPtr cache;
  };

  std::vector<subscription_t> subscriptions_;
  uint64_t number_crunch_limit_;
  uint32_t sequence_number_ = 0;
};

class Fusion : public rclcpp::Node
{
public:
  explicit Fusion(const ::nodes::FusionSettings & settings)
  : Node(settings.node_name, node_options()),
    number_crunch_limit_(settings.number_crunch_limit)
  {
    subscriptions_[0].subscription = this->create_subscription<message_t>(
      settings.input_0, 1,
      [this](const message_t::SharedPtr msg) {input_callback(0U, msg);});

    subscriptions_[1].subscription = this->create_subscription<message_t>(
      settings.input_1, 1,
      [this](const message_t::SharedPtr msg) {input_callback(1U, msg);});
    publisher_ = this->create_publisher<message_t>(settings.output_topic, 1);
  }

private:
  void input_callback(
    const uint64_t input_number,
    const message_t::SharedPtr input_message)
  {
    uint64_t timestamp = now_as_int();
    subscriptions_[input_number].cache = input_message;

    if (!subscriptions_[0].cache || !subscriptions_[1].cache) {
      return;
    }

    auto number_cruncher_result = number_cruncher(number_crunch_limit_);

    auto output_message = publisher_->borrow_loaned_message();

    uint32_t missed_samples =
      get_missed_samples_and_update_seq_nr(
      subscriptions_[0].cache, subscriptions_[0].sequence_number) +
      get_missed_samples_and_update_seq_nr(
      subscriptions_[1].cache,
      subscriptions_[1].sequence_number);

    output_message.get().size = 0;
    merge_history_into_sample(output_message.get(), subscriptions_[0].cache);
    merge_history_into_sample(output_message.get(), subscriptions_[1].cache);
    set_sample(
      this->get_name(), sequence_number_++, missed_samples, timestamp,
      output_message.get());

    output_message.get().data[0] = number_cruncher_result;
    publisher_->publish(std::move(output_message));

    subscriptions_[0].cache.reset();
    subscriptions_[1].cache.reset();
  }

private:
  struct subscription_t
  {
    rclcpp::Subscription<message_t>::SharedPtr subscription;
    uint32_t sequence_number = 0;
    message_t::SharedPtr cache;
  };
  rclcpp::Publisher<message_t>::SharedPtr publisher_;

  subscription_t subscriptions_[2];

  uint64_t number_crunch_limit_;
  uint32_t sequence_number_ = 0;
};

class Intersection : public rclcpp::Node
{
public:
  explicit Intersection(const ::nodes::IntersectionSettings & settings)
  : Node(settings.node_name, node_options())
  {
    for (auto & connection : settings.connections) {
      rclcpp::SubscriptionOptionsWithAllocator<std::allocator<void>> options;
      rclcpp::CallbackGroup::SharedPtr callback_group = this->create_callback_group(
        rclcpp::CallbackGroupType::MutuallyExclusive);
      options.callback_group = callback_group;
      connections_.emplace_back(
        Connection{
            this->create_publisher<message_t>(connection.output_topic, 1),
            this->create_subscription<message_t>(
              connection.input_topic, 1,
              [this, id = connections_.size()](const message_t::SharedPtr msg) {
                input_callback(msg, id);
              }),
            callback_group,
            connection.number_crunch_limit});
    }
  }
  rclcpp::CallbackGroup::SharedPtr get_callback_group_of_subscription(
    const std::string & input_topic)
  {
    for (auto & connection : connections_) {
      if (input_topic == connection.subscription->get_topic_name()) {
        return connection.callback_group;
      }
    }
    RCLCPP_FATAL(get_logger(), "Subscription for topic '%s' not found!", input_topic.c_str());
    std::exit(1);
  }

private:
  void input_callback(
    const message_t::SharedPtr input_message,
    const uint64_t id)
  {
    uint64_t timestamp = now_as_int();
    auto number_cruncher_result =
      number_cruncher(connections_[id].number_crunch_limit);

    auto output_message = connections_[id].publisher->borrow_loaned_message();
    output_message.get().size = 0;
    merge_history_into_sample(output_message.get(), input_message);

    uint32_t missed_samples = get_missed_samples_and_update_seq_nr(
      input_message, connections_[id].input_sequence_number);

    set_sample(
      this->get_name(), connections_[id].sequence_number++,
      missed_samples, timestamp, output_message.get());

    output_message.get().data[0] = number_cruncher_result;
    connections_[id].publisher->publish(std::move(output_message));
  }

private:
  struct Connection
  {
    rclcpp::Publisher<message_t>::SharedPtr publisher;
    rclcpp::Subscription<message_t>::SharedPtr subscription;
    rclcpp::CallbackGroup::SharedPtr callback_group;
    uint64_t number_crunch_limit;
    uint32_t sequence_number = 0;
    uint32_t input_sequence_number = 0;
  };
  std::vector<Connection> connections_;
};

class Sensor : public rclcpp::Node
{
public:
  explicit Sensor(const ::nodes::SensorSettings & settings)
  : Node(settings.node_name, node_options())
  {
    publisher_ = this->create_publisher<message_t>(settings.topic_name, 1);
    timer_ = this->create_wall_timer(
      settings.cycle_time,
      [this] {timer_callback();});
  }

private:
  void timer_callback()
  {
    uint64_t timestamp = now_as_int();
    auto message = publisher_->borrow_loaned_message();
    message.get().size = 0;

    set_sample(
      this->get_name(), sequence_number_++, 0, timestamp,
      message.get());

    publisher_->publish(std::move(message));
  }

private:
  rclcpp::Publisher<message_t>::SharedPtr publisher_;
  rclcpp::TimerBase::SharedPtr timer_;
  uint32_t sequence_number_ = 0;
};

class Transform : public rclcpp::Node
{
public:
  explicit Transform(const ::nodes::TransformSettings & settings)
  : Node(settings.node_name, node_options()),
    number_crunch_limit_(settings.number_crunch_limit)
  {
    subscription_ = this->create_subscription<message_t>(
      settings.input_topic, 1,
      [this](const message_t::SharedPtr msg) {input_callback(msg);});
    publisher_ = this->create_publisher<message_t>(settings.output_topic, 1);
  }

private:
  void input_callback(const message_t::SharedPtr input_message)
  {
    uint64_t timestamp = now_as_int();
    auto number_cruncher_result = number_cruncher(number_crunch_limit_);

    auto output_message = publisher_->borrow_loaned_message();
    output_message.get().size = 0;
    merge_history_into_sample(output_message.get(), input_message);

    uint32_t missed_samples = get_missed_samples_and_update_seq_nr(
      input_message, input_sequence_number_);

    set_sample(
      this->get_name(), sequence_number_++, missed_samples, timestamp,
      output_message.get());

    output_message.get().data[0] = number_cruncher_result;
    publisher_->publish(std::move(output_message));
  }

private:
  rclcpp::Publisher<message_t>::SharedPtr publisher_;
  rclcpp::Subscription<message_t>::SharedPtr subscription_;
  uint64_t number_crunch_limit_;
  uint32_t sequence_number_ = 0;
  uint32_t input_sequence_number_ = 0;
};

struct RclcppSystem
{
  using NodeBaseType = rclcpp::Node;

  using Command = lean_nodes::Command;
  using Cyclic = lean_nodes::Cyclic;
  using Fusion = lean_nodes::Fusion;
  using Intersection = lean_nodes::Intersection;
  using Sensor = lean_nodes::Sensor;
  using Transform = lean_nodes::Transform;
};

}
}

#endif
