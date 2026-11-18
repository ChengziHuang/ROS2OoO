#pragma once

#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <vector>

#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/string.hpp>
#include <std_msgs/msg/u_int64_multi_array.hpp>

namespace let_ros_test
{
using ConformanceMsg = std_msgs::msg::UInt64MultiArray;

inline rclcpp::QoS conformance_qos(rclcpp::Node & node)
{
  const auto depth = node.declare_parameter<int64_t>("depth", 64);
  if (depth < 1) {
    throw std::invalid_argument("conformance depth must be positive");
  }
  rclcpp::QoS qos(static_cast<size_t>(depth));
  if (node.declare_parameter<bool>("reliable", true)) {
    qos.reliable();
  } else {
    qos.best_effort();
  }
  if (node.declare_parameter<bool>("transient", false)) {
    qos.transient_local();
  }
  return qos;
}

class ConformanceSource : public rclcpp::Node
{
public:
  explicit ConformanceSource(const rclcpp::NodeOptions & options)
  : Node("conformance_source", options)
  {
    const auto qos = conformance_qos(*this);
    total_ = declare_parameter<int64_t>("total", 10);
    burst_ = declare_parameter<int64_t>("burst", 1);
    expected_matches_ = declare_parameter<int64_t>("expected_matches", -1);
    if (total_ < 1 || burst_ < 1) {
      throw std::invalid_argument("conformance total and burst must be positive");
    }
    pub_ = create_publisher<ConformanceMsg>("conformance", qos);
    if (declare_parameter<bool>("duplicate_publisher", false)) {
      extra_pub_ = create_publisher<ConformanceMsg>("conformance", qos);
    }
    timer_ = create_wall_timer(std::chrono::milliseconds(10), [this] {
        const auto matches = pub_->get_subscription_count();
        if (expected_matches_ >= 0 && matches != static_cast<size_t>(expected_matches_)) {
          throw std::runtime_error("conformance publisher match count differs from expected");
        }
        for (int64_t n = 0; n < burst_ && sent_ < total_; ++n) {
          ConformanceMsg m;
          m.data = {static_cast<uint64_t>(sent_++), matches};
          pub_->publish(m);
        }
      });
    if (declare_parameter<bool>("initial_publish", false)) {
      ConformanceMsg m;
      m.data = {static_cast<uint64_t>(sent_++), pub_->get_subscription_count()};
      pub_->publish(m);
    }
  }

  int64_t sent() const {return sent_;}
  size_t matches() const {return pub_->get_subscription_count();}

private:
  int64_t total_ = 0;
  int64_t burst_ = 1;
  int64_t sent_ = 0;
  int64_t expected_matches_ = -1;
  rclcpp::Publisher<ConformanceMsg>::SharedPtr pub_;
  rclcpp::Publisher<ConformanceMsg>::SharedPtr extra_pub_;
  rclcpp::TimerBase::SharedPtr timer_;
};

class ConformanceSink : public rclcpp::Node
{
public:
  explicit ConformanceSink(const rclcpp::NodeOptions & options)
  : Node("conformance_sink", options)
  {
    sub_ = create_subscription<ConformanceMsg>("conformance", conformance_qos(*this),
        [this](ConformanceMsg::ConstSharedPtr m) {
          if (m->data.size() != 2) {
            throw std::runtime_error("invalid conformance message");
          }
          received.push_back(m->data[0]);
          std::cout << "CONFORMANCE_RX " << m->data[0] << " " << m->data[1] << std::endl;
        });
  }

  std::vector<uint64_t> received;

private:
  rclcpp::Subscription<ConformanceMsg>::SharedPtr sub_;
};

class ConformanceStringSink : public rclcpp::Node
{
public:
  explicit ConformanceStringSink(const rclcpp::NodeOptions & options)
  : Node("conformance_sink", options)
  {
    sub_ = create_subscription<std_msgs::msg::String>("conformance", conformance_qos(*this),
        [this](std_msgs::msg::String::ConstSharedPtr) {++received;});
  }
  int64_t received = 0;

private:
  rclcpp::Subscription<std_msgs::msg::String>::SharedPtr sub_;
};

class ConformanceOrderSink : public rclcpp::Node
{
public:
  explicit ConformanceOrderSink(const rclcpp::NodeOptions & options)
  : Node("conformance_order_sink", options)
  {
    pub_ = create_publisher<ConformanceMsg>("order_result", 64);
    auto receive = [this](char label, ConformanceMsg::ConstSharedPtr m) {
        const auto sequence = m->data.at(0);
        state = state * 131 + (label == 'A' ? 100 : 200) + sequence;
        order.push_back(std::string(1, label) + std::to_string(sequence));
        ConformanceMsg out;
        out.data = {state};
        pub_->publish(out);
        std::cout << "CONFORMANCE_ORDER " << label << " " << sequence << " " << state << std::endl;
      };
    a_ = create_subscription<ConformanceMsg>("a", 64,
      [receive](ConformanceMsg::ConstSharedPtr m) {receive('A', m);});
    b_ = create_subscription<ConformanceMsg>("b", 64,
      [receive](ConformanceMsg::ConstSharedPtr m) {receive('B', m);});
  }
  uint64_t state = 0;
  std::vector<std::string> order;

private:
  rclcpp::Publisher<ConformanceMsg>::SharedPtr pub_;
  rclcpp::Subscription<ConformanceMsg>::SharedPtr a_, b_;
};
}
