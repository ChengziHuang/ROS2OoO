#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include <rcl/publisher.h>
#include <rcl/node.h>
#include <rosidl_runtime_c/message_type_support_struct.h>
#include <rclcpp/node_interfaces/node_base_interface.hpp>
#include <rclcpp/subscription_base.hpp>

namespace let_ros
{
namespace detail
{

struct PublisherInfo
{
  const rcl_publisher_t * handle = nullptr;
  const rcl_node_t * node = nullptr;
  const rosidl_message_type_support_t * type_support = nullptr;
  std::string topic;
  uint64_t seq = 0;
  uint64_t outside_messages = 0;
};

std::vector<PublisherInfo> registered_publishers();

bool topic_endpoints_compatible(
  const rcl_publisher_t * publisher, const rosidl_message_type_support_t * type_support,
  const rclcpp::SubscriptionBase & subscription);

void initialize_component_connections(
  const std::vector<rclcpp::node_interfaces::NodeBaseInterface::SharedPtr> & nodes);

struct Slot
{
  int64_t job = -1;
  int count = 0;
  std::vector<std::vector<uint8_t>> msgs;
};

struct PubTarget
{
  int task = -1;
  int index = -1;
  size_t subscribers = 0;
  const rosidl_message_type_support_t * type_support = nullptr;
  std::vector<Slot> ring;
};

struct JobContext
{
  int task = -1;
  int64_t job = 0;
  int64_t release_ns = 0;
  const std::vector<char> * allowed = nullptr;
  std::vector<char> * used = nullptr;
  Slot ** slots = nullptr;
};

extern thread_local JobContext * tl_job;

void set_targets(std::unordered_map<const rcl_publisher_t *, PubTarget *> targets);
void clear_targets();

struct HookErrors
{
  std::atomic<int64_t> foreign_publish{0};
  std::atomic<int64_t> outside_publish{0};
  std::atomic<int64_t> serialize_failed{0};
  std::atomic<int64_t> publish_set_violation{0};
  std::atomic<int> first_violation_task{-1};
  std::atomic<int> first_violation_pub{-1};
};
HookErrors & hook_errors();

struct IntraProcessCounts
{
  std::atomic<int64_t> publishers{0};
  std::atomic<int64_t> subscriptions{0};
};
IntraProcessCounts & intra_process_counts();

}
}
