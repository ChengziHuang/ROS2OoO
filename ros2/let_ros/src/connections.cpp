#include <algorithm>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <rclcpp/exceptions.hpp>
#include <rclcpp/node_interfaces/node_topics.hpp>
#include <rclcpp/publisher_base.hpp>
#include <rmw/qos_profiles.h>

#include "internal.hpp"

namespace
{
struct PublisherEntry
{
  rclcpp::node_interfaces::NodeBaseInterface * node;
  std::weak_ptr<rclcpp::PublisherBase> publisher;
};
std::mutex publishers_mutex;
std::vector<PublisherEntry> publishers;

void remember_publisher(rclcpp::node_interfaces::NodeBaseInterface * node,
  const rclcpp::PublisherBase::SharedPtr & publisher)
{
  std::lock_guard<std::mutex> lock(publishers_mutex);
  publishers.erase(std::remove_if(publishers.begin(), publishers.end(),
      [](const auto & p) {return p.publisher.expired();}), publishers.end());
  publishers.push_back({node, publisher});
}
}

namespace rclcpp::node_interfaces
{
void NodeTopics::add_publisher(rclcpp::PublisherBase::SharedPtr publisher,
  rclcpp::CallbackGroup::SharedPtr callback_group)
{
  if (callback_group) {
    if (!node_base_->callback_group_in_node(callback_group)) {
      throw rclcpp::exceptions::MissingGroupNodeException("publisher");
    }
  } else {
    callback_group = node_base_->get_default_callback_group();
  }
  for (auto & entry : publisher->get_event_handlers()) {
    callback_group->add_waitable(entry.second);
  }
  try {
    node_base_->trigger_notify_guard_condition();
    callback_group->trigger_notify_guard_condition();
  } catch (const rclcpp::exceptions::RCLError & e) {
    throw std::runtime_error(std::string("failed to notify publisher creation: ") + e.what());
  }
  remember_publisher(node_base_, publisher);
}
}

namespace let_ros::detail
{
bool topic_endpoints_compatible(
  const rcl_publisher_t * publisher, const rosidl_message_type_support_t * type_support,
  const rclcpp::SubscriptionBase & subscription)
{
  const char * topic = rcl_publisher_get_topic_name(publisher);
  if (!topic || std::strcmp(topic, subscription.get_topic_name()) != 0) {
    return false;
  }
  const auto & sub_type = subscription.get_message_type_support_handle();
  if (!type_support) {
    throw std::runtime_error("missing publisher type support on " + std::string(topic));
  }
  const auto * ph = type_support->get_type_hash_func ?
    type_support->get_type_hash_func(type_support) : nullptr;
  const auto * sh = sub_type.get_type_hash_func ? sub_type.get_type_hash_func(&sub_type) : nullptr;
  if (ph && sh && ph->version != ROSIDL_TYPE_HASH_VERSION_UNSET &&
    sh->version != ROSIDL_TYPE_HASH_VERSION_UNSET)
  {
    if (ph->version != sh->version || std::memcmp(ph->value, sh->value, sizeof(ph->value)) != 0) {
      return false;
    }
  } else {
    const auto * pd = type_support->get_type_description_func ?
      type_support->get_type_description_func(type_support) : nullptr;
    const auto * sd = sub_type.get_type_description_func ?
      sub_type.get_type_description_func(&sub_type) : nullptr;
    if (!pd || !sd || !pd->type_description.type_name.data || !sd->type_description.type_name.data) {
      throw std::runtime_error("cannot verify message type identity on " + std::string(topic));
    }
    if (std::strcmp(pd->type_description.type_name.data, sd->type_description.type_name.data) != 0) {
      return false;
    }
  }
  const auto * pub_qos = rcl_publisher_get_actual_qos(publisher);
  if (!pub_qos) {
    throw std::runtime_error("cannot read publisher QoS on " + std::string(topic));
  }
  const auto sub_qos = subscription.get_actual_qos().get_rmw_qos_profile();
  rmw_qos_compatibility_type_t compatible = RMW_QOS_COMPATIBILITY_ERROR;
  char reason[2048]{};
  if (rmw_qos_profile_check_compatible(*pub_qos, sub_qos, &compatible, reason, sizeof(reason)) !=
    RMW_RET_OK)
  {
    throw std::runtime_error("cannot check endpoint QoS on " + std::string(topic));
  }
  if (compatible == RMW_QOS_COMPATIBILITY_WARNING) {
    throw std::runtime_error("cannot establish endpoint QoS compatibility on " +
            std::string(topic) + ": " + reason);
  }
  return compatible == RMW_QOS_COMPATIBILITY_OK;
}

void initialize_component_connections(
  const std::vector<rclcpp::node_interfaces::NodeBaseInterface::SharedPtr> & nodes)
{
  std::set<const rclcpp::node_interfaces::NodeBaseInterface *> selected;
  for (const auto & node : nodes) {
    selected.insert(node.get());
  }
  struct ClearTargets
  {
    ~ClearTargets() {clear_targets();}
  } clear;
  std::map<const rcl_publisher_t *, PubTarget> temporary;
  std::map<const rcl_publisher_t *, size_t> notified;
  std::map<const rcl_publisher_t *, int64_t> total_matches;
  const auto outside_before = hook_errors().outside_publish.load();
  for (int pass = 0; pass < 256; ++pass) {
    std::vector<rclcpp::SubscriptionBase::SharedPtr> subscriptions;
    for (const auto & node : nodes) {
      node->for_each_callback_group([&](rclcpp::CallbackGroup::SharedPtr group) {
          group->collect_all_ptrs(
            [&](const rclcpp::SubscriptionBase::SharedPtr & sub) {
              subscriptions.push_back(sub);
            },
            [](const rclcpp::ServiceBase::SharedPtr &) {},
            [](const rclcpp::ClientBase::SharedPtr &) {},
            [](const rclcpp::TimerBase::SharedPtr &) {},
            [](const rclcpp::Waitable::SharedPtr &) {});
        });
    }
    std::vector<rclcpp::PublisherBase::SharedPtr> current;
    {
      std::lock_guard<std::mutex> lock(publishers_mutex);
      for (const auto & entry : publishers) {
        if (selected.count(entry.node)) {
          if (auto publisher = entry.publisher.lock()) {
            current.push_back(std::move(publisher));
          }
        }
      }
    }
    std::unordered_map<const rcl_publisher_t *, PubTarget *> targets;
    std::map<const rcl_publisher_t *, const rosidl_message_type_support_t *> types;
    for (const auto & info : registered_publishers()) {
      types[info.handle] = info.type_support;
    }
    for (const auto & publisher : current) {
      const auto handle = publisher->get_publisher_handle().get();
      auto & target = temporary[handle];
      target.subscribers = 0;
      for (const auto & sub : subscriptions) {
        target.subscribers += topic_endpoints_compatible(handle, types[handle], *sub) ? 1 : 0;
      }
      targets[handle] = &target;
    }
    set_targets(std::move(targets));
    bool changed = false;
    for (const auto & publisher : current) {
      const auto handle = publisher->get_publisher_handle().get();
      const size_t count = temporary[handle].subscribers;
      auto event = publisher->get_event_handlers().find(RCL_PUBLISHER_MATCHED);
      if (event == publisher->get_event_handlers().end() || notified[handle] == count) {
        continue;
      }
      if (count > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        throw std::runtime_error("too many graph subscriptions for a matched event");
      }
      auto info = std::make_shared<rclcpp::MatchedInfo>();
      info->current_count = static_cast<int32_t>(count);
      info->current_count_change = info->current_count - static_cast<int32_t>(notified[handle]);
      info->total_count_change = std::max<int32_t>(0, info->current_count_change);
      total_matches[handle] += info->total_count_change;
      if (total_matches[handle] > std::numeric_limits<int32_t>::max()) {
        throw std::runtime_error("too many cumulative component matches");
      }
      info->total_count = static_cast<int32_t>(total_matches[handle]);
      event->second->execute(info);
      notified[handle] = count;
      changed = true;
    }
    if (hook_errors().outside_publish.load() != outside_before) {
      throw std::runtime_error("a component published while initializing connections");
    }
    if (!changed) {
      return;
    }
  }
  throw std::runtime_error("component connections did not stabilize after 256 passes");
}
}
