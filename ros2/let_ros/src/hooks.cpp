#include <dlfcn.h>

#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>

#include <rcl/publisher.h>
#include <rcl/time.h>
#include <rmw/rmw.h>
#include <rmw/serialized_message.h>
#include <rcutils/allocator.h>
#include <rcutils/error_handling.h>

#include "internal.hpp"

namespace let_ros
{
namespace detail
{

thread_local JobContext * tl_job = nullptr;

namespace
{

template<typename Fn>
Fn real(const char * name)
{
  void * sym = dlsym(RTLD_NEXT, name);
  if (!sym) {
    throw std::runtime_error(std::string("let_ros: cannot find real ") + name);
  }
  return reinterpret_cast<Fn>(sym);
}

using publisher_init_t = rcl_ret_t (*)(rcl_publisher_t *, const rcl_node_t *,
  const rosidl_message_type_support_t *, const char *, const rcl_publisher_options_t *);
using publisher_fini_t = rcl_ret_t (*)(rcl_publisher_t *, rcl_node_t *);
using publish_t = rcl_ret_t (*)(const rcl_publisher_t *, const void *, rmw_publisher_allocation_t *);
using publish_serialized_t = rcl_ret_t (*)(const rcl_publisher_t *,
  const rcl_serialized_message_t *, rmw_publisher_allocation_t *);
using can_loan_t = bool (*)(const rcl_publisher_t *);
using clock_get_now_t = rcl_ret_t (*)(rcl_clock_t *, rcl_time_point_value_t *);
using sub_count_t = rcl_ret_t (*)(const rcl_publisher_t *, size_t *);
using get_topic_name_t = const char * (*)(const rcl_publisher_t *);

struct RealFunctions
{
  publisher_init_t publisher_init = real<publisher_init_t>("rcl_publisher_init");
  publisher_fini_t publisher_fini = real<publisher_fini_t>("rcl_publisher_fini");
  publish_t publish = real<publish_t>("rcl_publish");
  publish_serialized_t publish_serialized =
    real<publish_serialized_t>("rcl_publish_serialized_message");
  clock_get_now_t clock_get_now = real<clock_get_now_t>("rcl_clock_get_now");
  sub_count_t sub_count = real<sub_count_t>("rcl_publisher_get_subscription_count");
};

const RealFunctions & fns()
{
  static RealFunctions f;
  return f;
}

std::mutex registry_mutex;
std::unordered_map<const rcl_publisher_t *, PublisherInfo> registry;

void note_unmodeled_publication(const rcl_publisher_t * publisher)
{
  std::lock_guard<std::mutex> lock(registry_mutex);
  auto it = registry.find(publisher);
  if (it != registry.end()) {
    ++it->second.outside_messages;
  }
}

std::unordered_map<const rcl_publisher_t *, PubTarget *> targets;

struct SerializationBuffer
{
  rmw_serialized_message_t msg = rmw_get_zero_initialized_serialized_message();
  bool ready = false;

  ~SerializationBuffer()
  {
    if (ready) {
      if (rmw_serialized_message_fini(&msg) != RMW_RET_OK) {
        rcutils_reset_error();
      }
    }
  }

  rmw_serialized_message_t & get()
  {
    if (!ready) {
      rcutils_allocator_t alloc = rcutils_get_default_allocator();
      if (rmw_serialized_message_init(&msg, 4096, &alloc) != RMW_RET_OK) {
        throw std::runtime_error("let_ros: cannot allocate serialization buffer");
      }
      ready = true;
    }
    return msg;
  }
};

thread_local SerializationBuffer tl_buffer;

rmw_serialized_message_t & buffer()
{
  return tl_buffer.get();
}

bool admit(const PubTarget & t, JobContext & ctx)
{
  if (ctx.used) {
    (*ctx.used)[static_cast<size_t>(t.index)] = 1;
  }
  if (ctx.allowed && !(*ctx.allowed)[static_cast<size_t>(t.index)]) {
    auto & e = hook_errors();
    if (e.publish_set_violation.fetch_add(1) == 0) {
      e.first_violation_task.store(t.task);
      e.first_violation_pub.store(t.index);
    }
    return false;
  }
  return true;
}

Slot * slot_for(PubTarget & t, const JobContext & ctx)
{
  Slot * s = nullptr;
  if (ctx.slots) {
    s = ctx.slots[t.index];
  } else if (!t.ring.empty()) {
    s = &t.ring[static_cast<size_t>(ctx.job % static_cast<int64_t>(t.ring.size()))];
  }
  if (s && s->count == static_cast<int>(s->msgs.size())) {
    s->msgs.emplace_back();
  }
  return s;
}

void count_violation(const PubTarget & t)
{
  auto & e = hook_errors();
  if (e.publish_set_violation.fetch_add(1) == 0) {
    e.first_violation_task.store(t.task);
    e.first_violation_pub.store(t.index);
  }
}

}

std::vector<PublisherInfo> registered_publishers()
{
  std::lock_guard<std::mutex> lock(registry_mutex);
  std::vector<PublisherInfo> out;
  out.reserve(registry.size());
  for (auto & kv : registry) {
    out.push_back(kv.second);
  }
  return out;
}

void set_targets(std::unordered_map<const rcl_publisher_t *, PubTarget *> t)
{
  targets = std::move(t);
}

void clear_targets()
{
  targets.clear();
}

HookErrors & hook_errors()
{
  static HookErrors e;
  return e;
}

}
}

using namespace let_ros::detail;

extern "C" {

rcl_ret_t rcl_publisher_init(
  rcl_publisher_t * publisher,
  const rcl_node_t * node,
  const rosidl_message_type_support_t * type_support,
  const char * topic_name,
  const rcl_publisher_options_t * options)
{
  rcl_ret_t ret = fns().publisher_init(publisher, node, type_support, topic_name, options);
  if (ret == RCL_RET_OK) {
    static get_topic_name_t get_name = real<get_topic_name_t>("rcl_publisher_get_topic_name");
    PublisherInfo info;
    info.handle = publisher;
    info.node = node;
    info.type_support = type_support;
    const char * resolved = get_name(publisher);
    info.topic = resolved ? resolved : topic_name;
    std::lock_guard<std::mutex> lock(registry_mutex);
    static uint64_t next_seq = 0;
    info.seq = next_seq++;
    registry[publisher] = info;
  }
  return ret;
}

rcl_ret_t rcl_publisher_fini(rcl_publisher_t * publisher, rcl_node_t * node)
{
  {
    std::lock_guard<std::mutex> lock(registry_mutex);
    registry.erase(publisher);
  }
  return fns().publisher_fini(publisher, node);
}

rcl_ret_t rcl_publish(
  const rcl_publisher_t * publisher,
  const void * ros_message,
  rmw_publisher_allocation_t * allocation)
{
  if (!targets.empty()) {
    auto it = targets.find(publisher);
    if (it != targets.end()) {
      JobContext * ctx = tl_job;
      if (!ctx) {
        hook_errors().outside_publish++;
        return RCL_RET_OK;
      }
      PubTarget & t = *it->second;
      if (t.task != ctx->task) {
        hook_errors().foreign_publish++;
        return RCL_RET_OK;
      }
      if (!admit(t, *ctx)) {
        return RCL_RET_OK;
      }
      rmw_serialized_message_t & buf = buffer();
      size_t capacity = buf.buffer_capacity;
      std::memset(buf.buffer, 0, capacity);
      buf.buffer_length = 0;
      if (rmw_serialize(ros_message, t.type_support, &buf) != RMW_RET_OK) {
        hook_errors().serialize_failed++;
        return RCL_RET_ERROR;
      }
      if (buf.buffer_capacity != capacity) {
        std::memset(buf.buffer, 0, buf.buffer_capacity);
        buf.buffer_length = 0;
        if (rmw_serialize(ros_message, t.type_support, &buf) != RMW_RET_OK) {
          hook_errors().serialize_failed++;
          return RCL_RET_ERROR;
        }
      }
      Slot * s = slot_for(t, *ctx);
      if (!s) {
        count_violation(t);
        return RCL_RET_OK;
      }
      s->msgs[s->count].assign(buf.buffer, buf.buffer + buf.buffer_length);
      s->count++;
      return RCL_RET_OK;
    }
  }
  if (!tl_job) {
    note_unmodeled_publication(publisher);
  }
  return fns().publish(publisher, ros_message, allocation);
}

rcl_ret_t rcl_publish_serialized_message(
  const rcl_publisher_t * publisher,
  const rcl_serialized_message_t * serialized_message,
  rmw_publisher_allocation_t * allocation)
{
  if (!targets.empty()) {
    auto it = targets.find(publisher);
    if (it != targets.end()) {
      JobContext * ctx = tl_job;
      if (!ctx) {
        hook_errors().outside_publish++;
        return RCL_RET_OK;
      }
      PubTarget & t = *it->second;
      if (t.task != ctx->task) {
        hook_errors().foreign_publish++;
        return RCL_RET_OK;
      }
      if (!admit(t, *ctx)) {
        return RCL_RET_OK;
      }
      Slot * s = slot_for(t, *ctx);
      if (!s) {
        count_violation(t);
        return RCL_RET_OK;
      }
      s->msgs[s->count].assign(serialized_message->buffer,
        serialized_message->buffer + serialized_message->buffer_length);
      s->count++;
      return RCL_RET_OK;
    }
  }
  if (!tl_job) {
    note_unmodeled_publication(publisher);
  }
  return fns().publish_serialized(publisher, serialized_message, allocation);
}

bool rcl_publisher_can_loan_messages(const rcl_publisher_t * publisher)
{
  (void)publisher;
  return false;
}

rcl_ret_t rcl_publisher_get_subscription_count(
  const rcl_publisher_t * publisher,
  size_t * subscription_count)
{
  if (!targets.empty() && subscription_count) {
    auto it = targets.find(publisher);
    if (it != targets.end()) {
      *subscription_count = it->second->subscribers;
      return RCL_RET_OK;
    }
  }
  return fns().sub_count(publisher, subscription_count);
}

rcl_ret_t rcl_clock_get_now(rcl_clock_t * clock, rcl_time_point_value_t * time_point_value)
{
  JobContext * ctx = tl_job;
  if (ctx && clock && time_point_value && clock->type == RCL_ROS_TIME) {
    *time_point_value = ctx->release_ns;
    return RCL_RET_OK;
  }
  return fns().clock_get_now(clock, time_point_value);
}

}
