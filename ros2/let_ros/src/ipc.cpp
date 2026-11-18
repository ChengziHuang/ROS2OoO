#include <atomic>
#include <cstdint>
#include <memory>

#include <rclcpp/experimental/intra_process_manager.hpp>
#include <rclcpp/publisher_base.hpp>
#include <rclcpp/subscription_base.hpp>

#include "internal.hpp"

namespace let_ros
{
namespace detail
{

IntraProcessCounts & intra_process_counts()
{
  static IntraProcessCounts c;
  return c;
}

}
}

namespace rclcpp
{

void PublisherBase::setup_intra_process(
  uint64_t intra_process_publisher_id,
  IntraProcessManagerSharedPtr ipm)
{
  if (ipm) {
    ipm->remove_publisher(intra_process_publisher_id);
  }
  intra_process_is_enabled_ = false;
  let_ros::detail::intra_process_counts().publishers++;
}

void SubscriptionBase::setup_intra_process(
  uint64_t intra_process_subscription_id,
  IntraProcessManagerWeakPtr weak_ipm)
{
  if (auto ipm = weak_ipm.lock()) {
    ipm->remove_subscription(intra_process_subscription_id);
  }
  use_intra_process_ = false;
  let_ros::detail::intra_process_counts().subscriptions++;
}

}
