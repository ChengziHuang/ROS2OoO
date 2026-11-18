#include <cstdio>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <ament_index_cpp/get_package_share_directory.hpp>
#include <rcl/arguments.h>
#include <rcl/context.h>
#include <rcutils/error_handling.h>

#include "let_ros/driver.hpp"

#include "reference_system/system/type/rclcpp_system.hpp"
#include "reference_system/number_cruncher.hpp"
#include "lean_reference_nodes.hpp"
#include "autoware_reference_system/autoware_system_builder.hpp"
#include "autoware_reference_system/system/timing/benchmark.hpp"
#include "autoware_reference_system/system/timing/default.hpp"

namespace
{

std::string car_ns(int i)
{
  return "/car_" + std::to_string(i);
}

class V2XFusion : public rclcpp::Node
{
public:
  V2XFusion(int car, const std::vector<int> & neighbours, uint64_t crunch, bool lean)
  : Node("V2XFusion", car_ns(car),
      lean ? let_ros::lean_nodes::node_options() : rclcpp::NodeOptions()),
    crunch_(crunch), cache_(neighbours.size())
  {
    publisher_ = create_publisher<message_t>("V2XObjects", 1);
    own_ = create_subscription<message_t>("ObjectCollisionEstimator", 1,
        [this](const message_t::SharedPtr m) {on_own(*m);});
    for (size_t x = 0; x < neighbours.size(); ++x) {
      others_.push_back(create_subscription<message_t>(
          car_ns(neighbours[x]) + "/ObjectCollisionEstimator", 1,
          [this, x](const message_t::SharedPtr m) {cache_[x] = m;}));
    }
  }

private:
  void on_own(const message_t & m)
  {
    auto out = publisher_->borrow_loaned_message();
    out.get() = m;
    out.get().data[0] = number_cruncher(crunch_);
    uint64_t fold = 1469598103934665603ULL;
    int64_t present = 0;
    for (const auto & c : cache_) {
      uint64_t v = c ? static_cast<uint64_t>(c->data[0]) + 1 : 0;
      present += c ? 1 : 0;
      fold = (fold ^ v) * 1099511628211ULL;
    }
    out.get().data[1] = static_cast<int64_t>(cache_.size());
    out.get().data[2] = present;
    out.get().data[3] = static_cast<int64_t>(fold);
    publisher_->publish(std::move(out));
  }

  uint64_t crunch_;
  std::vector<message_t::SharedPtr> cache_;
  rclcpp::Publisher<message_t>::SharedPtr publisher_;
  rclcpp::Subscription<message_t>::SharedPtr own_;
  std::vector<rclcpp::Subscription<message_t>::SharedPtr> others_;
};

class GlobalArgs
{
public:
  GlobalArgs()
  {
    ctx_ = rclcpp::contexts::get_global_default_context()->get_rcl_context();
    saved_ = rcl_get_zero_initialized_arguments();
    if (rcl_arguments_copy(&ctx_->global_arguments, &saved_) != RCL_RET_OK) {
      throw std::runtime_error("cannot copy global arguments");
    }
  }
  ~GlobalArgs() {restore();}

  void set(const std::vector<std::string> & args)
  {
    std::vector<const char *> argv{"let_ros_fleet", "--ros-args"};
    for (auto & a : args) {
      argv.push_back(a.c_str());
    }
    rcl_arguments_t parsed = rcl_get_zero_initialized_arguments();
    if (rcl_parse_arguments(static_cast<int>(argv.size()), argv.data(),
      rcl_get_default_allocator(), &parsed) != RCL_RET_OK)
    {
      throw std::runtime_error("cannot parse vehicle arguments");
    }
    if (rcl_arguments_fini(&ctx_->global_arguments) != RCL_RET_OK) {rcutils_reset_error();}
    ctx_->global_arguments = parsed;
    rcutils_reset_error();
  }

  void restore()
  {
    if (ctx_) {
      if (rcl_arguments_fini(&ctx_->global_arguments) != RCL_RET_OK) {rcutils_reset_error();}
      rcl_arguments_t copy = rcl_get_zero_initialized_arguments();
      if (rcl_arguments_copy(&saved_, &copy) != RCL_RET_OK) {rcutils_reset_error();}
      ctx_->global_arguments = copy;
      rcutils_reset_error();
    }
  }

private:
  std::shared_ptr<rcl_context_t> ctx_;
  rcl_arguments_t saved_;
};

template<typename System>
std::vector<std::shared_ptr<rclcpp::Node>> vehicle(const std::string & timing)
{
  if (timing == "default") {
    return create_autoware_nodes<System, nodes::timing::Default>();
  } else if (timing == "throughput") {
    return create_autoware_nodes<System, nodes::timing::BenchmarkThroughput>();
  } else if (timing == "cpu") {
    return create_autoware_nodes<System, nodes::timing::BenchmarkCPUUsage>();
  }
  throw std::runtime_error("unknown timing " + timing);
}

using Sets = std::map<std::string, std::map<std::string, std::set<std::string>>>;

Sets fleet_sets(const Sets & base, int cars, const std::vector<std::vector<int>> & nb)
{
  Sets out;
  for (int i = 0; i < cars; ++i) {
    std::string ns = car_ns(i);
    for (auto & node : base) {
      std::string name = node.first[0] == '/' ? node.first : "/" + node.first;
      for (auto & src : node.second) {
        std::string key = src.first;
        if (key != "timer") {
          key = (name == "/BehaviorPlanner" && key == "/ObjectCollisionEstimator") ?
            ns + "/V2XObjects" : ns + key;
        }
        auto & topics = out[ns + name][key];
        for (auto & t : src.second) {
          topics.insert(ns + t);
        }
      }
    }
    out[ns + "/V2XFusion"][ns + "/ObjectCollisionEstimator"] = {ns + "/V2XObjects"};
    for (int j : nb[static_cast<size_t>(i)]) {
      out[ns + "/V2XFusion"][car_ns(j) + "/ObjectCollisionEstimator"] = {};
    }
  }
  return out;
}

}

int main(int argc, char ** argv)
{
  try {
    auto args = let_ros::parse_driver_args(argc, argv);
    std::string timing = "default";
    int cars = 4;
    int reach = 1;
    int64_t step_ns = 37000000;
    int64_t v2x_delay_ns = 0;
    bool sets_given = !args.base.publish_sets.empty();
    bool no_sets = false;
    bool param_services = false;
    for (size_t i = 0; i < args.rest.size(); ++i) {
      const std::string & a = args.rest[i];
      auto val = [&]() {
          if (i + 1 >= args.rest.size()) {
            throw std::runtime_error("missing value after " + a);
          }
          return args.rest[++i];
        };
      if (a == "--cars") {
        cars = static_cast<int>(let_ros::parse_int(a, val(), 1, 100000));
      } else if (a == "--v2x") {
        reach = static_cast<int>(let_ros::parse_int(a, val(), 0, 100000));
      } else if (a == "--offset-step") {
        step_ns = let_ros::parse_duration_ns(val());
      } else if (a == "--v2x-delay") {
        std::string v = val();
        v2x_delay_ns = let_ros::parse_duration_ns(v);
        if (v2x_delay_ns < 0) {
          throw std::runtime_error("--v2x-delay must not be negative, got " + v);
        }
      } else if (a == "--timing") {
        timing = val();
      } else if (a == "--no-publish-sets") {
        no_sets = true;
      } else if (a == "--param-services") {
        param_services = true;
      } else if (a == "-h" || a == "--help") {
        let_ros::print_usage(argv[0],
          "\n  --cars N                   vehicles (default 4)\n"
          "  --v2x K                    neighbours on each side in a ring (default 1)\n"
          "  --offset-step 37ms         start of car i is i*step mod 100ms, in [0, 100ms)\n"
          "  --v2x-delay 20ms           transport delay of the object lists that a V2X node\n"
          "                             receives from the neighbours (default 0)\n"
          "  --timing default|cpu       reference_system timing set (cpu: no work in callbacks)\n"
          "  --publish-sets FILE        sets of one vehicle (default: shipped file)\n"
          "  --no-publish-sets          no publish sets\n"
          "  --param-services           create the nodes with the ROS parameter services,\n"
          "                             as reference_system does (slow for large fleets)");
        return 0;
      } else {
        throw std::runtime_error("unknown argument " + a);
      }
    }
    if (timing != "default" && timing != "throughput" && timing != "cpu") {
      throw std::runtime_error("--timing must be default, throughput or cpu, got " + timing);
    }

    std::vector<std::vector<int>> nb(static_cast<size_t>(cars));
    for (int i = 0; i < cars; ++i) {
      std::set<int> s;
      for (int d = 1; d <= reach; ++d) {
        s.insert(((i - d) % cars + cars) % cars);
        s.insert((i + d) % cars);
      }
      s.erase(i);
      nb[static_cast<size_t>(i)].assign(s.begin(), s.end());
    }

    if (!no_sets) {
      Sets base = args.base.publish_sets;
      if (!sets_given) {
        base = let_ros::load_publish_sets(
          ament_index_cpp::get_package_share_directory("let_ros") +
          "/config/autoware_publish_sets.txt");
      }
      args.base.publish_sets = fleet_sets(base, cars, nb);
    } else {
      args.base.publish_sets.clear();
    }
    const int64_t cycle = 100000000LL;
    const int64_t step = (step_ns % cycle + cycle) % cycle;
    for (int i = 0; i < cars; ++i) {
      args.base.timer_offset_ns[car_ns(i) + "/"] = (static_cast<int64_t>(i) * step) % cycle;
    }
    if (v2x_delay_ns > 0) {
      for (int i = 0; i < cars; ++i) {
        for (int j : nb[static_cast<size_t>(i)]) {
          args.base.link_delay_ns[car_ns(j) + "/ObjectCollisionEstimator " + car_ns(i) +
            "/V2XFusion"] = v2x_delay_ns;
        }
      }
    }

    let_ros::init_ros(argc, argv);
    set_benchmark_mode(true);
    uint64_t v2x_crunch = nodes::timing::Default::EUCLIDEAN_INTERSECTION;
    if (timing == "throughput") {
      v2x_crunch = nodes::timing::BenchmarkThroughput::EUCLIDEAN_INTERSECTION;
    } else if (timing == "cpu") {
      v2x_crunch = nodes::timing::BenchmarkCPUUsage::EUCLIDEAN_INTERSECTION;
    }

    let_ros::NodeFactory factory = [&]() {
        let_ros::NodeList out;
        GlobalArgs global;
        for (int i = 0; i < cars; ++i) {
          global.set({"-r", "__ns:=" + car_ns(i),
              "-r", "BehaviorPlanner:ObjectCollisionEstimator:=V2XObjects"});
          std::vector<std::shared_ptr<rclcpp::Node>> nodes = param_services ?
            vehicle<RclcppSystem>(timing) : vehicle<let_ros::lean_nodes::RclcppSystem>(timing);
          for (auto & n : nodes) {
            out.emplace_back(n->get_node_base_interface(), n);
          }
        }
        global.restore();
        for (int i = 0; i < cars; ++i) {
          auto v = std::make_shared<V2XFusion>(i, nb[static_cast<size_t>(i)], v2x_crunch,
              !param_services);
          out.emplace_back(v->get_node_base_interface(), v);
        }
        return out;
      };
    let_ros::finish(let_ros::run_driver(args, factory));
  } catch (const std::exception & e) {
    std::cerr << "error: " << e.what() << std::endl;
    let_ros::finish(2);
  }
}
