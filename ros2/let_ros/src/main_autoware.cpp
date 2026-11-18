#include <iostream>
#include <string>

#include "let_ros/driver.hpp"

#include "reference_system/system/type/rclcpp_system.hpp"
#include "autoware_reference_system/autoware_system_builder.hpp"
#include "autoware_reference_system/system/timing/benchmark.hpp"
#include "autoware_reference_system/system/timing/default.hpp"

int main(int argc, char ** argv)
{
  try {
    auto args = let_ros::parse_driver_args(argc, argv);
    std::string timing = "default";
    bool stats = false;
    for (size_t i = 0; i < args.rest.size(); ++i) {
      if (args.rest[i] == "--timing" && i + 1 < args.rest.size()) {
        timing = args.rest[++i];
      } else if (args.rest[i] == "--stats") {
        stats = true;
      } else if (args.rest[i] == "-h" || args.rest[i] == "--help") {
        let_ros::print_usage(argv[0],
          "\n  --timing default|cpu       reference_system timing set (cpu: no work in callbacks)\n"
          "  --stats                    keep sample statistics (wall-clock stamps,\n"
          "                             so hashes differ between runs)");
        return 0;
      } else {
        throw std::runtime_error("unknown argument " + args.rest[i]);
      }
    }
    let_ros::init_ros(argc, argv);
    set_benchmark_mode(!stats);

    let_ros::NodeFactory factory = [&]() {
        std::vector<std::shared_ptr<rclcpp::Node>> nodes;
        if (timing == "default") {
          nodes = create_autoware_nodes<RclcppSystem, nodes::timing::Default>();
        } else if (timing == "throughput") {
          nodes = create_autoware_nodes<RclcppSystem, nodes::timing::BenchmarkThroughput>();
        } else if (timing == "cpu") {
          nodes = create_autoware_nodes<RclcppSystem, nodes::timing::BenchmarkCPUUsage>();
        } else {
          throw std::runtime_error("unknown timing " + timing);
        }
        let_ros::NodeList out;
        for (auto & n : nodes) {
          out.emplace_back(n->get_node_base_interface(), n);
        }
        return out;
      };
    let_ros::finish(let_ros::run_driver(args, factory));
  } catch (const std::exception & e) {
    std::cerr << "error: " << e.what() << std::endl;
    let_ros::finish(2);
  }
}
