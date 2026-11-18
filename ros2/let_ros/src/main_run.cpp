#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <ament_index_cpp/get_resource.hpp>
#include <class_loader/class_loader.hpp>
#include <rclcpp_components/node_factory.hpp>
#ifdef LET_ROS_HAS_OPENCV
#include <opencv2/core/utility.hpp>
#endif

#include "let_ros/driver.hpp"

namespace
{

struct Spec
{
  std::string package;
  std::string cls;
  std::vector<std::string> args;
};

std::vector<Spec> read_specs(const std::string & path)
{
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("cannot open node list " + path);
  }
  std::vector<Spec> out;
  std::string line;
  while (std::getline(in, line)) {
    auto hash = line.find('#');
    if (hash != std::string::npos) {
      line = line.substr(0, hash);
    }
    std::istringstream ss(line);
    Spec s;
    if (!(ss >> s.package >> s.cls)) {
      continue;
    }
    std::string a;
    while (ss >> a) {
      s.args.push_back(a);
    }
    out.push_back(std::move(s));
  }
  return out;
}

std::string library_for(const std::string & package, const std::string & cls)
{
  std::string content;
  std::string base;
  if (!ament_index_cpp::get_resource("rclcpp_components", package, content, &base)) {
    throw std::runtime_error("package " + package + " registers no components");
  }
  std::istringstream ss(content);
  std::string line;
  while (std::getline(ss, line)) {
    auto semi = line.find(';');
    if (semi == std::string::npos) {
      continue;
    }
    if (line.substr(0, semi) == cls) {
      std::string lib = line.substr(semi + 1);
      if (!lib.empty() && lib[0] != '/') {
        lib = base + "/" + lib;
      }
      return lib;
    }
  }
  throw std::runtime_error("component " + cls + " not found in package " + package);
}

}

int main(int argc, char ** argv)
{
  try {
    auto args = let_ros::parse_driver_args(argc, argv);
    std::string list;
    bool intra_process = false;
    int opencv_threads = 0;
    for (size_t i = 0; i < args.rest.size(); ++i) {
      if (args.rest[i] == "--nodes" && i + 1 < args.rest.size()) {
        list = args.rest[++i];
      } else if (args.rest[i] == "--intra-process") {
        intra_process = true;
      } else if (args.rest[i] == "--opencv-threads" && i + 1 < args.rest.size()) {
        opencv_threads = static_cast<int>(let_ros::parse_int(
            "--opencv-threads", args.rest[++i], 1, std::numeric_limits<int>::max()));
      } else if (args.rest[i] == "-h" || args.rest[i] == "--help") {
        let_ros::print_usage(argv[0], " --nodes FILE\n  --nodes FILE               "
          "one node per line: <package> <Class> [ROS args]\n"
          "  --intra-process            create the nodes with use_intra_process_comms(true)\n"
          "  --opencv-threads N         OpenCV threads per algorithm (default unchanged)");
        return 0;
      } else {
        throw std::runtime_error("unknown argument " + args.rest[i]);
      }
    }
    if (list.empty()) {
      throw std::runtime_error("--nodes FILE is required");
    }
    auto specs = read_specs(list);
    let_ros::init_ros(argc, argv);
    if (opencv_threads > 0) {
#ifdef LET_ROS_HAS_OPENCV
      cv::setNumThreads(opencv_threads);
      std::cerr << "OpenCV threads: " << cv::getNumThreads() << '\n';
#else
      throw std::runtime_error("--opencv-threads needs OpenCV core at build time");
#endif
    }

    std::map<std::string, std::unique_ptr<class_loader::ClassLoader>> loaders;
    std::vector<std::shared_ptr<rclcpp_components::NodeFactory>> factories;
    for (auto & s : specs) {
      std::string lib = library_for(s.package, s.cls);
      auto & loader = loaders[lib];
      if (!loader) {
        loader = std::make_unique<class_loader::ClassLoader>(lib);
      }
      std::string name = "rclcpp_components::NodeFactoryTemplate<" + s.cls + ">";
      factories.push_back(loader->createInstance<rclcpp_components::NodeFactory>(name));
    }

    let_ros::NodeFactory factory = [&]() {
        let_ros::NodeList out;
        for (size_t i = 0; i < specs.size(); ++i) {
          std::vector<std::string> ros_args{"--ros-args"};
          ros_args.insert(ros_args.end(), specs[i].args.begin(), specs[i].args.end());
          rclcpp::NodeOptions opts;
          opts.arguments(ros_args);
          opts.start_parameter_services(false);
          opts.start_parameter_event_publisher(false);
          opts.use_intra_process_comms(intra_process);
          auto wrapper = std::make_shared<rclcpp_components::NodeInstanceWrapper>(
            factories[i]->create_node_instance(opts));
          out.emplace_back(wrapper->get_node_base_interface(), wrapper);
        }
        return out;
      };
    let_ros::finish(let_ros::run_driver(args, factory));
  } catch (const std::exception & e) {
    std::cerr << "error: " << e.what() << std::endl;
    let_ros::finish(2);
  }
}
