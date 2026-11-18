#include "conformance_nodes.hpp"

#include <thread>

int main(int argc, char ** argv)
{
  const std::string scenario = argc > 1 ? argv[1] : "reliable";
  const bool mismatch = scenario == "qos_mismatch" || scenario == "durability_mismatch";
  const bool type_mismatch = scenario == "type_mismatch";
  const bool shallow = scenario == "depth_drop";
  const bool initial = scenario == "constructor_latched";
  try {
    rclcpp::init(argc, argv);
    if (scenario == "record_order") {
      rclcpp::NodeOptions ao, bo, co;
      for (auto * opt : {&ao, &bo, &co}) {
        opt->start_parameter_services(false).start_parameter_event_publisher(false);
      }
      for (auto * opt : {&ao, &bo}) {
        opt->append_parameter_override("total", int64_t{2});
        opt->append_parameter_override("burst", int64_t{2});
      }
      ao.arguments({"--ros-args", "-r", "__node:=order_a", "-r", "conformance:=a"});
      bo.arguments({"--ros-args", "-r", "__node:=order_b", "-r", "conformance:=b"});
      auto a = std::make_shared<let_ros_test::ConformanceSource>(ao);
      auto b = std::make_shared<let_ros_test::ConformanceSource>(bo);
      auto c = std::make_shared<let_ros_test::ConformanceOrderSink>(co);
      rclcpp::executors::SingleThreadedExecutor exec;
      exec.add_node(a); exec.add_node(b); exec.add_node(c);
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      while (a->sent() != 2 || b->sent() != 2) {exec.spin_once(std::chrono::milliseconds(20));}
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(400);
      while (std::chrono::steady_clock::now() < end) {
        exec.spin_some();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      std::cout << "NATIVE_RESULT {\"sent\":4,\"received\":" << c->order.size() <<
        ",\"state\":" << c->state << ",\"creation_rejected\":false}" << std::endl;
      rclcpp::shutdown();
      return 0;
    }
    rclcpp::NodeOptions po;
    rclcpp::NodeOptions so;
    po.start_parameter_services(false).start_parameter_event_publisher(false);
    so.start_parameter_services(false).start_parameter_event_publisher(false);
    po.append_parameter_override("reliable", scenario != "qos_mismatch" && scenario != "best_effort");
    so.append_parameter_override("reliable", scenario != "best_effort" && scenario != "reliable_to_best_effort");
    po.append_parameter_override("total", int64_t{shallow ? 32 : initial ? 1 : 10});
    po.append_parameter_override("burst", int64_t{shallow ? 32 : 1});
    if (shallow) {so.append_parameter_override("depth", int64_t{1});}
    if (scenario == "durability_mismatch") {so.append_parameter_override("transient", true);}
    if (initial) {
      po.append_parameter_override("initial_publish", true);
      po.append_parameter_override("transient", true);
      so.append_parameter_override("transient", true);
    }
    if (scenario == "duplicate_publisher") {
      po.append_parameter_override("duplicate_publisher", true);
    }
    auto source = std::make_shared<let_ros_test::ConformanceSource>(po);
    std::shared_ptr<let_ros_test::ConformanceSink> sink;
    std::shared_ptr<let_ros_test::ConformanceStringSink> string_sink;
    if (type_mismatch) {
      string_sink = std::make_shared<let_ros_test::ConformanceStringSink>(so);
    } else {
      sink = std::make_shared<let_ros_test::ConformanceSink>(so);
    }
    rclcpp::executors::SingleThreadedExecutor executor;
    executor.add_node(source);
    executor.add_node(type_mismatch ? std::static_pointer_cast<rclcpp::Node>(string_sink) :
      std::static_pointer_cast<rclcpp::Node>(sink));
    const auto discovery_end = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!mismatch && !type_mismatch && source->matches() == 0 &&
      std::chrono::steady_clock::now() < discovery_end)
    {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (shallow) {
      while (source->sent() == 0) {executor.spin_once(std::chrono::milliseconds(20));}
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(400);
    while (std::chrono::steady_clock::now() < end) {
      executor.spin_some();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto count = sink ? sink->received.size() : static_cast<size_t>(string_sink->received);
    std::cout << "NATIVE_RESULT {\"sent\":" << source->sent() << ",\"received\":" << count <<
      ",\"matches\":" << source->matches() << ",\"creation_rejected\":false}" << std::endl;
    rclcpp::shutdown();
    return 0;
  } catch (const std::exception & e) {
    std::cerr << e.what() << std::endl;
    std::cout << "NATIVE_RESULT {\"creation_rejected\":true}" << std::endl;
    if (rclcpp::ok()) {rclcpp::shutdown();}
    return 2;
  }
}
