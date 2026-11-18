cmake_minimum_required(VERSION 3.16)
project(let_ros_test_nodes CXX)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(std_msgs REQUIRED)

add_library(let_ros_test_nodes SHARED src/test_nodes.cpp src/conformance_nodes.cpp)
target_link_libraries(let_ros_test_nodes
  rclcpp::rclcpp rclcpp_components::component ${std_msgs_TARGETS})
rclcpp_components_register_nodes(let_ros_test_nodes
  "let_ros_test::Source" "let_ros_test::Relay" "let_ros_test::Fuse"
  "let_ros_test::Sampler" "let_ros_test::Gate" "let_ros_test::Burst"
  "let_ros_test::TwoTimers" "let_ros_test::TimerStamp" "let_ros_test::LazyRelay"
  "let_ros_test::Generic" "let_ros_test::MessageStamp"
  "let_ros_test::ConformanceSource" "let_ros_test::ConformanceSink"
  "let_ros_test::ConformanceStringSink" "let_ros_test::ConformanceOrderSink")

add_executable(native_conformance src/native_conformance.cpp)
target_link_libraries(native_conformance rclcpp::rclcpp ${std_msgs_TARGETS})
install(TARGETS native_conformance DESTINATION lib/${PROJECT_NAME})

install(TARGETS let_ros_test_nodes
  ARCHIVE DESTINATION lib LIBRARY DESTINATION lib RUNTIME DESTINATION bin)
install(DIRECTORY graphs DESTINATION share/${PROJECT_NAME})
ament_package()
