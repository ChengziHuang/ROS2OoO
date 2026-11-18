cmake_minimum_required(VERSION 3.16)
project(let_ros_perception CXX)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(rcutils REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(stereo_msgs REQUIRED)
find_package(std_msgs REQUIRED)

add_library(let_ros_perception SHARED src/perception_nodes.cpp)
ament_target_dependencies(let_ros_perception
  rclcpp rclcpp_components rcutils sensor_msgs stereo_msgs std_msgs)
rclcpp_components_register_nodes(let_ros_perception
  "let_ros_perception::StereoReplaySource"
  "let_ros_perception::ImageSink"
  "let_ros_perception::CameraInfoSink"
  "let_ros_perception::DisparitySink"
  "let_ros_perception::PointCloudSink")

install(TARGETS let_ros_perception
  ARCHIVE DESTINATION lib LIBRARY DESTINATION lib RUNTIME DESTINATION bin)
ament_package()
