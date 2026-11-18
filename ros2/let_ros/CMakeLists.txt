cmake_minimum_required(VERSION 3.16)
project(let_ros CXX)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 20)
endif()
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(-Wall -Wextra)

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rcl REQUIRED)
find_package(rmw REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(class_loader REQUIRED)
find_package(ament_index_cpp REQUIRED)
find_package(Threads REQUIRED)
find_package(reference_system QUIET)
find_package(autoware_reference_system QUIET)

set(CORE_SOURCES src/hooks.cpp src/ipc.cpp src/connections.cpp src/simulator.cpp src/calendar.cpp src/streams.cpp
  src/baselines.cpp)

function(let_ros_executable target)
  add_executable(${target} ${ARGN} ${CORE_SOURCES})
  target_include_directories(${target} PRIVATE include src)
  target_link_libraries(${target}
    rclcpp::rclcpp rcl::rcl rmw::rmw Threads::Threads ${CMAKE_DL_LIBS})
  set_target_properties(${target} PROPERTIES ENABLE_EXPORTS ON)
  install(TARGETS ${target} DESTINATION lib/${PROJECT_NAME})
endfunction()

let_ros_executable(let_ros_run src/main_run.cpp)
target_link_libraries(let_ros_run
  rclcpp_components::component class_loader::class_loader ament_index_cpp::ament_index_cpp)
find_package(OpenCV QUIET COMPONENTS core)
if(OpenCV_FOUND)
  target_compile_definitions(let_ros_run PRIVATE LET_ROS_HAS_OPENCV=1)
  target_include_directories(let_ros_run PRIVATE ${OpenCV_INCLUDE_DIRS})
  target_link_libraries(let_ros_run ${OpenCV_LIBS})
endif()

if(reference_system_FOUND AND autoware_reference_system_FOUND)
  let_ros_executable(let_ros_autoware src/main_autoware.cpp)
  ament_target_dependencies(let_ros_autoware reference_system autoware_reference_system)
  target_compile_options(let_ros_autoware PRIVATE -Wno-unused-parameter)
  let_ros_executable(let_ros_fleet src/main_fleet.cpp)
  ament_target_dependencies(let_ros_fleet reference_system autoware_reference_system)
  target_link_libraries(let_ros_fleet ament_index_cpp::ament_index_cpp)
  target_compile_options(let_ros_fleet PRIVATE -Wno-unused-parameter)
else()
  message(STATUS "reference_system not found: let_ros_autoware is not built")
endif()

install(DIRECTORY include/ DESTINATION include)
install(PROGRAMS scripts/bench.sh scripts/bench_fleet.sh
  scripts/bench_sweeps.sh scripts/bench_bounds.sh scripts/bench_model.sh scripts/summarize.py
  scripts/dag_bound.py scripts/schedule_bound.py scripts/common.sh scripts/selftest.sh
  scripts/bench_leads.sh scripts/bench_node.sh
  scripts/check_native_conformance.py
  DESTINATION lib/${PROJECT_NAME})
install(DIRECTORY config DESTINATION share/${PROJECT_NAME})
ament_package()
