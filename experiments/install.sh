#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
PREFIX=${LETROS_PREFIX:-$HOME/letros}
export MAKEFLAGS=${MAKEFLAGS:--j$(nproc)}
export LET_ROS_NATIVE_OPTIONAL=${LET_ROS_NATIVE_OPTIONAL:-1}
bash ros2/let_ros/scripts/install_ros_conda.sh "$PREFIX" ros2/let_ros ros2/let_ros_test_nodes "${1:-all}"
