#!/usr/bin/env bash
set -euo pipefail
PREFIX=$(realpath -m "${1:?prefix, e.g. ~/letros}")
LETROS=$(realpath "${2:?path to let_ros}")
TESTNODES=$(realpath "${3:?path to let_ros_test_nodes}")
STAGE=${4:-all}
case "$STAGE" in all|env|build) ;; *) echo "stage must be all, env or build" >&2; exit 1;; esac
mkdir -p "$PREFIX/bin" "$PREFIX/ws/src"
MM="$PREFIX/bin/micromamba"

if [ "$STAGE" != build ]; then
if [ ! -x "$MM" ]; then
  echo "== micromamba"
  tmp=$(mktemp -d)
  if ! curl -fsSL -m 120 https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xj -C "$tmp" bin/micromamba; then
    curl -fsSL -m 300 https://conda.anaconda.org/conda-forge/linux-64/micromamba-2.9.0-0.tar.bz2 | tar -xj -C "$tmp" bin/micromamba
  fi
  mv "$tmp/bin/micromamba" "$MM"
  rm -rf "$tmp"
fi

echo "== ROS 2 Jazzy environment (takes a few minutes)"
export MAMBA_ROOT_PREFIX="$PREFIX/mamba"
PKGS=$(grep -v '^#' "$LETROS/scripts/environment.txt" | tr '\n' ' ')
if [ ! -d "$PREFIX/env/conda-meta" ]; then
  "$MM" create -y --no-rc --override-channels -p "$PREFIX/env" \
    -c https://conda.anaconda.org/robostack-jazzy -c https://conda.anaconda.org/conda-forge $PKGS
  "$MM" clean -a -y --no-rc
fi

cat > "$PREFIX/activate.sh" <<EOS
export MAMBA_ROOT_PREFIX="$PREFIX/mamba"
eval "\$("$MM" shell hook -s bash)"
micromamba activate "$PREFIX/env"
export ROS_AUTOMATIC_DISCOVERY_RANGE=OFF
export ROS_LOG_DIR=\${ROS_LOG_DIR:-\${TMPDIR:-/tmp}/roslog-\$USER}
[ -f "$PREFIX/ws/install/setup.bash" ] && source "$PREFIX/ws/install/setup.bash"
true
EOS

echo "== sources"
cd "$PREFIX/ws/src"
REFSYS_COMMIT=${REFSYS_COMMIT:-de8f3bc06f399ac99d6073460b50c5b59fc73a9c}
if [ ! -d reference-system ]; then
  git clone https://github.com/ros-realtime/reference-system.git
  git -C reference-system checkout -q "$REFSYS_COMMIT"
fi
echo "reference-system at $(git -C reference-system rev-parse --short HEAD)"
fi

if [ "$STAGE" = env ]; then
  echo "env stage done. Next: $0 $PREFIX $LETROS $TESTNODES build"
  exit 0
fi
[ -d "$PREFIX/env/conda-meta" ] && [ -d "$PREFIX/ws/src/reference-system" ] || {
  echo "run the env stage first" >&2; exit 1; }

echo "== workspace"
cd "$PREFIX/ws/src"
rm -rf let_ros let_ros_test_nodes
cp -r "$LETROS" let_ros
cp -r "$TESTNODES" let_ros_test_nodes
set +u
source "$PREFIX/activate.sh"
set -u
cd "$PREFIX/ws"
colcon build --cmake-args -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release \
  --packages-up-to let_ros let_ros_test_nodes

echo "== self test"
set +u
source "$PREFIX/activate.sh"
set -u
bash "$PREFIX/ws/install/let_ros/lib/let_ros/selftest.sh"
echo "done. Use: source $PREFIX/activate.sh"
