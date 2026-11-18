#!/usr/bin/env bash
set -euo pipefail
perception_prefix=$(realpath -m "${1:?usage: install.sh PREFIX [env|build|all]}")
perception_stage=${2:-all}
case "$perception_stage" in env|build|all) ;; *) echo 'stage must be env, build or all' >&2; exit 2;; esac
perception_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$perception_prefix/bin" "$perception_prefix/ws/src"
perception_mm=$perception_prefix/bin/micromamba
if [[ "$perception_stage" != build ]]; then
  if [[ ! -x "$perception_mm" ]]; then
    python3 - "$perception_prefix" <<'PY'
from pathlib import Path
import sys
import tarfile
import urllib.request
prefix = Path(sys.argv[1])
archive = prefix / 'micromamba.tar.bz2'
urllib.request.urlretrieve('https://micro.mamba.pm/api/micromamba/linux-64/latest', archive)
with tarfile.open(archive) as package:
    member = package.getmember('bin/micromamba')
    if not member.isfile():
        raise RuntimeError('micromamba archive has no regular executable')
    with package.extractfile(member) as src, (prefix / 'bin/micromamba').open('wb') as out:
        import shutil
        shutil.copyfileobj(src, out)
(prefix / 'bin/micromamba').chmod(0o755)
PY
  fi
  export MAMBA_ROOT_PREFIX=$perception_prefix/mamba
  mapfile -t perception_packages < <(sed -e 's/\r$//' -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$/d' \
    "$perception_root/let_ros/scripts/environment.txt")
  perception_env_action=create
  if [[ -d "$perception_prefix/env/conda-meta" ]]; then perception_env_action=install; fi
  "$perception_mm" "$perception_env_action" -y --no-rc --override-channels -p "$perception_prefix/env" \
    -c https://conda.anaconda.org/robostack-jazzy -c https://conda.anaconda.org/conda-forge \
    "${perception_packages[@]}" ros-jazzy-image-proc ros-jazzy-stereo-image-proc \
    ros-jazzy-cv-bridge ros-jazzy-sensor-msgs ros-jazzy-stereo-msgs \
    ros-jazzy-rosbag2-py ros-jazzy-rosbag2-storage-default-plugins av
  cat > "$perception_prefix/activate.sh" <<EOS
export MAMBA_ROOT_PREFIX="$perception_prefix/mamba"
eval "\$("$perception_mm" shell hook -s bash)"
micromamba activate "$perception_prefix/env"
export ROS_AUTOMATIC_DISCOVERY_RANGE=OFF
export ROS_LOG_DIR=\${ROS_LOG_DIR:-\${TMPDIR:-/tmp}/letros-perception-log-\$USER}
if [[ -f "$perception_prefix/ws/install/setup.bash" ]]; then
  source "$perception_prefix/ws/install/setup.bash"
fi
true
EOS
fi
if [[ "$perception_stage" == env ]]; then
  echo "Environment ready. Next: bash $0 '$perception_prefix' build"
  exit 0
fi
[[ -f "$perception_prefix/activate.sh" ]] || { echo 'run the env stage first' >&2; exit 1; }
for perception_package in let_ros let_ros_test_nodes let_ros_perception; do
  perception_link=$perception_prefix/ws/src/$perception_package
  perception_source=$perception_root/$perception_package
  if [[ -e "$perception_link" || -L "$perception_link" ]]; then
    [[ -L "$perception_link" && "$(readlink -f "$perception_link")" == "$perception_source" ]] || {
      echo "workspace source already exists at $perception_link; use a new prefix" >&2; exit 1;
    }
  else
    ln -s "$perception_source" "$perception_link"
  fi
done
set +u
source "$perception_prefix/activate.sh"
set -u
cd "$perception_prefix/ws"
export CMAKE_BUILD_PARALLEL_LEVEL=${CMAKE_BUILD_PARALLEL_LEVEL:-4}
colcon build --executor sequential --packages-select let_ros let_ros_test_nodes let_ros_perception \
  --cmake-clean-first \
  --cmake-args -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release
set +u
source "$perception_prefix/ws/install/setup.bash"
set -u
bash "$perception_prefix/ws/install/let_ros/lib/let_ros/selftest.sh"
echo "Ready: source $perception_prefix/activate.sh"
