# Deterministic execution and parallel simulation of ROS 2 applications

This repository holds the simulator, the industrial workloads and experiment scripts, and the recorded evaluation results.

- `ros2/let_ros` is the simulator, a ROS 2 package. It loads rclcpp component nodes and runs their callbacks as jobs. `let_ros_fleet` simulates the Autoware reference system (RS) and fleets of it. `let_ros_run` simulates any graph of component nodes given as a node list.
- `ros2/let_ros_test_nodes` holds the component nodes and fixed graphs used by the simulator tests.
- `ros2/let_ros_perception`, `ros2/perception_data` and `ros2/perception_benchmark` hold the stereo vision use case (Stereo) on the R2B 2024 recordings.
- `workloads/periodic` holds the 188 periodic task networks from which the periodic synthetic graphs are made.
- `experiments` holds the industrial campaign settings (`*.conf`), their tasks, and the installation and run scripts.
- `analysis` collects the raw results of a campaign into CSV files and draws the figures.
- `results` holds the files from which the figures are drawn.

The option `--engine` chooses the simulators.

| `--engine` | Simulator |
|---|---|
| `seq` | sequential simulation |
| `tag` | Sync-I, a barrier after every instant |
| `barrier` | Sync-W, windows as long as the smallest computation delay |
| `frontier` | Async, with the lead set by `--lead` |
| `ooo` | our simulator |
| `static` | a static schedule from measured job costs |

## Synthetic evaluation set

The evaluation uses 1385 synthetic graphs: 999 random graphs, 198 parameter-sweep graphs and 188 periodic task networks.
The recorded results in `results` support analysis and plotting of the synthetic evaluation.

## Requirements

Linux on x86-64 with internet access during the installation.
The installation puts micromamba, ROS 2 Jazzy from RoboStack, the Autoware reference system and the built packages into `~/letros`, which takes about 2 GB.
Set `LETROS_PREFIX` to install somewhere else.
The figures need Python 3 with matplotlib.

## Build

```
bash experiments/install.sh
source ~/letros/activate.sh
```

The installation ends with a self test that prints one PASS or FAIL line per check.

## Run a simulation

```
BIN=~/letros/ws/install/let_ros/lib/let_ros
$BIN/let_ros_fleet --cars 8 --offset-step 37ms --horizon 10s --engine seq,barrier,ooo --workers 8 --pin
```

Each run prints one JSON line with its time, its number of jobs and a hash of all published messages.
`--help` lists all options.

## Reproduce the figures

```
python3 -m pip install matplotlib
python3 analysis/plot_figures.py results figures --png
```

The script reads the recorded evaluation results, writes the figures to `figures`, and prints the statistics used in them.
