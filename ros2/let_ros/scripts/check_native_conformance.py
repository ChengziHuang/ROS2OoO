"""Compare ordinary nodes under native ROS 2 and the simulator on this host."""
import argparse
import json
import os
from pathlib import Path
import subprocess

def execute(command, stem, env):
    result = subprocess.run(command, capture_output=True, text=True, env=env, timeout=15)
    Path(str(stem) + '.out').write_text(result.stdout)
    Path(str(stem) + '.err').write_text(result.stderr)
    Path(str(stem) + '.command.json').write_text(json.dumps(command, indent=2))
    received = []
    order = []
    metadata = []
    for line in result.stdout.splitlines():
        if line.startswith('CONFORMANCE_ORDER '):
            _, topic, sequence, state = line.split()
            order.append((topic + sequence, int(state)))
        elif line.startswith('CONFORMANCE_RX '):
            _, sequence, matches = line.split()
            received.append((int(sequence), int(matches)))
        elif line.startswith('NATIVE_RESULT '):
            metadata.append(json.loads(line[len('NATIVE_RESULT '):]))
        elif line.startswith('{'):
            metadata.append(json.loads(line))
    return {'exit': result.returncode, 'received': received, 'order': order, 'metadata': metadata}

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--native', required=True)
    ap.add_argument('--simulator', required=True)
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--observe-before-fix', action='store_true')
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, ROS_DOMAIN_ID='177', ROS_AUTOMATIC_DISCOVERY_RANGE='LOCALHOST')
    scenarios = ['reliable', 'best_effort', 'reliable_to_best_effort',
                 'qos_mismatch', 'durability_mismatch', 'type_mismatch', 'depth_drop',
                 'constructor_latched', 'duplicate_publisher', 'record_order']
    verdicts = []
    for scenario in scenarios:
        mismatch = scenario in ('qos_mismatch', 'durability_mismatch', 'type_mismatch')
        shallow = scenario == 'depth_drop'
        initial = scenario == 'constructor_latched'
        total = 32 if shallow else 1 if initial else 10
        source = ('let_ros_test_nodes let_ros_test::ConformanceSource'
                  f' -p total:={total} -p burst:={total if shallow else 1}'
                  f' -p reliable:={str(scenario not in ("qos_mismatch", "best_effort")).lower()}')
        sink_type = 'ConformanceStringSink' if scenario == 'type_mismatch' else 'ConformanceSink'
        sink = (f'let_ros_test_nodes let_ros_test::{sink_type}'
                f' -p reliable:={str(scenario not in ("best_effort", "reliable_to_best_effort")).lower()}')
        if scenario == 'durability_mismatch':
            sink += ' -p transient:=true'
        if shallow:
            sink += ' -p depth:=1'
        if initial:
            source += ' -p initial_publish:=true -p transient:=true'
            sink += ' -p transient:=true'
        if scenario == 'duplicate_publisher':
            source += ' -p duplicate_publisher:=true'
        if scenario in ('qos_mismatch', 'durability_mismatch'):
            source += ' -p expected_matches:=0'
        elif scenario != 'type_mismatch':
            source += ' -p expected_matches:=1'
        graph = a.out / f'{scenario}.txt'
        if scenario == 'record_order':
            graph.write_text(
                'let_ros_test_nodes let_ros_test::ConformanceSource -r __node:=order_a '
                '-r conformance:=a -p total:=2 -p burst:=2\n'
                'let_ros_test_nodes let_ros_test::ConformanceSource -r __node:=order_b '
                '-r conformance:=b -p total:=2 -p burst:=2\n'
                'let_ros_test_nodes let_ros_test::ConformanceOrderSink\n')
        else:
            graph.write_text(source + '\n' + sink + '\n')
        native = [execute([a.native, scenario], a.out / f'{scenario}.native{i}', env)
                  for i in range(3)]
        sim = []
        for label, options in [('seq', ['--engine', 'seq']),
                               ('calendar', ['--engine', 'seq', '--model', 'calendar']),
                               ('ooo', ['--engine', 'ooo', '--workers', '3'])]:
            command = [a.simulator, '--nodes', str(graph), '--horizon', '130ms',
                       '--default-deadline', '10ms', *options]
            sim.append(execute(command, a.out / f'{scenario}.{label}', env))
        rejected_type = scenario == 'type_mismatch' and all(
            r['exit'] == 2 and r['metadata'] and r['metadata'][0].get('creation_rejected')
            for r in native)
        if scenario == 'record_order':
            expected_order = ['A0', 'A1', 'B0', 'B1']
            ok = (all(r['exit'] == 0 and len(r['order']) == 4 and
                      [v[0] for v in r['order']] != expected_order for r in native) and
                  all(r['exit'] == 0 and [v[0] for v in r['order']] == expected_order for r in sim) and
                  len({tuple(r['order']) for r in sim}) == 1)
            kind = 'native_ready_set_interleaving_vs_explicit_record_batching'
        elif initial or scenario == 'duplicate_publisher':
            expected = [(0, 0)] if initial else [(n, 1) for n in range(10)]
            diagnostic = 'outside a callback job' if initial else 'at most one publisher'
            errors = [(a.out / f'{scenario}.{label}.err').read_text()
                      for label in ('seq', 'calendar', 'ooo')]
            ok = (all(r['exit'] == 0 and r['received'] == expected for r in native) and
                  all(r['exit'] == 2 and not r['received'] for r in sim) and
                  all(diagnostic in error for error in errors))
            kind = 'explicit_rejection_of_native_behavior_outside_current_model'
        elif rejected_type:
            errors = [(a.out / f'{scenario}.{label}.err').read_text()
                      for label in ('seq', 'calendar', 'ooo')]
            ok = (all(r['exit'] == 2 for r in sim) and
                  all('type' in error.lower() and 'subscription' in error.lower() for error in errors))
            kind = 'native_and_simulator_reject_type_conflict_during_construction'
        elif shallow:
            ok = (all(r['exit'] == 0 and r['metadata'][0]['sent'] == 32 and
                      0 < len(r['received']) < 32 for r in native) and
                  all(r['exit'] == 0 and [n for n, _ in r['received']] == list(range(32))
                      for r in sim))
            kind = 'expected_finite_history_vs_lossless_model_difference'
        else:
            expected = [] if mismatch else [(n, 1) for n in range(10)]
            ok = all(r['exit'] == 0 and r['received'] == expected for r in native + sim)
            kind = 'matching_payload_order_and_connection_semantics'
        verdict = {'scenario': scenario, 'ok': ok, 'kind': kind,
                   'native': native, 'simulator': sim}
        verdicts.append(verdict)
        print(f'{scenario}: {"PASS" if ok else "MISMATCH"}, '
              f'native receives {[len(r["received"]) for r in native]}, '
              f'sim receives {[len(r["received"]) for r in sim]}', flush=True)
    (a.out / 'summary.json').write_text(json.dumps(verdicts, indent=2) + '\n')
    return 0 if a.observe_before_fix or all(r['ok'] for r in verdicts) else 1

if __name__ == '__main__':
    raise SystemExit(main())
