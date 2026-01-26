#!/usr/bin/python3
"""
CalvinDB Experiment Runner

Adapts the Minerva experiment runner for CalvinDB experiments.
Runs experiments using cluster commands and parses throughput/latency results.

Usage:
    python3 run_calvin_exp.py test.json ips.txt
    python3 run_calvin_exp.py test.json ips.txt --probe
    python3 run_calvin_exp.py test.json ips.txt --single
    python3 run_calvin_exp.py test.json ips.txt --auto
"""

import argparse
import json
import os
import subprocess
import time
import re
import glob
import statistics
import select
import fcntl
from pathlib import Path
from datetime import datetime

# Paths
SCRIPTS_PATH = str(Path(__file__).resolve().parent)
RESULTS_PATH = SCRIPTS_PATH + "/results"
DATA_PATH = SCRIPTS_PATH + "/data"
TEMP_LATENCY_PATH = SCRIPTS_PATH + "/temp_latency"
CALVIN_CONF_PATH = SCRIPTS_PATH + "/calvin.conf"
CLUSTER_BIN = SCRIPTS_PATH + "/bin/scripts/cluster"

# Default config fields
BENCHMARK_CONFIG_FIELDS = {
    "benchmark_type": None,
    "duration": None,
    "batch": None,
    "servers": None
}

TPCC_CONFIG_FIELDS = {
    "NumWarehouse": None
}

YCSB_CONFIG_FIELDS = {
    "contention_ratio": None
}

# Define which fields can be variable in each config section
VARIABLE_FIELDS = {
    "benchmark": ["batch"],
    "ycsb": ["contention_ratio"],
    "tpcc": ["NumWarehouse"],
    "shared": ["servers"],
    "network": ["Latency"]
}

# Probe values for max_batch_size
PROBE_BATCH_VALUES = [30, 40, 60, 80, 100, 120]


class CalvinConfigGenerator:
    """Class to generate calvin.conf file based on IP list and server count."""
    
    def __init__(self, ip_list: list):
        self.ip_list = ip_list
    
    def generate_calvin_conf(self, num_servers: int) -> str:
        """
        Generate calvin.conf content.
        Format: node_id:replica_id:ip:port
        All nodes in replica 0, port 10001
        """
        lines = []
        for i in range(num_servers):
            if i >= len(self.ip_list):
                raise ValueError(f"Not enough IPs in list. Need {num_servers}, have {len(self.ip_list)}")
            lines.append(f"{i}:{i}:{self.ip_list[i]}:10001")
        return "\n".join(lines) + "\n"
    
    def write_calvin_conf(self, num_servers: int):
        """Write calvin.conf file."""
        content = self.generate_calvin_conf(num_servers)
        with open(CALVIN_CONF_PATH, 'w') as f:
            f.write(content)
        print(f"  Written calvin.conf with {num_servers} servers")


class ExpConfigGenerator:
    """Class to generate experiment configurations with variable field tracking."""
    
    def __init__(self, template: dict, ip_list: list):
        self.template = template
        self.ip_list = ip_list
        
        self._variable_field = None
        self._variable_values = None
        self._variable_category = None
        
        # Output: list of exp_config dicts
        self.configs = []
    
    @property
    def variable_field(self):
        return self._variable_field
    
    @property
    def variable_values(self):
        return self._variable_values
    
    @property
    def variable_category(self):
        return self._variable_category
    
    def set_variable_field(self, field_name: str, values: list, category: str):
        """Set the variable field. Can only be set once."""
        if self._variable_field is not None:
            raise ValueError(f"Variable field already set to {self._variable_field}. Cannot set to {field_name}")
        self._variable_field = field_name
        self._variable_values = values
        self._variable_category = category
    
    def _create_base_config(self, num_servers: int) -> dict:
        """Create base experiment config for a given number of servers."""
        config = {
            "BenchmarkConfig": BENCHMARK_CONFIG_FIELDS.copy(),
            "TPCCConfig": TPCC_CONFIG_FIELDS.copy(),
            "YCSBConfig": YCSB_CONFIG_FIELDS.copy()
        }
        
        config["BenchmarkConfig"]["benchmark_type"] = self.template["BenchmarkConfig"]["benchmark_type"]
        config["BenchmarkConfig"]["duration"] = self.template["BenchmarkConfig"]["duration"]
        config["BenchmarkConfig"]["batch"] = self.template["BenchmarkConfig"]["batch"]
        config["BenchmarkConfig"]["servers"] = num_servers
        
        # TPCC config
        if "TPCCConfig" in self.template:
            config["TPCCConfig"]["NumWarehouse"] = self.template["TPCCConfig"].get("NumWarehouse", 100)
        
        # YCSB config
        if "YCSBConfig" in self.template:
            config["YCSBConfig"]["contention_ratio"] = self.template["YCSBConfig"].get("contention_ratio", 0)
        
        return config
    
    def _detect_variable_field(self, probe_run: bool):
        """Detect which field is variable in the template."""
        if probe_run:
            # For probe run, batch is always the variable
            self.set_variable_field("batch", PROBE_BATCH_VALUES, "benchmark")
            return
        
        # Check servers (shared)
        servers_config = self.template["BenchmarkConfig"]["servers"]
        if isinstance(servers_config, list):
            self.set_variable_field("servers", servers_config, "shared")
            return
        
        # Check batch
        batch = self.template["BenchmarkConfig"].get("batch")
        if isinstance(batch, list):
            self.set_variable_field("batch", batch, "benchmark")
            return
        
        # Check YCSB fields
        if self.template["BenchmarkConfig"]["benchmark_type"] == "YCSB":
            ycsb = self.template.get("YCSBConfig", {})
            if isinstance(ycsb.get("contention_ratio"), list):
                self.set_variable_field("contention_ratio", ycsb["contention_ratio"], "ycsb")
                return
        
        # Check TPCC fields
        if self.template["BenchmarkConfig"]["benchmark_type"] == "TPCC":
            tpcc = self.template.get("TPCCConfig", {})
            if isinstance(tpcc.get("NumWarehouse"), list):
                self.set_variable_field("NumWarehouse", tpcc["NumWarehouse"], "tpcc")
                return
        
        # Check Latency field
        if "Latency" in self.template and isinstance(self.template["Latency"], list) and len(self.template["Latency"]) > 0:
            self.set_variable_field("Latency", self.template["Latency"], "network")
            return
    
    def _deep_copy(self, obj):
        """Create a deep copy of a dict."""
        return json.loads(json.dumps(obj))
    
    def _set_variable_value_in_config(self, config: dict, value):
        """Set the variable field value in the config."""
        if self._variable_field == "servers":
            config["BenchmarkConfig"]["servers"] = value
        elif self._variable_field == "batch":
            config["BenchmarkConfig"]["batch"] = value
        elif self._variable_field == "contention_ratio":
            config["YCSBConfig"]["contention_ratio"] = value
        elif self._variable_field == "NumWarehouse":
            config["TPCCConfig"]["NumWarehouse"] = value
        elif self._variable_field == "Latency":
            config["Latency"] = value
    
    def generate(self, probe_run: bool = False):
        """
        Generate all configs based on variable fields.
        
        Returns list of config dicts.
        """
        self.configs = []
        
        # Detect which field is variable
        self._detect_variable_field(probe_run)
        
        # Get number of servers
        servers_config = self.template["BenchmarkConfig"]["servers"]
        if isinstance(servers_config, list):
            num_servers = servers_config[0]
        else:
            num_servers = servers_config
        
        # Create base config
        base_config = self._create_base_config(num_servers)
        
        # No variable field - single config
        if self._variable_field is None:
            self.configs.append(base_config)
            return self.configs
        
        # Generate configs for each variable value
        for value in self._variable_values:
            config = self._deep_copy(base_config)
            self._set_variable_value_in_config(config, value)
            self.configs.append(config)
        
        return self.configs


def load_ip_list(ip_file: str) -> list:
    """Load IP addresses from a text file (one IP per line)."""
    with open(ip_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def load_template(template_file: str) -> dict:
    """Load experiment template from JSON file."""
    with open(template_file, 'r') as f:
        return json.load(f)


def generate_exp_config(template: dict, ip_list: list, probe_run: bool = False):
    """
    Generate experiment configs.
    
    Returns:
        tuple: (configs, variable_field, variable_values)
    """
    generator = ExpConfigGenerator(template, ip_list)
    configs = generator.generate(probe_run)
    return configs, generator.variable_field, generator.variable_values


class ThroughputParser:
    """Parse throughput from CalvinDB stdout."""
    
    # Pattern: Machine: X Completed Y txns/sec
    THROUGHPUT_PATTERN = re.compile(r"Machine: (\d+) Completed ([\d.]+) txns/sec")
    
    def parse_output(self, output: str) -> dict:
        """
        Parse throughput output.
        
        Returns dict with:
            - per_machine: dict of machine_id -> list of throughputs
            - total: list of total throughputs per second
        """
        # Group by timestamp (approximate by line order)
        per_machine = {}
        lines = output.strip().split('\n')
        
        for line in lines:
            match = self.THROUGHPUT_PATTERN.search(line)
            if match:
                machine_id = int(match.group(1))
                throughput = float(match.group(2))
                if machine_id not in per_machine:
                    per_machine[machine_id] = []
                per_machine[machine_id].append(throughput)
        
        return {
            "per_machine": per_machine,
            "total": self._calculate_totals(per_machine)
        }
    
    def _calculate_totals(self, per_machine: dict) -> list:
        """
        Calculate throughput per time interval.
        
        Each machine reports its throughput independently. We take the average
        across machines for each time interval as the system throughput.
        """
        if not per_machine:
            return []
        
        # Find the machine with the most measurements
        max_len = max(len(v) for v in per_machine.values())
        
        totals = []
        for i in range(max_len):
            values = []
            for machine_id, throughputs in per_machine.items():
                if i < len(throughputs):
                    values.append(throughputs[i])
            if values:
                # Use average across machines (they should be similar)
                totals.append(statistics.mean(values))
        
        return totals
    
    def get_stats(self, throughputs: list) -> dict:
        """Calculate throughput statistics."""
        if not throughputs:
            return {"avg": 0, "max": 0, "min": 0}
        
        return {
            "avg": statistics.mean(throughputs),
            "max": max(throughputs),
            "min": min(throughputs)
        }


class LatencyParser:
    """Parse latency from CalvinDB data files."""
    
    def parse_files(self, data_dir: str = DATA_PATH) -> list:
        """
        Parse all report.* files in data directory.
        
        Returns list of all latencies.
        """
        latencies = []
        files = glob.glob(f"{data_dir}/report.*")
        
        for filepath in files:
            try:
                with open(filepath, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                latencies.append(float(line))
                            except ValueError:
                                pass
            except Exception as e:
                print(f"  Warning: Error reading {filepath}: {e}")
        
        return latencies
    
    def get_stats(self, latencies: list) -> dict:
        """Calculate latency statistics."""
        if not latencies:
            return {
                "mean": 0,
                "median": 0,
                "stddev": 0,
                "p95": 0,
                "p99": 0,
                "count": 0
            }
        
        sorted_latencies = sorted(latencies)
        n = len(sorted_latencies)
        
        return {
            "mean": statistics.mean(latencies),
            "median": statistics.median(latencies),
            "stddev": statistics.stdev(latencies) if n > 1 else 0,
            "p95": sorted_latencies[int(n * 0.95)] if n > 0 else 0,
            "p99": sorted_latencies[int(n * 0.99)] if n > 0 else 0,
            "count": n
        }


class ExperimentRunner:
    """Class to run CalvinDB experiments."""
    
    DEFAULT_REPETITIONS = 5
    PROBE_REPETITIONS = 1
    
    # Max wait time for first throughput message (seconds)
    MAX_STARTUP_WAIT = 900
    
    # Throughput pattern for detecting startup complete
    STARTUP_PATTERN = re.compile(r"Machine: \d+ Completed [\d.]+ txns/sec")
    
    def __init__(self, template_file: str, ip_file: str, probe_run: bool = False, single_run: bool = False):
        self.template_file = template_file
        self.ip_file = ip_file
        self.probe_run = probe_run
        self.single_run = single_run
        
        if single_run:
            self.repetitions = 1
        elif probe_run:
            self.repetitions = self.PROBE_REPETITIONS
        else:
            self.repetitions = self.DEFAULT_REPETITIONS
        
        # Load inputs
        self.template = load_template(template_file)
        self.ip_list = load_ip_list(ip_file)
        
        # Generate configs
        self.configs, self.variable_field, self.variable_values = generate_exp_config(
            self.template, self.ip_list, self.probe_run
        )
        
        # Config generator for calvin.conf
        self.calvin_config_gen = CalvinConfigGenerator(self.ip_list)
        
        # Parsers
        self.throughput_parser = ThroughputParser()
        self.latency_parser = LatencyParser()
        
        # Create results directory
        template_name = Path(template_file).stem
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results_dir = f"{RESULTS_PATH}/{template_name}_{self.timestamp}"
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Ensure data directory exists
        os.makedirs(DATA_PATH, exist_ok=True)
        
        # Ensure temp latency directory exists
        os.makedirs(TEMP_LATENCY_PATH, exist_ok=True)
    
    def _run_cluster_command(self, command: str, **kwargs) -> subprocess.CompletedProcess:
        """Run a cluster command."""
        cmd = [CLUSTER_BIN, f'--command={command}']
        for key, value in kwargs.items():
            cmd.append(f'--{key}={value}')
        
        print(f"  Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=SCRIPTS_PATH)
        return result
    
    def _put_config(self):
        """Copy cluster config to all servers."""
        print("  Copying config to servers...")
        result = self._run_cluster_command("put-config")
        if result.returncode != 0:
            print(f"  Warning: put-config returned {result.returncode}")
            print(f"  stderr: {result.stderr}")
    
    def _start_cluster(self, config: dict) -> subprocess.Popen:
        """
        Start the CalvinDB cluster.
        
        Returns the Popen process for capturing output.
        """
        benchmark_type = config["BenchmarkConfig"]["benchmark_type"]
        experiment = 0 if benchmark_type == "YCSB" else 1
        
        # Get hot_records based on benchmark type
        if benchmark_type == "TPCC":
            hot_records = config["TPCCConfig"]["NumWarehouse"]
        else:
            hot_records = config["YCSBConfig"]["contention_ratio"]
            if hot_records == 0:
                hot_records = 9999000
        
        # Get batch size
        batch = config["BenchmarkConfig"]["batch"]
        
        cmd = [
            CLUSTER_BIN,
            '--command=start',
            f'--experiment={experiment}',
            '--percent_mr=100',
            f'--hot_records={hot_records}',
            f'--max_batch_size={batch}'
        ]
        
        print(f"  Starting cluster: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=SCRIPTS_PATH
        )
        return process
    
    def _kill_cluster(self):
        """Stop the CalvinDB cluster."""
        print("  Stopping cluster...")
        result = self._run_cluster_command("kill")
        if result.returncode != 0:
            print(f"  Warning: kill returned {result.returncode}")
    
    def _get_data(self):
        """Get latency data from servers."""
        print("  Getting latency data...")
        result = self._run_cluster_command("get-data")
        if result.returncode != 0:
            print(f"  Warning: get-data returned {result.returncode}")
    
    def _clear_data_dir(self):
        """Clear contents of data directory."""
        files = glob.glob(f"{DATA_PATH}/*")
        for f in files:
            try:
                os.remove(f)
            except Exception as e:
                print(f"  Warning: Could not remove {f}: {e}")
    
    def _clear_temp_latency_dir(self):
        """Clear contents of temp latency directory."""
        files = glob.glob(f"{TEMP_LATENCY_PATH}/*")
        for f in files:
            try:
                os.remove(f)
            except Exception as e:
                print(f"  Warning: Could not remove {f}: {e}")
    
    def _save_latency_to_temp(self, rep_idx: int):
        """Copy latency files from data/ to temp_latency/ with rep prefix."""
        files = glob.glob(f"{DATA_PATH}/report.*")
        for filepath in files:
            filename = os.path.basename(filepath)
            dest = f"{TEMP_LATENCY_PATH}/rep{rep_idx}_{filename}"
            try:
                import shutil
                shutil.copy2(filepath, dest)
            except Exception as e:
                print(f"  Warning: Could not copy {filepath} to {dest}: {e}")
    
    def _load_all_temp_latencies(self) -> list:
        """Load all latency data from temp_latency directory."""
        latencies = []
        files = glob.glob(f"{TEMP_LATENCY_PATH}/*")
        
        for filepath in files:
            try:
                with open(filepath, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                latencies.append(float(line))
                            except ValueError:
                                pass
            except Exception as e:
                print(f"  Warning: Error reading {filepath}: {e}")
        
        return latencies
    
    def _set_nonblocking(self, fd):
        """Set file descriptor to non-blocking mode."""
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    
    def _read_available_lines(self, process: subprocess.Popen, timeout: float = 0.1) -> list:
        """
        Read all available lines from process stdout without blocking.
        
        Returns list of lines read.
        """
        lines = []
        try:
            # Use select to check if data is available
            readable, _, _ = select.select([process.stdout], [], [], timeout)
            if readable:
                while True:
                    line = process.stdout.readline()
                    if line:
                        lines.append(line)
                    else:
                        break
                    # Check if more data available
                    readable, _, _ = select.select([process.stdout], [], [], 0)
                    if not readable:
                        break
        except Exception:
            pass
        return lines
    
    def _wait_for_startup(self, process: subprocess.Popen) -> list:
        """
        Wait for the first throughput message indicating cluster is ready.
        
        Returns list of output lines captured during wait.
        """
        print(f"  Waiting for cluster startup (max {self.MAX_STARTUP_WAIT}s)...")
        
        # Set stdout to non-blocking
        self._set_nonblocking(process.stdout.fileno())
        
        start_time = time.time()
        output_lines = []
        line_buffer = ""
        
        while time.time() - start_time < self.MAX_STARTUP_WAIT:
            try:
                # Try to read available data
                readable, _, _ = select.select([process.stdout], [], [], 0.5)
                if readable:
                    chunk = process.stdout.read(4096)
                    if chunk:
                        line_buffer += chunk
                        # Process complete lines
                        while '\n' in line_buffer:
                            line, line_buffer = line_buffer.split('\n', 1)
                            line = line + '\n'
                            output_lines.append(line)
                            print(f"    {line.strip()}")
                            
                            # Check if this is a throughput message
                            if self.STARTUP_PATTERN.search(line):
                                print(f"  Cluster ready! (took {time.time() - start_time:.1f}s)")
                                return output_lines, line_buffer
            except (IOError, BlockingIOError):
                pass
            time.sleep(0.1)
        
        print(f"  Warning: Startup timeout after {self.MAX_STARTUP_WAIT}s")
        return output_lines, line_buffer
    
    def _run_cluster_for_duration(self, config: dict, verbose: bool = True) -> tuple:
        """
        Core method to run cluster for the configured duration.
        
        This is the single source of truth for how experiments are run.
        
        Args:
            config: Experiment configuration dict
            verbose: Whether to print output lines
            
        Returns:
            tuple: (output_lines, throughput_stats)
        """
        duration = config["BenchmarkConfig"]["duration"]
        
        # Start cluster
        process = self._start_cluster(config)
        
        # Wait for first throughput message (cluster ready)
        output_lines, line_buffer = self._wait_for_startup(process)
        
        # Run for duration
        if verbose:
            print(f"  Running for {duration}s...")
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration:
                # Read output non-blocking
                try:
                    readable, _, _ = select.select([process.stdout], [], [], 0.5)
                    if readable:
                        chunk = process.stdout.read(4096)
                        if chunk:
                            line_buffer += chunk
                            # Process complete lines
                            while '\n' in line_buffer:
                                line, line_buffer = line_buffer.split('\n', 1)
                                line = line + '\n'
                                output_lines.append(line)
                                if verbose:
                                    print(f"    {line.strip()}")
                except (IOError, BlockingIOError):
                    pass
                time.sleep(0.1)
        except KeyboardInterrupt:
            if verbose:
                print("\n  Interrupted by user")
        
        # Kill cluster
        self._kill_cluster()
        
        # Wait for process to finish and get remaining output
        try:
            remaining, _ = process.communicate(timeout=10)
            if remaining:
                output_lines.append(remaining)
        except subprocess.TimeoutExpired:
            process.kill()
        
        # Get latency data
        self._get_data()
        
        # Parse results
        output = ''.join(output_lines)
        throughput_result = self.throughput_parser.parse_output(output)
        throughput_stats = self.throughput_parser.get_stats(throughput_result['total'])
        
        return output_lines, throughput_result, throughput_stats
    
    def _get_num_servers(self, config: dict) -> int:
        """Get number of servers from config."""
        return config["BenchmarkConfig"]["servers"]
    
    def _get_result_filename(self, exp_idx: int, var_value) -> str:
        """Generate result filename based on variable field."""
        if self.variable_field:
            return f"exp_{exp_idx}_{self.variable_field}_{var_value}.txt"
        else:
            return f"exp_{exp_idx}.txt"
    
    def _write_result_file(self, filepath: str, throughput_stats: dict, latency_stats: dict, 
                           raw_throughputs: list, rep_idx: int):
        """Write or append results to file."""
        mode = 'a' if os.path.exists(filepath) else 'w'
        
        with open(filepath, mode) as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"Repetition {rep_idx + 1}\n")
            f.write(f"{'='*60}\n\n")
            
            f.write("Throughput (txns/sec):\n")
            f.write(f"  Average: {throughput_stats['avg']:.2f}\n")
            f.write(f"  Max: {throughput_stats['max']:.2f}\n")
            f.write(f"  Min: {throughput_stats['min']:.2f}\n")
            f.write(f"  Raw values: {raw_throughputs}\n\n")
            
            f.write("Latency (ms):\n")
            f.write(f"  Mean: {latency_stats['mean']:.2f}\n")
            f.write(f"  Median: {latency_stats['median']:.2f}\n")
            f.write(f"  Stddev: {latency_stats['stddev']:.2f}\n")
            f.write(f"  95th percentile: {latency_stats['p95']:.2f}\n")
            f.write(f"  99th percentile: {latency_stats['p99']:.2f}\n")
            f.write(f"  Sample count: {latency_stats['count']}\n")
    
    def run_single_experiment(self, exp_idx: int, config: dict, var_value):
        """Run a single experiment configuration with repetitions."""
        num_servers = self._get_num_servers(config)
        duration = config["BenchmarkConfig"]["duration"]
        
        print(f"\n{'='*60}")
        print(f"Experiment {exp_idx + 1}")
        if self.variable_field:
            print(f"  {self.variable_field} = {var_value}")
        print(f"  Servers: {num_servers}")
        print(f"  Duration: {duration}s")
        print(f"  Repetitions: {self.repetitions}")
        print(f"{'='*60}")
        
        # Setup calvin.conf
        self.calvin_config_gen.write_calvin_conf(num_servers)
        self._put_config()
        
        result_filename = self._get_result_filename(exp_idx, var_value)
        result_filepath = f"{self.results_dir}/{result_filename}"
        
        # Store all results for summary
        all_throughputs = []
        
        # Clear temp latency directory for this experiment
        self._clear_temp_latency_dir()
        
        for rep_idx in range(self.repetitions):
            print(f"\n  --- Repetition {rep_idx + 1}/{self.repetitions} ---")
            
            # Clear data directory
            self._clear_data_dir()
            
            # Run experiment using core method
            output_lines, throughput_result, throughput_stats = self._run_cluster_for_duration(config, verbose=True)
            
            # Save latency to temp before clearing
            self._save_latency_to_temp(rep_idx)
            
            latencies = self.latency_parser.parse_files()
            latency_stats = self.latency_parser.get_stats(latencies)
            
            # Store throughputs for summary
            all_throughputs.extend(throughput_result['total'])
            
            # Write results
            self._write_result_file(
                result_filepath, 
                throughput_stats, 
                latency_stats,
                throughput_result['total'],
                rep_idx
            )
            
            print(f"  Throughput: avg={throughput_stats['avg']:.2f}, max={throughput_stats['max']:.2f} txns/sec")
            print(f"  Latency: mean={latency_stats['mean']:.2f}, p99={latency_stats['p99']:.2f} ms")
            
            # Clear data for next run
            self._clear_data_dir()
        
        # Load all latencies from temp storage
        all_latencies = self._load_all_temp_latencies()
        
        # Write summary to file
        with open(result_filepath, 'a') as f:
            f.write(f"\n{'='*60}\n")
            f.write("EXPERIMENT SUMMARY\n")
            f.write(f"{'='*60}\n\n")
            
            if all_throughputs:
                f.write(f"Overall Throughput:\n")
                f.write(f"  Average: {statistics.mean(all_throughputs):.2f} txns/sec\n")
                f.write(f"  Max: {max(all_throughputs):.2f} txns/sec\n\n")
            
            if all_latencies:
                overall_latency_stats = self.latency_parser.get_stats(all_latencies)
                f.write(f"Overall Latency:\n")
                f.write(f"  Mean: {overall_latency_stats['mean']:.2f} ms\n")
                f.write(f"  Median: {overall_latency_stats['median']:.2f} ms\n")
                f.write(f"  Stddev: {overall_latency_stats['stddev']:.2f} ms\n")
                f.write(f"  95th percentile: {overall_latency_stats['p95']:.2f} ms\n")
                f.write(f"  99th percentile: {overall_latency_stats['p99']:.2f} ms\n")
        
        # Clear temp latency after experiment
        self._clear_temp_latency_dir()
        
        return {
            "throughputs": all_throughputs,
            "latencies": all_latencies,
            "var_value": var_value
        }
    
    def generate_csv(self, all_results: list):
        """Generate summary CSV file."""
        csv_path = f"{self.results_dir}/summary.csv"
        
        with open(csv_path, 'w') as f:
            # Header
            header = [
                self.variable_field if self.variable_field else "experiment",
                "throughput_avg",
                "throughput_max",
                "latency_mean",
                "latency_median",
                "latency_stddev",
                "latency_p95",
                "latency_p99",
                "sample_count"
            ]
            f.write(",".join(header) + "\n")
            
            # Data rows
            for result in all_results:
                var_value = result["var_value"]
                throughputs = result["throughputs"]
                latencies = result["latencies"]
                
                throughput_avg = statistics.mean(throughputs) if throughputs else 0
                throughput_max = max(throughputs) if throughputs else 0
                
                latency_stats = self.latency_parser.get_stats(latencies)
                
                row = [
                    str(var_value),
                    f"{throughput_avg:.2f}",
                    f"{throughput_max:.2f}",
                    f"{latency_stats['mean']:.2f}",
                    f"{latency_stats['median']:.2f}",
                    f"{latency_stats['stddev']:.2f}",
                    f"{latency_stats['p95']:.2f}",
                    f"{latency_stats['p99']:.2f}",
                    str(latency_stats['count'])
                ]
                f.write(",".join(row) + "\n")
        
        print(f"\nSummary CSV written to: {csv_path}")
    
    def run(self):
        """Run all experiments."""
        print(f"\n{'#'*60}")
        print("CalvinDB Experiment Runner")
        print(f"{'#'*60}")
        print(f"Template: {self.template_file}")
        print(f"IP file: {self.ip_file}")
        print(f"Results dir: {self.results_dir}")
        print(f"Variable field: {self.variable_field}")
        print(f"Variable values: {self.variable_values}")
        print(f"Total experiments: {len(self.configs)}")
        print(f"Repetitions per experiment: {self.repetitions}")
        
        all_results = []
        
        for exp_idx, config in enumerate(self.configs):
            var_value = self.variable_values[exp_idx] if self.variable_values else None
            result = self.run_single_experiment(exp_idx, config, var_value)
            all_results.append(result)
        
        # Generate summary CSV
        self.generate_csv(all_results)
        
        print(f"\n{'#'*60}")
        print("All experiments completed!")
        print(f"Results saved to: {self.results_dir}")
        print(f"{'#'*60}")
        
        return all_results
    
    def run_single_run(self, config: dict, batch: int) -> dict:
        """Run a single experiment with given batch size (for probing)."""
        config = json.loads(json.dumps(config))  # Deep copy
        config["BenchmarkConfig"]["batch"] = batch
        
        num_servers = self._get_num_servers(config)
        
        # Setup calvin.conf
        self.calvin_config_gen.write_calvin_conf(num_servers)
        self._put_config()
        
        # Clear data directory
        self._clear_data_dir()
        
        # Run experiment using core method
        output_lines, throughput_result, throughput_stats = self._run_cluster_for_duration(config, verbose=True)
        
        # Clear data
        self._clear_data_dir()
        
        return throughput_stats
    
    def run_probe_for_optimal_batch(self, config: dict) -> int:
        """
        Probe to find optimal batch size.
        
        Returns the batch size with highest throughput.
        """
        print("\n  --- Probing for optimal batch size ---")
        
        best_batch = PROBE_BATCH_VALUES[0]
        best_throughput = 0
        
        for batch in PROBE_BATCH_VALUES:
            print(f"\n  Testing batch={batch}...")
            stats = self.run_single_run(config, batch)
            
            print(f"    Throughput: avg={stats['avg']:.2f}, max={stats['max']:.2f}")
            
            if stats['avg'] > best_throughput:
                best_throughput = stats['avg']
                best_batch = batch
        
        print(f"\n  Optimal batch size: {best_batch} (throughput: {best_throughput:.2f})")
        return best_batch


class AutoExperimentRunner(ExperimentRunner):
    """
    Auto mode experiment runner.
    
    For each variable field experiment:
    1. Run a probe to find optimal batch size (single run per batch value)
    2. Run the actual experiment with optimal batch size for 5 repetitions
    """
    
    def __init__(self, template_file: str, ip_file: str):
        # Don't call parent __init__ with probe_run=True
        self.template_file = template_file
        self.ip_file = ip_file
        self.probe_run = False
        self.single_run = False
        self.repetitions = self.DEFAULT_REPETITIONS
        
        # Load inputs
        self.template = load_template(template_file)
        self.ip_list = load_ip_list(ip_file)
        
        # Generate configs (not probe mode - use actual variable)
        self.configs, self.variable_field, self.variable_values = generate_exp_config(
            self.template, self.ip_list, probe_run=False
        )
        
        # Config generator for calvin.conf
        self.calvin_config_gen = CalvinConfigGenerator(self.ip_list)
        
        # Parsers
        self.throughput_parser = ThroughputParser()
        self.latency_parser = LatencyParser()
        
        # Create results directory
        template_name = Path(template_file).stem
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results_dir = f"{RESULTS_PATH}/{template_name}_auto_{self.timestamp}"
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Ensure data directory exists
        os.makedirs(DATA_PATH, exist_ok=True)
        
        # Ensure temp latency directory exists
        os.makedirs(TEMP_LATENCY_PATH, exist_ok=True)
        
        # Store optimal batch per config
        self.optimal_batches = {}
    
    def run(self):
        """Run all experiments in auto mode."""
        print(f"\n{'#'*60}")
        print("CalvinDB Auto Experiment Runner")
        print(f"{'#'*60}")
        print(f"Template: {self.template_file}")
        print(f"IP file: {self.ip_file}")
        print(f"Results dir: {self.results_dir}")
        print(f"Variable field: {self.variable_field}")
        print(f"Variable values: {self.variable_values}")
        print(f"Total experiments: {len(self.configs)}")
        
        all_results = []
        
        for exp_idx, config in enumerate(self.configs):
            var_value = self.variable_values[exp_idx] if self.variable_values else None
            
            print(f"\n{'='*60}")
            print(f"Experiment {exp_idx + 1}: {self.variable_field}={var_value}")
            print(f"{'='*60}")
            
            # Phase 1: Probe for optimal batch
            optimal_batch = self.run_probe_for_optimal_batch(config)
            self.optimal_batches[exp_idx] = optimal_batch
            
            # Phase 2: Run with optimal batch
            config["BenchmarkConfig"]["batch"] = optimal_batch
            result = self.run_single_experiment(exp_idx, config, var_value)
            all_results.append(result)
        
        # Generate summary CSV
        self.generate_csv(all_results)
        
        # Write optimal batches
        with open(f"{self.results_dir}/optimal_batches.txt", 'w') as f:
            for exp_idx, batch in self.optimal_batches.items():
                var_value = self.variable_values[exp_idx] if self.variable_values else exp_idx
                f.write(f"{self.variable_field}={var_value}: optimal_batch={batch}\n")
        
        print(f"\n{'#'*60}")
        print("All auto experiments completed!")
        print(f"Results saved to: {self.results_dir}")
        print(f"{'#'*60}")
        
        return all_results


def run_exp(template_file: str, ip_file: str, probe_run: bool = False, 
            single_run: bool = False, auto_mode: bool = False):
    """
    Main entry point to run experiments.
    
    Args:
        template_file: Path to the experiment template JSON file
        ip_file: Path to the IP list text file (one IP per line)
        probe_run: If True, run probe experiments with varying batch sizes
        single_run: If True, run only 1 repetition per experiment
        auto_mode: If True, run auto mode (probe + optimal batch experiments)
    """
    if auto_mode:
        runner = AutoExperimentRunner(template_file, ip_file)
    else:
        runner = ExperimentRunner(template_file, ip_file, probe_run, single_run)
    runner.run()
    return runner


def parse_results_only(results_dir: str):
    """
    Parse existing results from a results directory and regenerate summary CSV.
    
    Args:
        results_dir: Path to the results directory containing experiment result files
    """
    print(f"\n{'#'*60}")
    print("CalvinDB Results Parser")
    print(f"{'#'*60}")
    print(f"Results dir: {results_dir}")
    
    if not os.path.isdir(results_dir):
        print(f"Error: Directory '{results_dir}' does not exist")
        return
    
    # Find all experiment result files (exp_*.txt)
    result_files = sorted(glob.glob(f"{results_dir}/exp_*.txt"))
    
    if not result_files:
        print(f"Error: No experiment result files (exp_*.txt) found in '{results_dir}'")
        return
    
    print(f"Found {len(result_files)} result files")
    
    # Parse each result file
    all_results = []
    latency_parser = LatencyParser()
    
    # Try to detect variable field from filenames
    # Format: exp_0_fieldname_value.txt or exp_0.txt
    variable_field = None
    
    for filepath in result_files:
        filename = os.path.basename(filepath)
        print(f"\nParsing: {filename}")
        
        # Extract variable field and value from filename
        # exp_0_batch_30.txt -> field=batch, value=30
        parts = filename.replace('.txt', '').split('_')
        if len(parts) >= 4:
            # exp_idx_field_value format
            variable_field = parts[2]
            var_value = parts[3]
            # Try to convert to number if possible
            try:
                var_value = int(var_value)
            except ValueError:
                try:
                    var_value = float(var_value)
                except ValueError:
                    pass
        else:
            var_value = parts[1] if len(parts) > 1 else "0"
        
        # Parse the result file
        throughputs = []
        latencies_stats_list = []
        
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Extract raw throughput values from "Raw values: [...]" lines
        raw_values_pattern = re.compile(r"Raw values: \[([\d., ]+)\]")
        for match in raw_values_pattern.finditer(content):
            values_str = match.group(1)
            values = [float(v.strip()) for v in values_str.split(',') if v.strip()]
            throughputs.extend(values)
        
        # Extract latency stats from each repetition
        # Look for latency sections
        latency_sections = re.findall(
            r"Latency \(ms\):\s*\n"
            r"\s*Mean: ([\d.]+)\s*\n"
            r"\s*Median: ([\d.]+)\s*\n"
            r"\s*Stddev: ([\d.]+)\s*\n"
            r"\s*95th percentile: ([\d.]+)\s*\n"
            r"\s*99th percentile: ([\d.]+)\s*\n"
            r"\s*Sample count: (\d+)",
            content
        )
        
        # Use the overall summary if available, otherwise aggregate
        overall_match = re.search(
            r"EXPERIMENT SUMMARY.*?"
            r"Overall Latency:\s*\n"
            r"\s*Mean: ([\d.]+)\s*\n"
            r"\s*Median: ([\d.]+)\s*\n"
            r"\s*Stddev: ([\d.]+)\s*\n"
            r"\s*95th percentile: ([\d.]+)\s*\n"
            r"\s*99th percentile: ([\d.]+)",
            content,
            re.DOTALL
        )
        
        if overall_match:
            latency_stats = {
                "mean": float(overall_match.group(1)),
                "median": float(overall_match.group(2)),
                "stddev": float(overall_match.group(3)),
                "p95": float(overall_match.group(4)),
                "p99": float(overall_match.group(5)),
                "count": sum(int(s[5]) for s in latency_sections) if latency_sections else 0
            }
        elif latency_sections:
            # Aggregate from repetitions (use last one's count as approximation)
            total_count = sum(int(s[5]) for s in latency_sections)
            # Weighted average based on count
            latency_stats = {
                "mean": statistics.mean([float(s[0]) for s in latency_sections]),
                "median": statistics.mean([float(s[1]) for s in latency_sections]),
                "stddev": statistics.mean([float(s[2]) for s in latency_sections]),
                "p95": statistics.mean([float(s[3]) for s in latency_sections]),
                "p99": statistics.mean([float(s[4]) for s in latency_sections]),
                "count": total_count
            }
        else:
            latency_stats = {"mean": 0, "median": 0, "stddev": 0, "p95": 0, "p99": 0, "count": 0}
        
        print(f"  Throughputs: {len(throughputs)} samples, avg={statistics.mean(throughputs) if throughputs else 0:.2f}")
        print(f"  Latency: mean={latency_stats['mean']:.2f}, p99={latency_stats['p99']:.2f}, count={latency_stats['count']}")
        
        all_results.append({
            "var_value": var_value,
            "throughputs": throughputs,
            "latency_stats": latency_stats
        })
    
    # Generate summary CSV
    csv_path = f"{results_dir}/summary.csv"
    
    with open(csv_path, 'w') as f:
        # Header
        header = [
            variable_field if variable_field else "experiment",
            "throughput_avg",
            "throughput_max",
            "latency_mean",
            "latency_median",
            "latency_stddev",
            "latency_p95",
            "latency_p99",
            "sample_count"
        ]
        f.write(",".join(header) + "\n")
        
        # Data rows
        for result in all_results:
            var_value = result["var_value"]
            throughputs = result["throughputs"]
            latency_stats = result["latency_stats"]
            
            throughput_avg = statistics.mean(throughputs) if throughputs else 0
            throughput_max = max(throughputs) if throughputs else 0
            
            row = [
                str(var_value),
                f"{throughput_avg:.2f}",
                f"{throughput_max:.2f}",
                f"{latency_stats['mean']:.2f}",
                f"{latency_stats['median']:.2f}",
                f"{latency_stats['stddev']:.2f}",
                f"{latency_stats['p95']:.2f}",
                f"{latency_stats['p99']:.2f}",
                str(latency_stats['count'])
            ]
            f.write(",".join(row) + "\n")
    
    print(f"\n{'#'*60}")
    print(f"Summary CSV written to: {csv_path}")
    print(f"{'#'*60}")


def main():
    parser = argparse.ArgumentParser(
        description="Run CalvinDB benchmark experiments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s test.json ips.txt
  %(prog)s test.json ips.txt --probe
  %(prog)s test.json ips.txt --single
  %(prog)s test.json ips.txt --auto
  %(prog)s --parse results/test_20260126_123456
        """
    )
    parser.add_argument(
        "template",
        nargs='?',
        help="Path to the experiment template JSON file"
    )
    parser.add_argument(
        "ip_list",
        nargs='?',
        help="Path to the IP list text file (one IP per line)"
    )
    parser.add_argument(
        "--probe", "-p",
        action="store_true",
        default=False,
        help="Run probe experiments with varying batch sizes (default: False)"
    )
    parser.add_argument(
        "--single", "-s",
        action="store_true",
        default=False,
        help="Run only 1 repetition per experiment (default: False, runs 5 repetitions)"
    )
    parser.add_argument(
        "--auto", "-a",
        action="store_true",
        default=False,
        help="Auto mode: probe for optimal batch, then run 5 repetitions (default: False)"
    )
    parser.add_argument(
        "--parse",
        metavar="RESULTS_DIR",
        help="Parse existing results from a results directory (no experiment run)"
    )
    
    args = parser.parse_args()
    
    # Parse-only mode
    if args.parse:
        parse_results_only(args.parse)
        return
    
    # Validate required arguments for experiment mode
    if not args.template or not args.ip_list:
        parser.error("template and ip_list are required unless using --parse")
    
    # Validate mutually exclusive options
    if args.auto and (args.probe or args.single):
        parser.error("--auto cannot be combined with --probe or --single")
    
    run_exp(args.template, args.ip_list, args.probe, args.single, args.auto)


if __name__ == "__main__":
    main()
