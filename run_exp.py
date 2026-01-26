#!/usr/bin/python3

import json
import argparse
import subprocess
import time
import os
import re
import statistics
from pathlib import Path
from datetime import datetime

# Import from start_server.py
from start_server import start_server, stop_server, send_config

CLIENT_BIN_PATH = str(Path(__file__).resolve().parent.parent) + "/src/Client/bin/Release/net9.0/Client"
CLIENT_DIR_PATH = str(Path(__file__).resolve().parent.parent) + "/src/Client"
SCRIPTS_PATH = str(Path(__file__).resolve().parent)
CONFIGS_PATH = SCRIPTS_PATH + "/configs"
TEMP_PATH = SCRIPTS_PATH + "/temp"
RESULTS_PATH = SCRIPTS_PATH + "/results"

MINERVA_CONFIG_FIELDS = {
    "ReadStorage": "/home/ubuntu/minerva_data",
    "DatabaseToLoad": [],
    "SolverExact": None,
    "ReplicaPriority": None,
    "LocalEpochInterval": None,
    "CoordinatorGlobalEpochInterval": None
}

BENCHMARK_CONFIG_FIELDS = {
    "benchmark_name": "Minerva_Benchmark",
    "benchmark_type": None,
    "duration": None,
    "clients": None,
    "pre_load_db": True,
    "servers": []
}

TPCC_CONFIG_FIELDS = {
    "NumWarehouse": None
}

YCSB_CONFIG_FIELDS = {
    "YCSB_type": None,
    "contention_ratio": None,
    "transaction_size": None,
    "key_size": None,
    "value_size": None,
    "record_count": None,
    "keyfile": "ycsb_keys.bin"
}

# Define which fields can be variable in each config section
VARIABLE_FIELDS = {
    "benchmark": ["clients"],
    "ycsb": ["YCSB_type", "contention_ratio"],
    "tpcc": [],
    "minerva": ["SolverExact", "LocalEpochInterval", "CoordinatorGlobalEpochInterval"],
    "shared": ["servers"],  # Affects both configs
    "network": ["Latency"]  # Network latency settings: list of (delay_ms, jitter_ms) tuples
}

# Path to network simulation script
NETWORK_SIM_SCRIPT = SCRIPTS_PATH + "/network_sim.sh"


class ExpConfigGenerator:
    """Class to generate experiment configurations with variable field tracking."""
    
    def __init__(self, template: dict, ip_list: list):
        self.template = template
        self.ip_list = ip_list
        
        self._variable_field = None
        self._variable_values = None
        self._variable_category = None  # 'benchmark', 'minerva', or 'shared'
        
        # Output: list of (exp_config, minerva_config) pairs
        self.config_pairs = []
    
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
            raise ValueError(f"Variable field already set to '{self._variable_field}', cannot set to '{field_name}'")
        self._variable_field = field_name
        self._variable_values = values
        self._variable_category = category
    
    def check_and_set_variable(self, config_section: dict, field_name: str, value, category: str):
        """
        Check if value is a list; if so, set it as the variable field.
        Assigns the value to config_section regardless.
        """
        if isinstance(value, list):
            self.set_variable_field(field_name, value, category)
        config_section[field_name] = value
        return value
    
    def _create_base_exp_config(self, num_servers: int) -> dict:
        """Create base experiment config for a given number of servers."""
        exp_config = {
            "BenchmarkConfig": BENCHMARK_CONFIG_FIELDS.copy(),
        }
        exp_config["BenchmarkConfig"]["servers"] = []
        exp_config["BenchmarkConfig"]["benchmark_type"] = self.template["BenchmarkConfig"]["benchmark_type"]
        exp_config["BenchmarkConfig"]["duration"] = self.template["BenchmarkConfig"]["duration"]
        exp_config["BenchmarkConfig"]["clients"] = self.template["BenchmarkConfig"]["clients"]
        
        for i in range(num_servers):
            exp_config["BenchmarkConfig"]["servers"].append(f"{self.ip_list[i]}:5000")
        
        return exp_config
    
    def _create_base_minerva_config(self, num_servers: int) -> dict:
        """Create base Minerva config for a given number of servers."""
        minerva_config = MINERVA_CONFIG_FIELDS.copy()
        minerva_template = self.template["MinervaConfig"]
        
        minerva_config["SolverExact"] = minerva_template["SolverExact"]
        minerva_config["LocalEpochInterval"] = minerva_template["LocalEpochInterval"]
        minerva_config["CoordinatorGlobalEpochInterval"] = minerva_template["CoordinatorGlobalEpochInterval"]
        
        # ReplicaPriority is based on number of servers: [0], [0, 1], [0, 1, 2], etc.
        minerva_config["ReplicaPriority"] = list(range(num_servers))
        
        # DatabaseToLoad based on benchmark type
        benchmark_type = self.template["BenchmarkConfig"]["benchmark_type"]
        minerva_config["DatabaseToLoad"] = [benchmark_type]
        
        return minerva_config
    
    def _add_benchmark_specific_config(self, exp_config: dict):
        """Add TPCC or YCSB specific configuration."""
        benchmark_type = self.template["BenchmarkConfig"]["benchmark_type"]
        
        if benchmark_type == "TPCC":
            exp_config["TPCCConfig"] = TPCC_CONFIG_FIELDS.copy()
            exp_config["TPCCConfig"]["NumWarehouse"] = self.template["TPCCConfig"]["NumWarehouse"]
            
        elif benchmark_type == "YCSB":
            exp_config["YCSBConfig"] = YCSB_CONFIG_FIELDS.copy()
            ycsb_exp = self.template["YCSBConfig"]
            ycsb_cfg = exp_config["YCSBConfig"]
            
            ycsb_cfg["YCSB_type"] = ycsb_exp["YCSB_type"]
            ycsb_cfg["contention_ratio"] = ycsb_exp["contention_ratio"]
            ycsb_cfg["transaction_size"] = ycsb_exp["transaction_size"]
            ycsb_cfg["key_size"] = ycsb_exp["key_size"]
            ycsb_cfg["value_size"] = ycsb_exp["value_size"]
            ycsb_cfg["record_count"] = ycsb_exp["record_count"]
        else:
            raise ValueError("Unsupported benchmark type")
    
    def _detect_variable_field(self, probe_run: bool):
        """Detect which field is variable in the template."""
        if probe_run:
            self.set_variable_field("clients", 
                [200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600, 2800, 3000],
                "benchmark")
            return
        
        # Check servers (shared)
        servers_config = self.template["BenchmarkConfig"]["servers"]
        if isinstance(servers_config, list):
            self.set_variable_field("servers", servers_config, "shared")
            return
        
        # Check benchmark fields
        clients = self.template["BenchmarkConfig"]["clients"]
        if isinstance(clients, list):
            self.set_variable_field("clients", clients, "benchmark")
            return
        
        # Check YCSB fields
        if self.template["BenchmarkConfig"]["benchmark_type"] == "YCSB":
            ycsb = self.template["YCSBConfig"]
            if isinstance(ycsb["YCSB_type"], list):
                self.set_variable_field("YCSB_type", ycsb["YCSB_type"], "benchmark")
                return
            if isinstance(ycsb["contention_ratio"], list):
                self.set_variable_field("contention_ratio", ycsb["contention_ratio"], "benchmark")
                return
        
        # Check minerva fields
        minerva = self.template["MinervaConfig"]
        if isinstance(minerva["SolverExact"], list):
            self.set_variable_field("SolverExact", minerva["SolverExact"], "minerva")
            return
        if isinstance(minerva["LocalEpochInterval"], list):
            self.set_variable_field("LocalEpochInterval", minerva["LocalEpochInterval"], "minerva")
            return
        if isinstance(minerva["CoordinatorGlobalEpochInterval"], list):
            self.set_variable_field("CoordinatorGlobalEpochInterval", minerva["CoordinatorGlobalEpochInterval"], "minerva")
            return
        
        # Check Latency field (top-level, network category)
        if "Latency" in self.template and isinstance(self.template["Latency"], list) and len(self.template["Latency"]) > 0:
            self.set_variable_field("Latency", self.template["Latency"], "network")
            return
    
    def _deep_copy(self, obj):
        """Create a deep copy of a dict."""
        return json.loads(json.dumps(obj))
    
    def _set_variable_value_in_config(self, exp_config: dict, minerva_config: dict, value):
        """Set the variable field value in the appropriate config."""
        if self._variable_field == "servers":
            # Rebuild server list for the specific count
            exp_config["BenchmarkConfig"]["servers"] = [f"{self.ip_list[i]}:5000" for i in range(value)]
            minerva_config["ReplicaPriority"] = list(range(value))
        elif self._variable_field == "clients":
            exp_config["BenchmarkConfig"]["clients"] = value
        elif self._variable_field == "YCSB_type":
            exp_config["YCSBConfig"]["YCSB_type"] = value
        elif self._variable_field == "contention_ratio":
            exp_config["YCSBConfig"]["contention_ratio"] = value
        elif self._variable_field == "SolverExact":
            minerva_config["SolverExact"] = value
        elif self._variable_field == "LocalEpochInterval":
            minerva_config["LocalEpochInterval"] = value
        elif self._variable_field == "CoordinatorGlobalEpochInterval":
            minerva_config["CoordinatorGlobalEpochInterval"] = value
        elif self._variable_field == "Latency":
            # Latency doesn't modify configs - it's handled at experiment runtime
            pass
    
    def generate(self, probe_run: bool = False):
        """
        Generate all config pairs based on variable fields.
        
        Returns list of (exp_config, minerva_config) tuples.
        Relationships:
        - Variable in benchmark (clients, YCSB_type, contention_ratio): 1 minerva → N exp configs
        - Variable in shared (servers): N minerva → N exp configs
        - Variable in minerva (SolverExact, etc.): N minerva → 1 exp config
        """
        self.config_pairs = []
        
        # Detect which field is variable
        self._detect_variable_field(probe_run)
        
        # Get number of servers (use first value if variable, otherwise use the single value)
        servers_config = self.template["BenchmarkConfig"]["servers"]
        if isinstance(servers_config, list):
            num_servers = max(servers_config)  # Use max for base config
        else:
            num_servers = servers_config
        
        # Create base configs
        base_exp_config = self._create_base_exp_config(num_servers)
        self._add_benchmark_specific_config(base_exp_config)
        base_minerva_config = self._create_base_minerva_config(num_servers)
        
        # No variable field - single pair
        if self._variable_field is None:
            self.config_pairs.append((base_exp_config, base_minerva_config))
            return self.config_pairs
        
        # Generate pairs based on variable category
        if self._variable_category == "benchmark":
            # 1 minerva config → N exp configs
            for value in self._variable_values:
                exp_copy = self._deep_copy(base_exp_config)
                minerva_copy = self._deep_copy(base_minerva_config)
                self._set_variable_value_in_config(exp_copy, minerva_copy, value)
                self.config_pairs.append((exp_copy, minerva_copy))
                
        elif self._variable_category == "shared":
            # N minerva configs → N exp configs (servers variable)
            for value in self._variable_values:
                exp_copy = self._deep_copy(base_exp_config)
                minerva_copy = self._deep_copy(base_minerva_config)
                self._set_variable_value_in_config(exp_copy, minerva_copy, value)
                self.config_pairs.append((exp_copy, minerva_copy))
                
        elif self._variable_category == "minerva":
            # N minerva configs → 1 exp config
            for value in self._variable_values:
                exp_copy = self._deep_copy(base_exp_config)
                minerva_copy = self._deep_copy(base_minerva_config)
                self._set_variable_value_in_config(exp_copy, minerva_copy, value)
                self.config_pairs.append((exp_copy, minerva_copy))
                
        elif self._variable_category == "network":
            # N latency settings → N experiments (configs unchanged, latency applied at runtime)
            for value in self._variable_values:
                exp_copy = self._deep_copy(base_exp_config)
                minerva_copy = self._deep_copy(base_minerva_config)
                # No config changes needed - latency is applied at experiment runtime
                self.config_pairs.append((exp_copy, minerva_copy))
        
        return self.config_pairs


# probe run means # of clients varying from 100 to 3000, but only run once
def generate_exp_config(template: dict, ip_list: list, probe_run: bool = False):
    """
    Generate experiment and minerva config pairs.
    
    Returns:
        tuple: (config_pairs, variable_field, variable_values)
        where config_pairs is a list of (exp_config, minerva_config) tuples
    """
    generator = ExpConfigGenerator(template, ip_list)
    config_pairs = generator.generate(probe_run)
    return config_pairs, generator.variable_field, generator.variable_values


def load_ip_list(ip_file: str) -> list:
    """Load IP addresses from a text file (one IP per line)."""
    with open(ip_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def load_template(template_file: str) -> dict:
    """Load experiment template from JSON file."""
    with open(template_file, 'r') as f:
        return json.load(f)


class ExperimentRunner:
    """Class to run experiments based on generated configurations."""
    
    # Number of repetitions per experiment
    DEFAULT_REPETITIONS = 5
    PROBE_REPETITIONS = 1
    
    # Wait time after starting servers (seconds)
    SERVER_STARTUP_WAIT = 30
    
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
        self.config_pairs, self.variable_field, self.variable_values = generate_exp_config(
            self.template, self.ip_list, self.probe_run
        )
        
        # Create results directory with template name and timestamp
        template_name = Path(template_file).stem  # Get filename without extension
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results_dir = f"{RESULTS_PATH}/{template_name}_{self.timestamp}"
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Ensure temp directory exists
        os.makedirs(TEMP_PATH, exist_ok=True)
    
    def _write_minerva_config(self, minerva_config: dict):
        """Write minerva config to scripts/configs/minerva_config.json"""
        config_path = f"{CONFIGS_PATH}/minerva_config.json"
        with open(config_path, 'w') as f:
            json.dump(minerva_config, f, indent=4)
        print(f"  Written minerva config to {config_path}")
    
    def _write_exp_config(self, exp_config: dict, exp_name: str) -> str:
        """Write experiment config to temp directory. Returns the path."""
        os.makedirs(TEMP_PATH, exist_ok=True)
        config_path = f"{TEMP_PATH}/{exp_name}.json"
        with open(config_path, 'w') as f:
            json.dump(exp_config, f, indent=4)
        print(f"  Written experiment config to {config_path}")
        return config_path
    
    def _stop_server(self, preserve_logs: bool = False):
        """Stop any running servers, optionally preserving logs."""
        print("  Stopping any existing servers...")
        # Delete logs unless preserve_logs is True or single_run mode is enabled
        delete_log = not (preserve_logs or self.single_run)
        try:
            stop_server(delete_log=delete_log)
            print(f"  Servers stopped successfully (logs {'preserved' if not delete_log else 'deleted'})")
        except Exception as e:
            print(f"  No servers to stop or already stopped: {e}")
    
    def _start_server(self, num_servers: int):
        """Start servers using start_server."""
        print(f"  Starting {num_servers} server(s)...")
        
        # Get the IPs needed for this experiment
        host_ips = self.ip_list[:num_servers]
        
        # Send config files to remote servers
        send_config(host_ips)
        
        # Start servers (1 server per host)
        start_server(1, host_ips)
        
        print(f"  Servers started. Waiting {self.SERVER_STARTUP_WAIT} seconds for initialization...")
        time.sleep(self.SERVER_STARTUP_WAIT)
        print("  Server startup wait complete")
    
    def _run_client(self, exp_config_path: str) -> str:
        """Run the client benchmark and return stdout output."""
        print(f"  Running client benchmark...")
        
        result = subprocess.run(
            [CLIENT_BIN_PATH, exp_config_path],
            capture_output=True,
            text=True,
            cwd=CLIENT_DIR_PATH
        )
        
        if result.returncode != 0:
            print(f"  Client error: {result.stderr}")
        
        return result.stdout
    
    def _save_result(self, output: str, exp_idx: int, rep_idx: int, var_value, result_file, elapsed_time: float):
        """Append experiment output to results file."""
        result_file.write(f"=== Repetition {rep_idx} ===\n")
        result_file.write(f"Time: {elapsed_time:.2f} seconds\n")
        result_file.write(output)
        result_file.write("\n")
        result_file.flush()
        print(f"  Results appended to file (took {elapsed_time:.2f}s)")
    
    def _get_result_filename(self, exp_idx: int, var_value) -> str:
        """Generate result filename for an experiment."""
        if var_value is not None:
            # For Latency, format as delay_jitter for filename
            if self.variable_field == "Latency" and isinstance(var_value, (list, tuple)):
                delay, jitter = var_value
                return f"exp_{exp_idx}_{self.variable_field}_{delay}ms_{jitter}ms.txt"
            return f"exp_{exp_idx}_{self.variable_field}_{var_value}.txt"
        else:
            return f"exp_{exp_idx}.txt"
    
    def _get_num_servers(self, exp_config: dict) -> int:
        """Get the number of servers from experiment config."""
        return len(exp_config["BenchmarkConfig"]["servers"])
    
    def _apply_latency(self, delay_ms: int, jitter_ms: int):
        """
        Apply network latency to all servers using network_sim.sh.
        
        Args:
            delay_ms: Network delay in milliseconds
            jitter_ms: Jitter in milliseconds
        """
        if delay_ms == 0 and jitter_ms == 0:
            print("  Skipping latency application (delay=0, jitter=0)")
            return
        
        print(f"  Applying network latency: {delay_ms}ms delay, {jitter_ms}ms jitter...")
        
        result = subprocess.run(
            [NETWORK_SIM_SCRIPT, "-f", self.ip_file, "-m", "add", 
             "-d", f"{delay_ms}ms", "-j", f"{jitter_ms}ms"],
            capture_output=True,
            text=True,
            cwd=SCRIPTS_PATH
        )
        
        if result.returncode != 0:
            print(f"  Warning: Failed to apply latency: {result.stderr}")
        else:
            print(f"  Network latency applied successfully")
    
    def _remove_latency(self):
        """Remove network latency from all servers using network_sim.sh."""
        print(f"  Removing network latency...")
        
        result = subprocess.run(
            [NETWORK_SIM_SCRIPT, "-f", self.ip_file, "-m", "del"],
            capture_output=True,
            text=True,
            cwd=SCRIPTS_PATH
        )
        
        if result.returncode != 0:
            print(f"  Warning: Failed to remove latency (may not have been set): {result.stderr}")
        else:
            print(f"  Network latency removed successfully")
    
    def run_single_experiment(self, exp_idx: int, exp_config: dict, minerva_config: dict, var_value, skip_latency: bool = False):
        """
        Run a single experiment (one config pair) with repetitions.
        
        An experiment consists of:
        1. Write minerva config (once)
        2. Apply network latency if Latency is the variable field (unless skip_latency=True)
        3. For each repetition:
           a. Stop existing servers
           b. Write exp config (each time since stop_server cleans temp)
           c. Start servers
           d. Wait for startup
           e. Run client
           f. Stop servers
        4. Remove network latency if it was applied
        
        Args:
            skip_latency: If True, skip applying/removing latency (used when latency is
                          already applied externally, e.g., by AutoExperimentRunner)
        """
        num_servers = self._get_num_servers(exp_config)
        exp_name = f"exp_config_{exp_idx}"
        
        # Step 1: Write minerva config (only needs to be done once)
        self._write_minerva_config(minerva_config)
        
        # Step 2: Apply network latency if this is a Latency experiment (unless skipped)
        latency_applied = False
        if not skip_latency and self.variable_field == "Latency" and var_value is not None:
            delay_ms, jitter_ms = var_value
            if delay_ms > 0 or jitter_ms > 0:
                self._apply_latency(delay_ms, jitter_ms)
                latency_applied = True
        
        # Open result file for this experiment
        result_filename = self._get_result_filename(exp_idx, var_value)
        result_path = f"{self.results_dir}/{result_filename}"
        print(f"  Results will be saved to {result_path}")
        
        try:
            with open(result_path, 'w') as result_file:
                # Write experiment header
                result_file.write(f"Experiment {exp_idx}\n")
                if var_value is not None:
                    result_file.write(f"{self.variable_field} = {var_value}\n")
                result_file.write(f"Repetitions: {self.repetitions}\n")
                result_file.write("=" * 60 + "\n\n")
                
                # Step 3: Run benchmark with repetitions (restart servers each time)
                for rep in range(self.repetitions):
                    rep_start_time = time.time()
                    print(f"  --- Repetition {rep + 1}/{self.repetitions} ---")
                    
                    # Stop any existing servers
                    self._stop_server()
                    
                    # Write exp config (must be after stop_server since it cleans temp dir)
                    exp_config_path = self._write_exp_config(exp_config, exp_name)
                    
                    # Start servers and wait
                    self._start_server(num_servers)
                    
                    # Run client
                    output = self._run_client(exp_config_path)
                    
                    rep_elapsed = time.time() - rep_start_time
                    self._save_result(output, exp_idx, rep + 1, var_value, result_file, rep_elapsed)
            
            # Final cleanup
            self._stop_server()
        finally:
            # Step 4: Always remove network latency if it was applied
            if latency_applied:
                self._remove_latency()
    
    def _parse_single_result_file(self, result_path: str) -> list:
        """
        Parse a single result file and extract metrics from each repetition.
        
        Returns:
            list of dicts with 'median_latency' and 'max_throughput' for each repetition
        """
        with open(result_path, 'r') as f:
            content = f.read()
        
        # Split by repetitions
        repetitions = re.split(r'=== Repetition \d+ ===', content)[1:]  # Skip header
        
        results = []
        for rep in repetitions:
            # Extract median latency from "Results for Type All" section
            # Look for the last "med: X.XX ms" which is in the "All" section
            med_matches = re.findall(r'med:\s*([\d.]+)\s*ms', rep)
            max_matches = re.findall(r'Max:\s*([\d.]+)\s*txs/sec', rep)
            
            if med_matches and max_matches:
                # Use the last match (from "Results for Type All" section)
                results.append({
                    'median_latency': float(med_matches[-1]),
                    'max_throughput': float(max_matches[-1])
                })
        
        return results
    
    def _parse_overall_throughput(self, output: str) -> float:
        """
        Parse Overall Throughput from client output.
        
        Looks for: "Overall Throughput: X.XX txs/sec" in "Results for Type All" section.
        Returns the throughput value, or 0.0 if not found.
        """
        # Find the "Results for Type All" section and extract overall throughput
        match = re.search(r'Results for Type All:.*?Overall Throughput:\s*([\d.]+)\s*txs/sec', output, re.DOTALL)
        if match:
            return float(match.group(1))
        return 0.0
    
    def _parse_all_results(self) -> dict:
        """
        Parse all result files in the results directory.
        
        Returns:
            dict mapping variable values to list of (median_latency, max_throughput) tuples
        """
        all_results = {}
        
        for i, (exp_config, minerva_config) in enumerate(self.config_pairs):
            var_value = self.variable_values[i] if self.variable_values else None
            result_filename = self._get_result_filename(i, var_value)
            result_path = f"{self.results_dir}/{result_filename}"
            
            if os.path.exists(result_path):
                rep_results = self._parse_single_result_file(result_path)
                all_results[var_value if var_value is not None else i] = rep_results
        
        return all_results
    
    def generate_csv(self):
        """
        Generate CSV summary of all experiment results.
        
        CSV columns:
        - variable_field: The variable field value
        - avg_median_latency: Average of median latency across repetitions (ms)
        - avg_max_throughput: Average of max throughput across repetitions (txs/sec)
        - std_max_throughput: Standard deviation of max throughput across repetitions
        """
        all_results = self._parse_all_results()
        
        csv_path = f"{self.results_dir}/summary.csv"
        
        with open(csv_path, 'w') as f:
            # Write header
            var_field_name = self.variable_field if self.variable_field else "experiment"
            f.write(f"{var_field_name},avg_median_latency_ms,avg_max_throughput_txs,std_max_throughput_txs\n")
            
            # Write data rows
            for var_value in sorted(all_results.keys(), key=lambda x: (x is None, x)):
                rep_results = all_results[var_value]
                
                if not rep_results:
                    continue
                
                median_latencies = [r['median_latency'] for r in rep_results]
                max_throughputs = [r['max_throughput'] for r in rep_results]
                
                avg_med_latency = statistics.mean(median_latencies)
                avg_max_throughput = statistics.mean(max_throughputs)
                
                # Standard deviation (use 0 if only one sample)
                if len(max_throughputs) > 1:
                    std_max_throughput = statistics.stdev(max_throughputs)
                else:
                    std_max_throughput = 0.0
                
                f.write(f"{var_value},{avg_med_latency:.2f},{avg_max_throughput:.2f},{std_max_throughput:.2f}\n")
        
        print(f"CSV summary saved to: {csv_path}")
        return csv_path
    
    def run_single_run(self, exp_config: dict, minerva_config: dict, clients: int) -> str:
        """
        Run a single benchmark run (no repetitions, no file saving).
        Used for probe runs to find optimal client count.
        
        Returns:
            The client output string
        """
        num_servers = self._get_num_servers(exp_config)
        
        # Modify exp_config with the specific client count
        exp_config_copy = json.loads(json.dumps(exp_config))
        exp_config_copy["BenchmarkConfig"]["clients"] = clients
        
        # Write configs
        self._write_minerva_config(minerva_config)
        
        # Stop any existing servers
        self._stop_server()
        
        # Write exp config
        exp_config_path = self._write_exp_config(exp_config_copy, "probe_config")
        
        # Start servers and wait
        self._start_server(num_servers)
        
        # Run client
        output = self._run_client(exp_config_path)
        
        # Stop servers
        self._stop_server()
        
        return output
    
    def run_probe_for_optimal_clients(self, exp_config: dict, minerva_config: dict) -> int:
        """
        Run probe experiments to find optimal client count.
        
        Tests client counts: [200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600, 2800, 3000]
        Returns the client count that achieves highest Overall Throughput.
        """
        probe_clients = [200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2600, 2800, 3000]
        
        best_clients = probe_clients[0]
        best_throughput = 0.0
        
        print(f"  Running probe to find optimal client count...")
        print(f"  Testing client counts: {probe_clients}")
        
        for clients in probe_clients:
            print(f"\n  --- Probe: {clients} clients ---")
            output = self.run_single_run(exp_config, minerva_config, clients)
            throughput = self._parse_overall_throughput(output)
            print(f"  Overall Throughput: {throughput:.2f} txs/sec")
            
            if throughput > best_throughput:
                best_throughput = throughput
                best_clients = clients
        
        print(f"\n  Probe complete. Best: {best_clients} clients with {best_throughput:.2f} txs/sec")
        return best_clients
    
    def run(self):
        """Run all experiments."""
        print(f"{'=' * 60}")
        print(f"Experiment Runner")
        print(f"{'=' * 60}")
        print(f"Template: {self.template_file}")
        print(f"IP list: {self.ip_file} ({len(self.ip_list)} servers available)")
        print(f"Probe run: {self.probe_run}")
        print(f"Variable field: {self.variable_field}")
        print(f"Variable values: {self.variable_values}")
        print(f"Number of experiments: {len(self.config_pairs)}")
        print(f"Repetitions per experiment: {self.repetitions}")
        print(f"Results directory: {self.results_dir}")
        print(f"{'=' * 60}")
        print()
        
        total_start_time = time.time()
        
        for i, (exp_config, minerva_config) in enumerate(self.config_pairs):
            var_value = self.variable_values[i] if self.variable_values else None
            
            exp_start_time = time.time()
            print(f"{'=' * 60}")
            print(f"Experiment {i + 1}/{len(self.config_pairs)}")
            if var_value is not None:
                print(f"{self.variable_field} = {var_value}")
            print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'=' * 60}")
            
            self.run_single_experiment(i, exp_config, minerva_config, var_value)
            
            exp_elapsed = time.time() - exp_start_time
            print(f"Experiment {i + 1} completed in {exp_elapsed:.2f} seconds")
            print()
        
        total_elapsed = time.time() - total_start_time
        print(f"{'=' * 60}")
        print(f"All experiments completed!")
        print(f"Total time: {total_elapsed:.2f} seconds ({total_elapsed/60:.2f} minutes)")
        print(f"Results saved to: {self.results_dir}")
        
        # Generate CSV summary
        self.generate_csv()
        
        print(f"{'=' * 60}")


class AutoExperimentRunner(ExperimentRunner):
    """
    Auto mode experiment runner.
    
    For each variable field experiment:
    1. Run a probe to find optimal client count (single run per client value)
    2. Run the actual experiment with optimal client count for 5 repetitions
    """
    
    def __init__(self, template_file: str, ip_file: str):
        # Initialize without probe_run or single_run
        self.template_file = template_file
        self.ip_file = ip_file
        self.probe_run = False
        self.single_run = False
        self.repetitions = self.DEFAULT_REPETITIONS
        
        # Load inputs
        self.template = load_template(template_file)
        self.ip_list = load_ip_list(ip_file)
        
        # Generate configs (not probe mode - use template's variable field)
        self.config_pairs, self.variable_field, self.variable_values = generate_exp_config(
            self.template, self.ip_list, probe_run=False
        )
        
        # Create results directory with template name and timestamp
        template_name = Path(template_file).stem  # Get filename without extension
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results_dir = f"{RESULTS_PATH}/{template_name}_{self.timestamp}"
        os.makedirs(self.results_dir, exist_ok=True)
        
        # Ensure temp directory exists
        os.makedirs(TEMP_PATH, exist_ok=True)
        
        # Store optimal clients for each experiment
        self.optimal_clients = {}
    
    def run(self):
        """Run all experiments in auto mode."""
        print(f"{'=' * 60}")
        print(f"Auto Experiment Runner")
        print(f"{'=' * 60}")
        print(f"Template: {self.template_file}")
        print(f"IP list: {self.ip_file} ({len(self.ip_list)} servers available)")
        print(f"Mode: AUTO (probe + 5 repetitions)")
        print(f"Variable field: {self.variable_field}")
        print(f"Variable values: {self.variable_values}")
        print(f"Number of experiments: {len(self.config_pairs)}")
        print(f"Results directory: {self.results_dir}")
        print(f"{'=' * 60}")
        print()
        
        total_start_time = time.time()
        
        for i, (exp_config, minerva_config) in enumerate(self.config_pairs):
            var_value = self.variable_values[i] if self.variable_values else None
            
            exp_start_time = time.time()
            print(f"{'=' * 60}")
            print(f"Experiment {i + 1}/{len(self.config_pairs)}")
            if var_value is not None:
                print(f"{self.variable_field} = {var_value}")
            print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'=' * 60}")
            
            # Apply network latency before probe if this is a Latency experiment
            latency_applied = False
            if self.variable_field == "Latency" and var_value is not None:
                delay_ms, jitter_ms = var_value
                if delay_ms > 0 or jitter_ms > 0:
                    self._apply_latency(delay_ms, jitter_ms)
                    latency_applied = True
            
            try:
                # Step 1: Run probe to find optimal client count
                optimal_clients = self.run_probe_for_optimal_clients(exp_config, minerva_config)
                self.optimal_clients[var_value if var_value is not None else i] = optimal_clients
                
                # Step 2: Update exp_config with optimal client count
                exp_config_with_optimal = json.loads(json.dumps(exp_config))
                exp_config_with_optimal["BenchmarkConfig"]["clients"] = optimal_clients
                
                print(f"\n  Running experiment with optimal {optimal_clients} clients...")
                
                # Step 3: Run actual experiment with 5 repetitions
                # Skip latency handling since we already applied it for the probe
                self.run_single_experiment(i, exp_config_with_optimal, minerva_config, var_value, skip_latency=True)
            finally:
                # Always remove latency after the experiment
                if latency_applied:
                    self._remove_latency()
            
            exp_elapsed = time.time() - exp_start_time
            print(f"Experiment {i + 1} completed in {exp_elapsed:.2f} seconds")
            print()
        
        total_elapsed = time.time() - total_start_time
        print(f"{'=' * 60}")
        print(f"All experiments completed!")
        print(f"Total time: {total_elapsed:.2f} seconds ({total_elapsed/60:.2f} minutes)")
        print(f"Results saved to: {self.results_dir}")
        
        # Print optimal clients summary
        print(f"\nOptimal clients per experiment:")
        for var_val, clients in self.optimal_clients.items():
            print(f"  {self.variable_field}={var_val}: {clients} clients")
        
        # Generate CSV summary
        self.generate_csv()
        
        print(f"{'=' * 60}")


def run_exp(template_file: str, ip_file: str, probe_run: bool = False, single_run: bool = False, auto_mode: bool = False):
    """
    Main entry point to run experiments.
    
    Args:
        template_file: Path to the experiment template JSON file
        ip_file: Path to the IP list text file (one IP per line)
        probe_run: If True, run probe experiments with varying client counts
        single_run: If True, run only 1 repetition per experiment
        auto_mode: If True, run auto mode (probe + optimal client experiments)
    """
    if auto_mode:
        runner = AutoExperimentRunner(template_file, ip_file)
    else:
        runner = ExperimentRunner(template_file, ip_file, probe_run, single_run)
    runner.run()
    return runner


def main():
    parser = argparse.ArgumentParser(
        description="Run Minerva benchmark experiments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s Experiments/sample_ycsb.json ips.txt
  %(prog)s Experiments/sample_tpcc.json ips.txt --probe
  %(prog)s Experiments/sample_ycsb.json ips.txt --auto
        """
    )
    parser.add_argument(
        "template",
        help="Path to the experiment template JSON file"
    )
    parser.add_argument(
        "ip_list",
        help="Path to the IP list text file (one IP per line)"
    )
    parser.add_argument(
        "--probe", "-p",
        action="store_true",
        default=False,
        help="Run probe experiments with varying client counts (default: False)"
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
        help="Auto mode: probe for optimal clients, then run 5 repetitions (default: False)"
    )
    
    args = parser.parse_args()
    
    # Validate mutually exclusive options
    if args.auto and (args.probe or args.single):
        parser.error("--auto cannot be used with --probe or --single")
    
    run_exp(args.template, args.ip_list, args.probe, args.single, args.auto)


if __name__ == "__main__":
    main()