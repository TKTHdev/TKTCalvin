#!/bin/bash

# Script to build CalvinDB on remote servers in parallel
# - Skips the first IP (current machine)
# - Exports LD_LIBRARY_PATH to .bashrc
# - Builds CalvinDB/src
# - Verifies calvindb_server exists

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IPS_FILE="$SCRIPT_DIR/ips.txt"
LOG_DIR="$SCRIPT_DIR/logs"

# Create logs directory
mkdir -p "$LOG_DIR"

# Read all IPs into an array
mapfile -t IPS < "$IPS_FILE"

# Skip the first IP (current machine)
REMOTE_IPS=("${IPS[@]:1}")

echo "================================================"
echo "Remote Server Build Script"
echo "================================================"
echo "Total servers in ips.txt: ${#IPS[@]}"
echo "Skipping first IP (this machine): ${IPS[0]}"
echo "Remote servers to build: ${#REMOTE_IPS[@]}"
echo "Log directory: $LOG_DIR"
echo "================================================"

# Function to build on a single server
build_server() {
    local ip=$1
    local log_file="$LOG_DIR/build_${ip}.log"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting build for $ip" | tee "$log_file"
    
    # SSH to the server and run commands
    ssh -i ~/msrg -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=30 "$ip" bash -s << 'REMOTE_SCRIPT' >> "$log_file" 2>&1
        set -e
        echo "=== Starting build on $(hostname) ==="
        echo "Date: $(date)"
        
        # Define LD_LIBRARY_PATH
        LD_LIB_PATH='export LD_LIBRARY_PATH=~/CalvinDB/ext/zeromq/src/.libs:~/CalvinDB/ext/protobuf/src/.libs:~/CalvinDB/ext/glog/.libs:~/CalvinDB/ext/gflags/.libs'
        
        # Add to .bashrc if not already present
        echo "=== Configuring LD_LIBRARY_PATH in .bashrc ==="
        if ! grep -q "LD_LIBRARY_PATH=~/CalvinDB/ext" ~/.bashrc 2>/dev/null; then
            echo "" >> ~/.bashrc
            echo "# CalvinDB library paths" >> ~/.bashrc
            echo "$LD_LIB_PATH" >> ~/.bashrc
            echo "Added LD_LIBRARY_PATH to .bashrc"
        else
            echo "LD_LIBRARY_PATH already configured in .bashrc"
        fi
        
        # Export for current session
        echo "=== Exporting LD_LIBRARY_PATH for current session ==="
        export LD_LIBRARY_PATH=~/CalvinDB/ext/zeromq/src/.libs:~/CalvinDB/ext/protobuf/src/.libs:~/CalvinDB/ext/glog/.libs:~/CalvinDB/ext/gflags/.libs
        echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
        
        # Build CalvinDB
        echo "=== Building CalvinDB ==="
        cd ~/CalvinDB/src
        make -j 4
        
        # Verify calvindb_server exists
        echo "=== Verifying calvindb_server ==="
        if [ -f "$HOME/CalvinDB/bin/scripts/calvindb_server" ]; then
            echo "SUCCESS: $HOME/CalvinDB/bin/scripts/calvindb_server exists"
            ls -la "$HOME/CalvinDB/bin/scripts/calvindb_server"
        else
            echo "ERROR: $HOME/CalvinDB/bin/scripts/calvindb_server NOT FOUND"
            exit 1
        fi
        
        echo "=== Build complete on $(hostname) ==="
REMOTE_SCRIPT
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: Build completed for $ip" | tee -a "$log_file"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] FAILED: Build failed for $ip (exit code: $exit_code)" | tee -a "$log_file"
    fi
    
    return $exit_code
}

# Export function and variables for parallel execution
export -f build_server
export LOG_DIR

# Run build in parallel for all remote servers
echo ""
echo "Starting parallel build for ${#REMOTE_IPS[@]} servers..."
echo ""

# Use background processes for parallel execution
pids=()
for ip in "${REMOTE_IPS[@]}"; do
    build_server "$ip" &
    pids+=($!)
    echo "Started build for $ip (PID: ${pids[-1]})"
done

echo ""
echo "Waiting for all servers to complete..."
echo ""

# Wait for all background processes and collect results
failed=0
for i in "${!pids[@]}"; do
    wait ${pids[$i]}
    if [ $? -ne 0 ]; then
        ((failed++))
    fi
done

echo ""
echo "================================================"
echo "Build Complete"
echo "================================================"
echo "Total servers processed: ${#REMOTE_IPS[@]}"
echo "Failed: $failed"
echo "Succeeded: $((${#REMOTE_IPS[@]} - failed))"
echo ""
echo "Log files are available in: $LOG_DIR"
echo "================================================"

exit $failed
