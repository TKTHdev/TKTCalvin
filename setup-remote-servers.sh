#!/bin/bash

# Script to setup remote servers in parallel
# - Skips the first IP (current machine)
# - Runs apt update, installs build-essential, and runs install-ext
# - Logs output for each server

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IPS_FILE="$SCRIPT_DIR/ips.txt"
LOG_DIR="$SCRIPT_DIR/logs"
INSTALL_SCRIPT="$SCRIPT_DIR/install-ext"

# Create logs directory
mkdir -p "$LOG_DIR"

# Read all IPs into an array
mapfile -t IPS < "$IPS_FILE"

# Skip the first IP (current machine)
REMOTE_IPS=("${IPS[@]:1}")

echo "================================================"
echo "Remote Server Setup Script"
echo "================================================"
echo "Total servers in ips.txt: ${#IPS[@]}"
echo "Skipping first IP (this machine): ${IPS[0]}"
echo "Remote servers to configure: ${#REMOTE_IPS[@]}"
echo "Log directory: $LOG_DIR"
echo "================================================"

# Function to setup a single server
setup_server() {
    local ip=$1
    local log_file="$LOG_DIR/setup_${ip}.log"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting setup for $ip" | tee "$log_file"
    
    # SSH to the server and run commands
    ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=30 "$ip" bash -s << 'REMOTE_SCRIPT' >> "$log_file" 2>&1
        set -e
        echo "=== Starting setup on $(hostname) ==="
        echo "Date: $(date)"
        
        # Update apt
        echo "=== Running apt update ==="
        sudo apt update -y
        
        # Install build-essential
        echo "=== Installing build-essential ==="
        sudo apt install -y build-essential
        
        # Clone the repository
        echo "=== Cloning CalvinDB repository ==="
        cd $HOME
        rm -rf TKTCalvin
        rm -rf CalvinDB
        git clone https://github.com/yunhaom94/TKTCalvin.git CalvinDB
        cd CalvinDB
        
        
        # Change to CalvinDB directory and run install-ext
        echo "=== Running install-ext ==="
        cd ~/CalvinDB
        ./install-ext
        
        echo "=== Setup complete on $(hostname) ==="
REMOTE_SCRIPT
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: Setup completed for $ip" | tee -a "$log_file"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] FAILED: Setup failed for $ip (exit code: $exit_code)" | tee -a "$log_file"
    fi
    
    return $exit_code
}

# Export function and variables for parallel execution
export -f setup_server
export LOG_DIR

# Run setup in parallel for all remote servers
echo ""
echo "Starting parallel setup for ${#REMOTE_IPS[@]} servers..."
echo ""

# Use background processes for parallel execution
pids=()
for ip in "${REMOTE_IPS[@]}"; do
    setup_server "$ip" &
    pids+=($!)
    echo "Started setup for $ip (PID: ${pids[-1]})"
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
echo "Setup Complete"
echo "================================================"
echo "Total servers processed: ${#REMOTE_IPS[@]}"
echo "Failed: $failed"
echo "Succeeded: $((${#REMOTE_IPS[@]} - failed))"
echo ""
echo "Log files are available in: $LOG_DIR"
echo "================================================"

exit $failed
