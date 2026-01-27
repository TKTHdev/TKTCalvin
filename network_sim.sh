#!/bin/bash

# Default Interface
INTERFACE="ens3"

# Help Function
usage() {
    echo "Usage: $0 -f <ip_file> -m <mode: add|del> [-d <delay>] [-j <jitter>] [-i <interface>]"
    echo ""
    echo "Options:"
    echo "  -f    Path to text file containing IP addresses (one per line)"
    echo "  -m    Mode: 'add' to apply latency, 'del' to remove it"
    echo "  -d    Delay amount (e.g., 100ms) - Required for 'add' mode"
    echo "  -j    Jitter amount (e.g., 10ms) - Required for 'add' mode"
    echo "  -i    Network interface (default: eth0)"
    echo "  -h    Show this help message"
    echo ""
    echo "Example (Add): $0 -f hosts.txt -m add -d 100ms -j 20ms"
    echo "Example (Del): $0 -f hosts.txt -m del"
    exit 1
}

# Parse Arguments
while getopts "f:m:d:j:i:h" opt; do
    case ${opt} in
        f) IP_FILE=$OPTARG ;;
        m) MODE=$OPTARG ;;
        d) DELAY=$OPTARG ;;
        j) JITTER=$OPTARG ;;
        i) INTERFACE=$OPTARG ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Validate Input
if [[ -z "$IP_FILE" ]] || [[ -z "$MODE" ]]; then
    echo "Error: IP file and Mode are required."
    usage
fi

if [[ ! -f "$IP_FILE" ]]; then
    echo "Error: File $IP_FILE not found."
    exit 1
fi

if [[ "$MODE" == "add" ]]; then
    if [[ -z "$DELAY" ]] || [[ -z "$JITTER" ]]; then
        echo "Error: Delay and Jitter are required for 'add' mode."
        usage
    fi
    # Command to add latency/jitter
    # We use 'change' instead of 'add' if a root qdisc might already exist, 
    # but strictly adhering to your request, we use 'add'.
    # Note: 'add' will fail if a root qdisc already exists.
    CMD="sudo tc qdisc add dev $INTERFACE root netem delay $DELAY $JITTER distribution normal"
elif [[ "$MODE" == "del" ]]; then
    # Command to remove latency
    CMD="sudo tc qdisc del dev $INTERFACE root"
else
    echo "Error: Mode must be 'add' or 'del'."
    usage
fi

# Parse delay/jitter values for verification (extract numeric ms value)
if [[ "$MODE" == "add" ]]; then
    DELAY_MS=$(echo "$DELAY" | sed 's/[^0-9.]//g')
    JITTER_MS=$(echo "$JITTER" | sed 's/[^0-9.]//g')
    # Calculate expected range (delay - jitter to delay + jitter)
    MIN_EXPECTED=$(echo "$DELAY_MS - $JITTER_MS" | bc)
    MAX_EXPECTED=$(echo "$DELAY_MS + $JITTER_MS" | bc)
fi

echo "---------------------------------------------------"
echo "Starting Network Emulation script..."
echo "Mode: $MODE"
echo "Target Interface: $INTERFACE"
if [[ "$MODE" == "add" ]]; then
    echo "Settings: Delay $DELAY +/- $JITTER"
    echo "Expected latency range: ${MIN_EXPECTED}ms - ${MAX_EXPECTED}ms"
fi
echo "---------------------------------------------------"

# Function to get ping latency (returns average of 2 pings)
get_ping_latencies() {
    local target_ip=$1
    # Ping 2 times and extract the time values
    ping -c 2 -W 2 "$target_ip" 2>/dev/null | grep "time=" | sed 's/.*time=\([0-9.]*\).*/\1/'
}

# Loop through IPs
while IFS= read -r IP || [ -n "$IP" ]; do
    # Skip empty lines or comments
    [[ "$IP" =~ ^#.*$ ]] || [[ -z "$IP" ]] && continue

    echo "Processing $IP..."

    # strip whitespace
    IP=$(echo "$IP" | xargs)

    # 1. Apply the Configuration
    # We use StrictHostKeyChecking=no to avoid hanging on new hosts
    # -n prevents ssh from reading stdin (which would consume remaining IPs)
    ssh -n -o StrictHostKeyChecking=no "$IP" "$CMD" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "  [✓] Command applied successfully."
    else
        echo "  [X] Failed to apply command (Machine might be down or rule already exists)."
    fi

    # 2. Verification Step using ping
    echo "  [?] Verifying configuration via ping (2 attempts)..."
    
    PING_RESULTS=$(get_ping_latencies "$IP")
    
    if [[ -z "$PING_RESULTS" ]]; then
        echo "      [X] Failed to ping host - host may be unreachable."
    else
        VERIFIED=false
        PING_COUNT=0
        
        while IFS= read -r LATENCY; do
            PING_COUNT=$((PING_COUNT + 1))
            echo "      Ping $PING_COUNT: ${LATENCY}ms"
            
            if [[ "$MODE" == "add" ]]; then
                # Check if latency falls within expected range
                IN_RANGE=$(echo "$LATENCY >= $MIN_EXPECTED && $LATENCY <= $MAX_EXPECTED" | bc)
                if [[ "$IN_RANGE" -eq 1 ]]; then
                    VERIFIED=true
                fi
            elif [[ "$MODE" == "del" ]]; then
                # Check if latency is less than 5ms (no artificial delay)
                LOW_LATENCY=$(echo "$LATENCY < 5" | bc)
                if [[ "$LOW_LATENCY" -eq 1 ]]; then
                    VERIFIED=true
                fi
            fi
        done <<< "$PING_RESULTS"
        
        if [[ "$MODE" == "add" ]]; then
            if [[ "$VERIFIED" == true ]]; then
                echo "      [✓] Verification PASSED: At least one ping within expected range (${MIN_EXPECTED}ms - ${MAX_EXPECTED}ms)"
            else
                echo "      [X] Verification FAILED: No ping within expected range (${MIN_EXPECTED}ms - ${MAX_EXPECTED}ms)"
            fi
        elif [[ "$MODE" == "del" ]]; then
            if [[ "$VERIFIED" == true ]]; then
                echo "      [✓] Verification PASSED: At least one ping < 5ms (no artificial delay)"
            else
                echo "      [X] Verification FAILED: All pings >= 5ms (delay may still be active)"
            fi
        fi
    fi
    echo "---------------------------------------------------"

done < "$IP_FILE"

echo "Done."