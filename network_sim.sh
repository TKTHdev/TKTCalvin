#!/bin/bash

# Default Interface
INTERFACE="eth0"

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

# Verification Command
VERIFY_CMD="tc qdisc show dev $INTERFACE"

echo "---------------------------------------------------"
echo "Starting Network Emulation script..."
echo "Mode: $MODE"
echo "Target Interface: $INTERFACE"
if [[ "$MODE" == "add" ]]; then
    echo "Settings: Delay $DELAY +/- $JITTER"
fi
echo "---------------------------------------------------"

# Loop through IPs
while IFS= read -r IP || [ -n "$IP" ]; do
    # Skip empty lines or comments
    [[ "$IP" =~ ^#.*$ ]] || [[ -z "$IP" ]] && continue

    echo "Processing $IP..."

    # 1. Apply the Configuration
    # We use StrictHostKeyChecking=no to avoid hanging on new hosts
    ssh -o StrictHostKeyChecking=no "$IP" "$CMD" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "  [✓] Command applied successfully."
    else
        echo "  [X] Failed to apply command (Machine might be down or rule already exists)."
    fi

    # 2. Verification Step
    echo "  [?] Verifying configuration on remote host..."
    CURRENT_CONFIG=$(ssh -o StrictHostKeyChecking=no "$IP" "$VERIFY_CMD" 2>/dev/null)
    
    if [[ -z "$CURRENT_CONFIG" ]]; then
        echo "      Result: No configuration found (Clean)."
    else
        echo "      Result: $CURRENT_CONFIG"
    fi
    echo "---------------------------------------------------"

done < "$IP_FILE"

echo "Done."