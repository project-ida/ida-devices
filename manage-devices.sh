#!/bin/bash

echo -e "\n🔧 What would you like to do?"
echo "1️⃣  View a device's output"
echo "2️⃣  Stop a running device"

# Use `read -e` to enable arrow keys & line editing
read -e -p "Enter 1 or 2: " MODE

# Get list of running devices
RUNNING_DEVICES=($(tmux ls 2>/dev/null | awk -F ':' '{print $1}'))

if [[ "$MODE" == "1" || "$MODE" == "2" ]]; then
    if [ ${#RUNNING_DEVICES[@]} -eq 0 ]; then
        echo "❌ No devices are currently running."
        exit 1
    fi

    echo -e "\n📺 After selecting a device, we'll attach to it via tmux."
    echo "Press Ctrl+B, then D to detach. Alternatively, type 'tmux detach' and hit enter."
    echo -e "\n📡 Running devices:"
    for i in "${!RUNNING_DEVICES[@]}"; do
        echo "$((i+1)). ${RUNNING_DEVICES[$i]}"
    done

    # Enable arrow key navigation when selecting a device
    read -e -p "Enter the number of the device to select: " DEVICE_NUM

    if [[ "$DEVICE_NUM" =~ ^[0-9]+$ ]] && (( DEVICE_NUM >= 1 && DEVICE_NUM <= ${#RUNNING_DEVICES[@]} )); then
        DEVICE_NAME="${RUNNING_DEVICES[$((DEVICE_NUM-1))]}"
    else
        echo "❌ Invalid selection."
        exit 1
    fi
fi

if [[ "$MODE" == "1" ]]; then
    tmux attach -t "$DEVICE_NAME"

elif [[ "$MODE" == "2" ]]; then
    echo "🛑 Stopping $DEVICE_NAME..."
    tmux kill-session -t "$DEVICE_NAME"
    echo "✅ $DEVICE_NAME has been stopped."
fi
