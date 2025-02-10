#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
STARTUP_SCRIPT="$SCRIPT_DIR/start-devices.sh"

echo -e "\n🔧 What would you like to do?"
echo "1️⃣  View a device's output"
echo "2️⃣  Start a device"
echo "3️⃣  Stop devices"

# Use `read -e` to enable arrow keys & line editing
read -e -p "Enter 1, 2, or 3: " MODE

# Get list of running devices
RUNNING_DEVICES=($(tmux ls 2>/dev/null | awk -F ':' '{print $1}'))

# Get list of installed devices from `start_devices.sh`
INSTALLED_DEVICES=()
if [ -f "$STARTUP_SCRIPT" ]; then
    INSTALLED_DEVICES=($(grep "tmux new-session" "$STARTUP_SCRIPT" | awk -F "'" '{print $2}' | awk '$1 == "python" {print $2}' | xargs -n 1 basename))
fi

if [[ "$MODE" == "1" || "$MODE" == "3" ]]; then
    if [ ${#RUNNING_DEVICES[@]} -eq 0 ]; then
        echo "❌ No devices are currently running."
        exit 1
    fi

    echo -e "\n📡 Running devices:"
    for i in "${!RUNNING_DEVICES[@]}"; do
        echo "$((i+1)). ${RUNNING_DEVICES[$i]}"
    done

    # Enable arrow key navigation when selecting a device
    read -e -p "Enter the numbers of the devices to select (comma-separated): " DEVICE_INPUT

    # Process multiple device selections
    SELECTED_DEVICES=()
    for num in $(echo "$DEVICE_INPUT" | tr "," " "); do
        if [[ "$num" =~ ^[0-9]+$ ]] && (( num >= 1 && num <= ${#RUNNING_DEVICES[@]} )); then
            SELECTED_DEVICES+=("${RUNNING_DEVICES[$((num-1))]}")
        fi
    done

    if [ ${#SELECTED_DEVICES[@]} -eq 0 ]; then
        echo "❌ No valid selections made. Exiting."
        exit 1
    fi
fi

if [[ "$MODE" == "1" ]]; then
    tmux attach -t "${SELECTED_DEVICES[0]}"  # Only allow selecting one for viewing

elif [[ "$MODE" == "2" ]]; then
    if [ ${#INSTALLED_DEVICES[@]} -eq 0 ]; then
        echo "❌ No installed devices found in $STARTUP_SCRIPT!"
        exit 1
    fi

    echo -e "\n🚀 Select one or more devices to start (comma-separated):"
    for i in "${!INSTALLED_DEVICES[@]}"; do
        echo "$((i+1)). ${INSTALLED_DEVICES[$i]}"
    done

    read -e -p "Enter the numbers of the devices to start (comma-separated): " DEVICE_INPUT

    # Process multiple device selections
    SELECTED_DEVICES=()
    for num in $(echo "$DEVICE_INPUT" | tr "," " "); do
        if [[ "$num" =~ ^[0-9]+$ ]] && (( num >= 1 && num <= ${#INSTALLED_DEVICES[@]} )); then
            SELECTED_DEVICES+=("${INSTALLED_DEVICES[$((num-1))]}")
        fi
    done

    if [ ${#SELECTED_DEVICES[@]} -eq 0 ]; then
        echo "❌ No valid selections made. Exiting."
        exit 1
    fi

    # Start selected devices
    for DEVICE_NAME in "${SELECTED_DEVICES[@]}"; do
        DEVICE_CMD=$(grep "$DEVICE_NAME" "$STARTUP_SCRIPT")

        if [ -n "$DEVICE_CMD" ]; then
            echo "🚀 Starting $DEVICE_NAME..."
            eval "$DEVICE_CMD"
            echo "✅ $DEVICE_NAME started."
        else
            echo "❌ Failed to find a valid command for $DEVICE_NAME."
        fi
    done

elif [[ "$MODE" == "3" ]]; then
    echo -e "\n🛑 Stopping selected devices..."

    # Stop each selected device
    for DEVICE_NAME in "${SELECTED_DEVICES[@]}"; do
        echo "🛑 Stopping $DEVICE_NAME..."
        tmux kill-session -t "$DEVICE_NAME"
        echo "✅ $DEVICE_NAME has been stopped."
    done
fi
