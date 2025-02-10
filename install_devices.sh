#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"  # Detect the script directory
STARTUP_SCRIPT="$SCRIPT_DIR/start_devices.sh"  # Store startup script in ida-devices directory
CRON_JOB="@reboot $STARTUP_SCRIPT"
CRON_RESTART="*/5 * * * * $STARTUP_SCRIPT"

# Function to list currently installed devices
list_installed_devices() {
    echo -e "\n📡 Currently installed devices:"
    if [ ! -f "$STARTUP_SCRIPT" ]; then
        echo "❌ No devices are configured to start automatically."
        return
    fi
    grep "tmux new-session" "$STARTUP_SCRIPT" | awk -F '"' '{print "✅ " $2}'
}

# Function to update the startup script
update_startup_script() {
    echo "🛠 Updating startup script..."
    echo "#!/bin/bash" > "$STARTUP_SCRIPT"
    for device in "${SELECTED_deviceS[@]}"; do
        SESSION_NAME="${device%.py}"
        echo "tmux has-session -t $SESSION_NAME 2>/dev/null || tmux new-session -d -s $SESSION_NAME 'python $SCRIPT_DIR/$device'" >> "$STARTUP_SCRIPT"
    done
    chmod +x "$STARTUP_SCRIPT"
}

# Ask the user whether to add or remove devices
echo -e "\n🔧 Would you like to: \n1️⃣  Add devices to startup \n2️⃣  Remove devices from startup"
read -p "Enter 1 or 2: " MODE

if [[ "$MODE" == "1" ]]; then
    # Detect available device scripts
    device_SCRIPTS=($(ls "$SCRIPT_DIR"/*.py 2>/dev/null | xargs -n 1 basename))

    if [ ${#device_SCRIPTS[@]} -eq 0 ]; then
        echo "❌ No device scripts found in $SCRIPT_DIR!"
        exit 1
    fi

    # Display available scripts
    echo -e "\n📡 Available device scripts:"
    for i in "${!device_SCRIPTS[@]}"; do
        echo "$((i+1)). ${device_SCRIPTS[$i]}"
    done

    # Get user selection
    read -p "Enter the numbers of the scripts to install (comma-separated): " INPUT
    SELECTED_deviceS=()
    for num in $(echo $INPUT | tr "," " "); do
        if [[ $num =~ ^[0-9]+$ ]] && (( num >= 1 && num <= ${#device_SCRIPTS[@]} )); then
            SELECTED_deviceS+=("${device_SCRIPTS[$((num-1))]}")
        fi
    done

    if [ ${#SELECTED_deviceS[@]} -eq 0 ]; then
        echo "❌ No valid selections made. Exiting."
        exit 1
    fi

    # Confirm selection
    echo -e "\n✔️ Selected devices for startup:"
    for device in "${SELECTED_deviceS[@]}"; do
        echo "✅ $device"
    done

    read -p "Proceed with installation? (y/n): " confirm
    if [[ "$confirm" != "y" ]]; then
        echo "❌ Installation aborted."
        exit 0
    fi

    update_startup_script

    # Set up cron jobs
    echo "⏳ Configuring cron jobs..."
    (crontab -l 2>/dev/null | grep -v "$STARTUP_SCRIPT" ; echo "$CRON_JOB") | crontab -
    (crontab -l 2>/dev/null | grep -v "$STARTUP_SCRIPT" ; echo "$CRON_RESTART") | crontab -

    echo -e "\n✅ Installation complete! Your selected devices will start automatically on reboot and restart if they crash."

elif [[ "$MODE" == "2" ]]; then
    list_installed_devices

    if [ ! -f "$STARTUP_SCRIPT" ]; then
        echo "❌ No devices are currently set to start at boot."
        exit 1
    fi

    # Extract installed devices
    INSTALLED_deviceS=($(grep "tmux new-session" "$STARTUP_SCRIPT" | awk -F "'" '{print $2}'))

    # Ask which ones to remove
    echo -e "\n🗑️  Select devices to REMOVE from startup:"
    for i in "${!INSTALLED_deviceS[@]}"; do
        echo "$((i+1)). ${INSTALLED_deviceS[$i]}"
    done

    read -p "Enter the numbers of the scripts to remove (comma-separated): " REMOVE_INPUT
    REMOVE_deviceS=()
    for num in $(echo $REMOVE_INPUT | tr "," " "); do
        if [[ $num =~ ^[0-9]+$ ]] && (( num >= 1 && num <= ${#INSTALLED_deviceS[@]} )); then
            REMOVE_deviceS+=("${INSTALLED_deviceS[$((num-1))]}")
        fi
    done

    if [ ${#REMOVE_deviceS[@]} -eq 0 ]; then
        echo "❌ No valid selections made. Exiting."
        exit 1
    fi

    # Remove selected devices
    echo -e "\n🚀 Removing selected devices from startup..."
    SELECTED_deviceS=()
    for device in "${INSTALLED_deviceS[@]}"; do
        if [[ ! " ${REMOVE_deviceS[@]} " =~ " ${device} " ]]; then
            SELECTED_deviceS+=("$device")
        fi
    done

    update_startup_script

    # If no devices are left, remove the cron jobs
    if [ ${#SELECTED_deviceS[@]} -eq 0 ]; then
        echo "🛑 No devices left. Removing cron jobs..."
        (crontab -l 2>/dev/null | grep -v "$STARTUP_SCRIPT") | crontab -
        rm -f "$STARTUP_SCRIPT"
    fi

    echo -e "\n✅ Removal complete! The selected devices will no longer start automatically."
else
    echo "❌ Invalid choice. Exiting."
    exit 1
fi

# Display helpful commands
echo -e "\n📊 To check running devices: \n  tmux ls"
echo -e "\n📺 To view a running device output: \n  tmux attach -t <device_name>"
echo -e "\n❌ To exit a device session (without stopping it): \n  Ctrl+B, then D"
