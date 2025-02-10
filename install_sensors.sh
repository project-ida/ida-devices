#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"  # Detect the script directory
STARTUP_SCRIPT="$SCRIPT_DIR/start_sensors.sh"  # Store startup script in ida-devices directory
CRON_JOB="@reboot $STARTUP_SCRIPT"
CRON_RESTART="*/5 * * * * $STARTUP_SCRIPT"

# Function to list currently installed sensors
list_installed_sensors() {
    echo -e "\n📡 Currently installed sensors:"
    if [ ! -f "$STARTUP_SCRIPT" ]; then
        echo "❌ No sensors are configured to start automatically."
        return
    fi
    grep "tmux new-session" "$STARTUP_SCRIPT" | awk -F '"' '{print "✅ " $2}'
}

# Function to update the startup script
update_startup_script() {
    echo "🛠 Updating startup script..."
    echo "#!/bin/bash" > "$STARTUP_SCRIPT"
    for sensor in "${SELECTED_SENSORS[@]}"; do
        SESSION_NAME="${sensor%.py}"
        echo "tmux has-session -t $SESSION_NAME 2>/dev/null || tmux new-session -d -s $SESSION_NAME 'python $SCRIPT_DIR/$sensor'" >> "$STARTUP_SCRIPT"
    done
    chmod +x "$STARTUP_SCRIPT"
}

# Ask the user whether to add or remove sensors
echo -e "\n🔧 Would you like to: \n1️⃣ Add sensors to startup \n2️⃣ Remove sensors from startup"
read -p "Enter 1 or 2: " MODE

if [[ "$MODE" == "1" ]]; then
    # Detect available sensor scripts
    SENSOR_SCRIPTS=($(ls "$SCRIPT_DIR"/*.py 2>/dev/null | xargs -n 1 basename))

    if [ ${#SENSOR_SCRIPTS[@]} -eq 0 ]; then
        echo "❌ No sensor scripts found in $SCRIPT_DIR!"
        exit 1
    fi

    # Display available scripts
    echo -e "\n📡 Available sensor scripts:"
    for i in "${!SENSOR_SCRIPTS[@]}"; do
        echo "$((i+1)). ${SENSOR_SCRIPTS[$i]}"
    done

    # Get user selection
    read -p "Enter the numbers of the scripts to install (comma-separated): " INPUT
    SELECTED_SENSORS=()
    for num in $(echo $INPUT | tr "," " "); do
        if [[ $num =~ ^[0-9]+$ ]] && (( num >= 1 && num <= ${#SENSOR_SCRIPTS[@]} )); then
            SELECTED_SENSORS+=("${SENSOR_SCRIPTS[$((num-1))]}")
        fi
    done

    if [ ${#SELECTED_SENSORS[@]} -eq 0 ]; then
        echo "❌ No valid selections made. Exiting."
        exit 1
    fi

    # Confirm selection
    echo -e "\n✔️ Selected sensors for startup:"
    for sensor in "${SELECTED_SENSORS[@]}"; do
        echo "✅ $sensor"
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

    echo -e "\n✅ Installation complete! Your selected sensors will start automatically on reboot and restart if they crash."

elif [[ "$MODE" == "2" ]]; then
    list_installed_sensors

    if [ ! -f "$STARTUP_SCRIPT" ]; then
        echo "❌ No sensors are currently set to start at boot."
        exit 1
    fi

    # Extract installed sensors
    INSTALLED_SENSORS=($(grep "tmux new-session" "$STARTUP_SCRIPT" | awk -F "'" '{print $2}'))

    # Ask which ones to remove
    echo -e "\n🗑️ Select sensors to REMOVE from startup:"
    for i in "${!INSTALLED_SENSORS[@]}"; do
        echo "$((i+1)). ${INSTALLED_SENSORS[$i]}"
    done

    read -p "Enter the numbers of the scripts to remove (comma-separated): " REMOVE_INPUT
    REMOVE_SENSORS=()
    for num in $(echo $REMOVE_INPUT | tr "," " "); do
        if [[ $num =~ ^[0-9]+$ ]] && (( num >= 1 && num <= ${#INSTALLED_SENSORS[@]} )); then
            REMOVE_SENSORS+=("${INSTALLED_SENSORS[$((num-1))]}")
        fi
    done

    if [ ${#REMOVE_SENSORS[@]} -eq 0 ]; then
        echo "❌ No valid selections made. Exiting."
        exit 1
    fi

    # Remove selected sensors
    echo -e "\n🚀 Removing selected sensors from startup..."
    SELECTED_SENSORS=()
    for sensor in "${INSTALLED_SENSORS[@]}"; do
        if [[ ! " ${REMOVE_SENSORS[@]} " =~ " ${sensor} " ]]; then
            SELECTED_SENSORS+=("$sensor")
        fi
    done

    update_startup_script

    # If no sensors are left, remove the cron jobs
    if [ ${#SELECTED_SENSORS[@]} -eq 0 ]; then
        echo "🛑 No sensors left. Removing cron jobs..."
        (crontab -l 2>/dev/null | grep -v "$STARTUP_SCRIPT") | crontab -
        rm -f "$STARTUP_SCRIPT"
    fi

    echo -e "\n✅ Removal complete! The selected sensors will no longer start automatically."
else
    echo "❌ Invalid choice. Exiting."
    exit 1
fi

# Display helpful commands
echo -e "\n📊 To check running sensors: \n  tmux ls"
echo -e "\n📺 To view a running sensor output: \n  tmux attach -t <sensor_name>"
echo -e "\n❌ To exit a sensor session (without stopping it): \n  Ctrl+B, then D"
