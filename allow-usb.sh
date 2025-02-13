#!/bin/bash

# Get the current user
CURRENT_USER=$(whoami)
RULES_FILE="/etc/udev/rules.d/99-usb-user.rules"

echo "🔍 Detecting current serial devices and their groups..."

# Initialize an empty string to hold rule entries
RULES=""

# Function to check device existence and get its assigned group
get_device_group() {
    DEVICE_PATH=$1
    DEFAULT_GROUP="dialout"  # Fallback if no device is found

    if ls $DEVICE_PATH 1>/dev/null 2>&1; then
        # Device exists, extract its group
        GROUP=$(ls -l $DEVICE_PATH | awk '{print $4}' | head -n 1)
        echo "$GROUP"
    else
        # No device found, return empty string
        echo ""
    fi
}

# Detect devices and their groups
TTYUSB_GROUP=$(get_device_group "/dev/ttyUSB*")
TTYACM_GROUP=$(get_device_group "/dev/ttyACM*")
TTYAMA_GROUP=$(get_device_group "/dev/ttyAMA*")

echo "✅ Detected groups:"
[[ -n "$TTYUSB_GROUP" ]] && echo "   - ttyUSB* : $TTYUSB_GROUP"
[[ -n "$TTYACM_GROUP" ]] && echo "   - ttyACM* : $TTYACM_GROUP"
[[ -n "$TTYAMA_GROUP" ]] && echo "   - ttyAMA* : $TTYAMA_GROUP"

echo "🔧 Setting up udev rules for detected devices..."

# Build rules only for detected devices
if [[ -n "$TTYUSB_GROUP" ]]; then
    RULES+="KERNEL==\"ttyUSB*\", OWNER=\"$CURRENT_USER\", MODE=\"0666\", GROUP=\"$TTYUSB_GROUP\"\n"
fi
if [[ -n "$TTYACM_GROUP" ]]; then
    RULES+="KERNEL==\"ttyACM*\", OWNER=\"$CURRENT_USER\", MODE=\"0666\", GROUP=\"$TTYACM_GROUP\"\n"
fi
if [[ -n "$TTYAMA_GROUP" ]]; then
    RULES+="KERNEL==\"ttyAMA*\", OWNER=\"$CURRENT_USER\", MODE=\"0666\", GROUP=\"$TTYAMA_GROUP\"\n"
fi

# Only update the udev file if we have rules to add
if [[ -n "$RULES" ]]; then
    echo -e "$RULES" | sudo tee "$RULES_FILE" > /dev/null
    echo "✅ Udev rules added to $RULES_FILE"

    # Reload udev rules
    echo "🔄 Reloading udev rules..."
    sudo udevadm control --reload-rules && sudo udevadm trigger

    # Display the created rules
    echo "📜 Verifying rules:"
    cat "$RULES_FILE"

    echo "✅ USB permissions setup complete! You may need to unplug and replug your USB device."
else
    echo "⚠️ No serial devices detected. No udev rules were created."
fi
