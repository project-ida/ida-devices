# ida-devices

This project provides diagnostics for various sensors on Raspberry Pi and Ubuntu systems.

## 🚀 Installing code

### **Option 1: System-Wide Installation (Recommended)**

This installs everything globally for seamless usage.

1. Clone the repository -

   ```sh
   git clone https://github.com/your_repo/ida-devices.git
   ```

   in the home directory (usually the directory that the terminal starts in - denoted by `~`).

2. Copy `psql_credentials_cloud` into the `ida-devices` directory.

3. Install Python libraries not included in the standard Python distribution -
   ```sh
   pip install .
   ```
   inside the `ida-devices` directory.

### **Option 2: Using a Virtual Environment (Optional)**

If you prefer to keep dependencies isolated, you can use a virtual environment:

1. Clone the repository -

   ```sh
   git clone https://github.com/your_repo/ida-devices.git
   ```

   in the home directory (usually the directory that the terminal starts in - denoted by `~`).

2. Copy `psql_credentials_cloud` into the `ida-devices` directory.

3. Create and activate a virtual environment inside the `ida-devices` directory:

   ```sh
   python -m venv venv
   source venv/bin/activate  # need to run this for every new terminal
   ```

4. Install Python libraries not included in the standard Python distribution -
   ```sh
   pip install .
   ```
   inside the `ida-devices` directory.

## 🔧 Installing Devices

"Installing" a device means:

- Allowing it to run at startup and re-run if it crashes (via cron)
- Making it easier to start/top/view a device when running `manage_devices.sh`

We use the `install_devices.sh` script for this.

### **Adding Devices**

```sh
bash install_devices.sh
```

1. Select **"Add devices to startup"** from the menu.
2. Choose the devices from the numbered list.

### **Removing Devices**

To remove a device from automatic startup:

```sh
bash install_devices.sh
```

1. Select **"Remove devices from startup"** from the menu.
2. Choose the devices you wish to remove from the numbered list.

## 📟 Manage Devices

Once device installation is complete, you can use the `manage_devices.sh` script to start/stop/view devices.

### **Viewing Device Output**

To view the real-time output of a running device, run:

```sh
bash manage_devices.sh
```

1. Select **"View a device's output"** from the menu.
2. Choose the device from the numbered list.
3. The terminal will attach to the device’s `tmux` session, showing live output.
4. To **detach** (without stopping the device), press `Ctrl+B`, then `D` (or type `tmux detach` and enter).

### **Starting a Device**

To start a device:

```sh
bash manage_devices.sh
```

1. Select **"Start a device"** from the menu.
2. Choose the devices from the numbered list.
3. The device will be started as a background process using the command installed in `start-devices.sh`

### **Stopping a Running Device**

To stop a device that is currently running:

```sh
bash manage_devices.sh
```

1. Select **"Stop a running device"** from the menu.
2. Choose the device from the numbered list.
3. The selected device will be stopped immediately.

## Manual actions

### **Checking Running Devices**

To check which devices are currently running, use:

```sh
tmux ls
```

This will list all active `tmux` sessions, which correspond to running devices.

### **Starting Devices**

If you want to manually start a device without adding it to startup:

```sh
tmux new-session -d -s <device_name> "python /path/to/device_script.py"
```

This will run the selected device in the background using `tmux`.
