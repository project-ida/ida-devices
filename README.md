# ida-devices

This project provides diagnostics for various sensors on Raspberry Pi and Ubuntu systems.

❗️FOR RASPBERRY PI USING DAQ HAT HARDWARE❗️

You'll need to set up the hardware before installing anything from this library.

Copied from [daqhats](https://github.com/mccdaq/daqhats):

> Power off the Raspberry Pi and attach one or more DAQ HAT boards, using unique address settings for each. Refer to [Installing the HAT board](https://mccdaq.github.io/daqhats/hardware.html) for detailed information. When using a single board, leave it at address 0 (all address jumpers removed.) One board must always be at address 0 to ensure that the OS reads a HAT EEPROM and initializes the hardware correctly.

## 🚀 Installing software

1. Clone the repository

   ```sh
   git clone https://github.com/project-ida/ida-devices.git
   ```

   in the home directory (usually the directory that the terminal starts in - to get there type `cd ~`).

2. For Raspberry Pi with daqhat hardware, install data acquisition drivers from digilent ([uldaq](https://github.com/mccdaq/uldaq), [daqhats](https://github.com/mccdaq/daqhats)) by running

   ```sh
   bash scripts/install-digilent-software.sh
   ```

   inside the `ida-devices` directory.

2. Install python software libraries by running

   ```sh
   bash scripts/install-python-libraries.sh
   ```

   inside the `ida-devices` directory.

3. Allow all USB ports to be accessed by the current user by running

   ```sh
   bash scripts/allow-usb.sh
   ```

   inside the `ida-devices` directory.

4. Set the computer name by running

   ```sh
   bash scripts/set-computer-name.sh
   ```

   inside the `ida-devices` directory.

5. Copy `psql_credentials` into the `ida-devices` directory.

6. Copy `telegram_credentials` into the `ida-devices` directory.


## 🧑‍💻 Running software

If you don't want to have scripts automatically re-start if a crash happens, then you are free to simply open a terminal and run

```sh
python whatever.py
```

If, however, you want something a bit more resilient, then you'll need to read onto the "Installing Devices" section.

## 🔧 Installing Devices

"Installing" a device means:

- Allowing it to run at startup and re-run if it crashes (via cron)
- Making it easier to start/top/view a device when running `manage-devices.sh`

We use the `install-devices.sh` script for this.

### **Adding Devices**

```sh
bash scripts/install-devices.sh
```

1. Select **"Add devices to startup"** from the menu.
2. Choose the devices from the numbered list.

### **Removing Devices**

```sh
bash scripts/install-devices.sh
```

1. Select **"Remove devices from startup"** from the menu.
2. Choose the devices you wish to remove from the numbered list.

## 📟 Manage Devices

Once device installation is complete, you can use the `manage-devices.sh` script to start/stop/view devices.

### **Viewing Device Output**

To view the real-time output of a running device, run:

```sh
bash scripts/manage-devices.sh
```

1. Select **"View a device's output"** from the menu.
2. Choose the device from the numbered list.
3. The terminal will attach to the device’s `tmux` session, showing live output.
4. To **detach** (without stopping the device), press `Ctrl+B`, then `D` (or type `tmux detach` and enter).

### **Starting a Device**

To start a device:

```sh
bash scripts/manage-devices.sh
```

1. Select **"Start a device"** from the menu.
2. Choose the devices from the numbered list.
3. The device will be started as a background process using the command installed in `scripts/start-devices.sh`

### **Stopping a Running Device**

To stop a device that is currently running:

```sh
bash scripts/manage-devices.sh
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
tmux new-session -d -s <device-name> "python /path/to/device-script.py; echo Press Enter to exit...; read"
```

This will run the selected device in the background using `tmux`.
