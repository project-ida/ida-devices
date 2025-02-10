# ida-devices

This project provides diagnostics for various sensors on Raspberry Pi and Ubuntu systems.

## 🚀 Installation

### **Option 1: System-Wide Installation (Recommended)**
This installs everything globally for seamless usage.

1. Clone the repository - `git clone https://github.com/your_repo/ida-devices.git` - in the home directory
2. Copy `psql_credentials_cloud` into the `ida-devices` directory
3. Install python libraries not included the standard python distribution - `pip install .` - inside the `ida-devices` directory

### **Option 2: Using a Virtual Environment (Optional)**
If you prefer to keep dependencies isolated, you can use a virtual environment:

1. Clone the repository - `git clone https://github.com/your_repo/ida-devices.git` - in the home directory
2. Copy `psql_credentials_cloud` into the `ida-devices` directory
3. Create and activate a virtual environment inside of the `ida-devices` directory:
   ```
   python -m venv venv
   source venv/bin/activate  # need to run this for every new terminal
   ```
4. Install python libraries not included the standard python distribution - `pip install .` - inside the `ida-devices` directory
