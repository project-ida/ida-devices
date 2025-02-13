#!/bin/bash

# Store the current directory
ORIGINAL_DIR="$(pwd)"

# Change to the user's home directory
cd "$HOME" || { echo "❌ Failed to change to home directory"; exit 1; }

echo "🚀 Updating package lists..."
sudo apt update

echo "🔧 Installing required packages..."
sudo apt-get install -y gcc g++ make libusb-1.0-0-dev

echo "⬇️ Downloading MCCDAQ libuldaq..."
wget -N https://github.com/mccdaq/uldaq/releases/download/v1.2.1/libuldaq-1.2.1.tar.bz2

echo "📦 Extracting libuldaq..."
tar -xvjf libuldaq-1.2.1.tar.bz2

echo "⚙️ Compiling and installing libuldaq..."
cd libuldaq-1.2.1 || { echo "❌ Failed to enter libuldaq directory"; exit 1; }
./configure && make
sudo make install

echo "🧹 Cleaning up..."
cd "$HOME"
rm -rf libuldaq-1.2.1 libuldaq-1.2.1.tar.bz2

echo "📥 Cloning daqhats repository..."
git clone https://github.com/mccdaq/daqhats.git

echo "📦 Installing daqhats..."
cd "$HOME/daqhats" || { echo "❌ Failed to enter daqhats directory"; exit 1; }
sudo ./install.sh

# Return to the original directory
cd "$ORIGINAL_DIR" || { echo "❌ Failed to return to original directory"; exit 1; }

echo "🐍 Installing Python dependencies..."
pip install .

echo "✅ Installation complete!"
