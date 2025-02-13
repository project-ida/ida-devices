#!/bin/bash

# Store the current directory
ORIGINAL_DIR="$(pwd)"

# Change to the user's home directory
cd "$HOME" || { echo "❌ Failed to change to home directory"; exit 1; }

echo "🚀 Updating package lists..."
if ! sudo apt update; then
    echo "❌ Failed to update package lists. Check your internet connection or package manager settings."
    exit 1
fi

echo "🔧 Installing required packages..."
if ! sudo apt-get install -y gcc g++ make libusb-1.0-0-dev; then
    echo "❌ Failed to install required system packages."
    exit 1
fi

echo "⬇️ Downloading MCCDAQ libuldaq..."
if ! wget -N https://github.com/mccdaq/uldaq/releases/download/v1.2.1/libuldaq-1.2.1.tar.bz2; then
    echo "❌ Failed to download libuldaq. Please check https://github.com/mccdaq/uldaq for help."
    exit 1
fi

echo "📦 Extracting libuldaq..."
if ! tar -xvjf libuldaq-1.2.1.tar.bz2; then
    echo "❌ Failed to extract libuldaq. Try re-downloading it. See: https://github.com/mccdaq/uldaq"
    exit 1
fi

echo "⚙️ Compiling and installing libuldaq..."
cd libuldaq-1.2.1 || { echo "❌ Failed to enter libuldaq directory. Check: https://github.com/mccdaq/uldaq"; exit 1; }

if ! ./configure && make; then
    echo "❌ Build failed for libuldaq. Check: https://github.com/mccdaq/uldaq"
    exit 1
fi

if ! sudo make install; then
    echo "❌ Installation failed for libuldaq. Check: https://github.com/mccdaq/uldaq"
    exit 1
fi

echo "📥 Cloning daqhats repository..."
if ! git clone https://github.com/mccdaq/daqhats.git; then
    echo "❌ Failed to clone daqhats repository. Check: https://github.com/mccdaq/daqhats"
    exit 1
fi

echo "📦 Installing daqhats..."
cd "$HOME/daqhats" || { echo "❌ Failed to enter daqhats directory. Check: https://github.com/mccdaq/daqhats"; exit 1; }

if ! sudo ./install.sh; then
    echo "❌ Failed to install daqhats. Check: https://github.com/mccdaq/daqhats/issues"
    exit 1
fi

# Return to the original directory
cd "$ORIGINAL_DIR" || { echo "❌ Failed to return to original directory"; exit 1; }

echo "🐍 Installing Python dependencies..."
if ! pip install .; then
    echo "❌ Failed to install Python dependencies. Try running 'pip install .' manually."
    exit 1
fi

echo "✅ Installation complete!"
