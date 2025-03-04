
#!/bin/bash

# Install libpq-dev (PostgreSQL development libraries)
echo "🔧 Installing libpq-dev (PostgreSQL development libraries)..."
if ! sudo apt-get install -y libpq-dev; then
    echo "❌ Failed to install libpq-dev. Check your package manager or visit https://www.postgresql.org"
    exit 1
fi

# Install Qt5 dependencies (libxcb-xinerama0, libxcb1)
echo "🔧 Installing Qt5 dependencies (libxcb-xinerama0, libxcb1)..."
if ! sudo apt-get install -y libxcb-xinerama0 libxcb1; then
    echo "❌ Failed to install Qt5 dependencies. Check your package manager or visit https://doc.qt.io/qt-5/linux-requirements.html"
    exit 1
fi

echo "⚙️ Attempting to configure pip to allow breaking system packages..."
if pip config set global.break-system-packages true 2>/dev/null; then
    echo "✅ Pip config set successfully."
else
    echo "⚠️ Pip config set failed (possibly old version or permissions), proceeding anyway..."
fi

echo "🐍 Installing Python dependencies..."
if ! pip install .; then
    echo "❌ Failed to install Python dependencies. Try running 'pip install .' manually."
    exit 1
fi

echo "✅ Installation complete!"
