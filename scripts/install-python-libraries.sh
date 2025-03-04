
#!/bin/bash

# Install libpq-dev (PostgreSQL development libraries)
echo "🔧 Installing libpq-dev (PostgreSQL development libraries)..."
if ! sudo apt-get install -y libpq-dev; then
    echo "❌ Failed to install libpq-dev. Check your package manager or visit https://www.postgresql.org"
    exit 1
fi

echo "⚙️ Attempting to configure pip to allow breaking system packages..."
# Try to set the config, but don't exit on failure
pip config set global.break-system-packages true 2>/dev/null || true
# If it fails (e.g., option not supported), we proceed anyway

echo "🐍 Installing Python dependencies..."
if ! pip install .; then
    echo "❌ Failed to install Python dependencies. Try running 'pip install .' manually."
    exit 1
fi

echo "✅ Installation complete!"
