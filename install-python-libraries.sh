
#!/bin/bash

# Install libpq-dev (PostgreSQL development libraries)
echo "🔧 Installing libpq-dev (PostgreSQL development libraries)..."
if ! sudo apt-get install -y libpq-dev; then
    echo "❌ Failed to install libpq-dev. Check your package manager or visit https://www.postgresql.org"
    exit 1
fi


echo "🐍 Installing Python dependencies..."
if ! pip install .; then
    echo "❌ Failed to install Python dependencies. Try running 'pip install .' manually."
    exit 1
fi

echo "✅ Installation complete!"
