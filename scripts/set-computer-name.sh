#!/bin/bash

# Prompt for input
read -rp "Enter a name to assign to this computer (COMPUTER_NAME): " new_name

# Validate input
if [[ -z "$new_name" ]]; then
  echo "❌ No name entered. Aborting."
  exit 1
fi

# Target file
BASHRC="$HOME/.bashrc"

# Remove any existing COMPUTER_NAME lines
sed -i '' '/^export COMPUTER_NAME=/d' "$BASHRC"

# Ensure the file ends with a newline before appending
tail -c1 "$BASHRC" | read -r _ || echo >> "$BASHRC"

# Append the new export line cleanly
echo "export COMPUTER_NAME=\"$new_name\"" >> "$BASHRC"

# Apply immediately in current session
export COMPUTER_NAME="$new_name"

# Confirm
echo "✅ COMPUTER_NAME set to '$new_name' and written to $BASHRC"
echo "🔁 Run 'source ~/.bashrc' or open a new terminal to make it persistent."
