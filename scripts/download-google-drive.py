"""
Interactive Google Drive Folder Downloader
==========================================

This script uses rclone and a curses-based menu to let users browse and
download folders from their Google Drive (specifically from the 'Computers' section).
It supports recursive navigation, allowing you to explore folders at any depth and
choose to download any folder or continue navigating.

📦 Dependencies:
- Python 3
- rclone (https://rclone.org)

✅ Setup Instructions (one-time):

1. **Install rclone** (if not already installed):
   $ sudo apt install rclone         # on Ubuntu/Debian
   or
   $ curl https://rclone.org/install.sh | sudo bash

2. **Configure Google Drive remote:**
   Run the following command and follow the prompts to link your Google account:
   $ rclone config

   - Choose "n" for a new remote
   - Name it: `googledrive` (this script expects this exact name)
   - Choose storage type: `drive` (Google Drive)
   - Follow the steps to authenticate using your Google account
   - For "root_folder_id" and "team_drive" just press Enter (use defaults)

3. **Test your rclone remote:**
   Confirm your configuration is working:
   $ rclone lsd googledrive:Computers

4. **Run the script:**
   $ python3 gdrive_recursive_downloader.py

This will open an interactive menu to:
- Select a computer backup folder
- Navigate recursively through subfolders
- Choose to download the current folder or continue to a subfolder
- Go back to the parent folder or return to computer selection
- Download the selected folder to ~/GoogleDrive/Computers/<computer>/<path>

"""

import curses
import os
import subprocess
import sys
from pathlib import Path
import unicodedata
import re

def show_waiting_message(stdscr, message="Please wait..."):
    stdscr.clear()
    max_y, max_x = stdscr.getmaxyx()
    safe_addstr(stdscr, max_y // 2, max_x // 2 - len(message) // 2, message, curses.A_BOLD)
    stdscr.refresh()

def run_command(command):
    """Run a shell command and return its output."""
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        return result.stdout.strip().split('\n')
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        sys.exit(1)

def ensure_gdrive_dir():
    """Ensure ~/GoogleDrive directory exists."""
    gdrive_path = Path.home() / "GoogleDrive"
    gdrive_path.mkdir(exist_ok=True)
    return gdrive_path

def list_folders(remote_path):
    """List folders in the given remote path using robust parsing."""
    command = f"rclone lsd \"{remote_path}\""
    folders = run_command(command)
    folder_names = []
    for line in folders:
        match = re.match(r"\s*-?\d+\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+-?\d+\s+(.*)", line)
        if match:
            folder_names.append(match.group(1).strip())
    return sorted(set(folder_names))

def sanitize_string(text):
    """Sanitize string to remove problematic characters and normalize."""
    # Normalize Unicode and convert to ASCII
    normalized = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    # Replace non-alphanumeric characters (except underscores and hyphens) with underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', normalized)
    return sanitized if sanitized.strip() else "Unprintable"

def safe_addstr(stdscr, y, x, text, attr=0):
    """Safely add a string to the curses screen."""
    try:
        safe_text = sanitize_string(text)
        stdscr.addstr(y, x, safe_text, attr)
    except curses.error as e:
        stdscr.addstr(y, x, "[Error: Unprintable]", attr)
        with open("curses_error.log", "a") as f:
            f.write(f"Error displaying '{text}' at ({y}, {x}): {e}\n")

def select_option(stdscr, options, prompt, original_options=None, show_download_option=False):
    """Display a scrollable menu using curses with optional download action."""
    if not options and not show_download_option:
        return None, False
    curses.curs_set(0)
    current_row = 0
    while True:
        stdscr.clear()
        max_y, max_x = stdscr.getmaxyx()
        max_display = max_y - 5  # 1 for prompt, 1 for help, 3 margin
        safe_addstr(stdscr, 0, 0, sanitize_string(prompt), curses.A_BOLD)
        display_options = options.copy()
        if show_download_option:
            display_options.insert(0, "Download this folder")
        if len(options) > 0 and options[0] != "..":
            display_options.insert(0, ".. (Go up)")
        start_idx = max(0, current_row - max_display + 1) if current_row >= max_display else 0
        end_idx = min(len(display_options), start_idx + max_display)
        visible_options = display_options[start_idx:end_idx]
        for i, option in enumerate(visible_options):
            actual_idx = start_idx + i
            prefix = "> " if actual_idx == current_row else "  "
            safe_addstr(stdscr, i + 2, 0, f"{prefix}{option}",
                        curses.A_REVERSE if actual_idx == current_row else 0)
        help_line = f"Use ↑ ↓ to scroll, Enter to select ({current_row + 1}/{len(display_options)})"
        safe_addstr(stdscr, max_display + 3, 0, help_line[:max_x])
        stdscr.refresh()
        key = stdscr.getch()
        if key == curses.KEY_UP and current_row > 0:
            current_row -= 1
        elif key == curses.KEY_DOWN and current_row < len(display_options) - 1:
            current_row += 1
        elif key == 10:  # Enter
            selected = display_options[current_row]
            if selected == "Download this folder":
                return None, True
            elif selected == ".. (Go up)":
                return "..", False
            # Adjust index to account for special entries
            adjusted_idx = current_row - (2 if show_download_option and len(options) > 0 else 1 if len(options) > 0 else 0)
            if adjusted_idx < 0 or adjusted_idx >= len(options):
                return None, False
            return (original_options[options[adjusted_idx]] if original_options else options[adjusted_idx]), False

def copy_folder(remote_path, local_path):
    """Copy the selected folder recursively to ~/GoogleDrive."""
    command = f"rclone copy \"{remote_path}\" \"{local_path}\" --progress"
    print(f"\nCopying from {remote_path} to {local_path}...\n")
    try:
        subprocess.run(command, shell=True, check=True)
        print("\n✅ Copy completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error during copy: {e}")
    finally:
        os.system("stty sane")
        input("\nPress Enter to exit...")
        sys.exit(0)

def main(stdscr):
    # Check terminal capabilities
    try:
        stdscr.clear()
        stdscr.refresh()
        curses.use_default_colors()
    except curses.error as e:
        print(f"Terminal initialization error: {e}")
        sys.exit(1)

    # Log start for debugging
    with open("curses_error.log", "a") as f:
        f.write("Starting curses session\n")

    # Ensure ~/GoogleDrive exists
    local_gdrive = ensure_gdrive_dir()

    # List computer folders
    show_waiting_message(stdscr, "Fetching computer list from Google Drive...")
    computers = list_folders("googledrive:Computers")
    if not computers:
        print("No computer folders found in googledrive:Computers.")
        sys.exit(1)

    # Log computer names
    with open("curses_error.log", "a") as f:
        f.write(f"Computers: {computers}\n")

    # Sanitize computer names for display, keep original for rclone
    computer_map = {sanitize_string(c): c for c in computers}
    sanitized_computers = list(computer_map.keys())

    # Let user select a computer
    selected_sanitized_computer, _ = select_option(stdscr, sanitized_computers, "Select a computer:", computer_map)
    selected_computer = computer_map.get(selected_sanitized_computer, selected_sanitized_computer)

    # Log selected computer
    with open("curses_error.log", "a") as f:
        f.write(f"Selected computer: {selected_computer}\n")

    # Initialize path for recursive navigation
    current_path = f"googledrive:Computers/{selected_computer}"
    local_base_path = local_gdrive / "Computers" / selected_computer
    path_stack = [selected_computer]

    # Main navigation loop
    while True:
        # List subfolders in the current path
        show_waiting_message(stdscr, f"Listing folders in {current_path}...")
        subfolders = list_folders(current_path)

        # Log subfolders
        with open("curses_error.log", "a") as f:
            f.write(f"Subfolders for {current_path}: {subfolders}\n")

        # Sanitize subfolder names for display, keep original for rclone
        subfolder_map = {sanitize_string(s): s for s in subfolders}
        sanitized_subfolders = list(subfolder_map.keys())

        # Let user select a subfolder or action
        prompt = f"Current path: {'/'.join(path_stack)}"
        selected, download = select_option(stdscr, sanitized_subfolders, prompt, subfolder_map, show_download_option=True)

        if download:
            # Download the current folder
            local_path = local_base_path / Path('/'.join(path_stack[1:]))
            local_path.parent.mkdir(parents=True, exist_ok=True)
            copy_folder(current_path, local_path)
        elif selected == "..":
            # Go up one level or back to computer selection
            if len(path_stack) > 1:
                path_stack.pop()
                current_path = "googledrive:Computers/" + "/".join(path_stack[1:])
            else:
                selected_sanitized_computer, _ = select_option(stdscr, sanitized_computers, "Select a computer:", computer_map)
                selected_computer = computer_map.get(selected_sanitized_computer, selected_sanitized_computer)
                current_path = f"googledrive:Computers/{selected_computer}"
                local_base_path = local_gdrive / "Computers" / selected_computer
                path_stack = [selected_computer]
        elif selected:
            # Navigate into selected subfolder
            path_stack.append(selected)
            current_path = f"{current_path}/{selected}"

if __name__ == "__main__":
    try:
        curses.wrapper(main)
    except curses.error as e:
        print(f"Curses error: {e}")
        sys.exit(1)