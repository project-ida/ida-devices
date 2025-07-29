# Copilot Instructions for ida-devices

## Project Overview
- **Purpose:** This repository provides diagnostics, data acquisition, and device management for various sensors and DAQ hardware (e.g., Digilent MCC, CAEN, Amptek, Ortec) on Raspberry Pi and Ubuntu systems.
- **Architecture:**
  - Each device or data source has a dedicated Python script at the repo root (e.g., `digilent-mcc118-pressure.py`, `caen-watch_acq_times.py`).
  - Shared logic is in `libs/` (e.g., Google Sheets, GPIB, Telegram, settings validation).
  - Device management and installation are handled by shell scripts in `scripts/`.
  - Data post-processing and database sync are handled by scripts like `caen-rootpostprocessing.py`.

## Key Conventions & Patterns
- **Device Scripts:**
  - Each device script is standalone, with its own `main()` and CLI argument parsing.
  - Logging is set up per-script, often to both file and console.
  - Device scripts may use `tmux` for background execution (see `manage-devices.sh`).
- **Configuration:**
  - Device and DAQ settings are stored in XML (`settings.xml`) and referenced in run folders.
  - Reference configs are in a `CONFIG` subfolder.
  - Environment variables (e.g., `COMPUTER_NAME`) are used for host identification.
- **Database & Sheets:**
  - PostgreSQL credentials must be provided in `psql_credentials.py` (not checked in).
  - Google Sheets integration is via `libs/google_sheet_utils.py`.
- **Device Management:**
  - Use `scripts/install-devices.sh` to add/remove devices from startup (via cron).
  - Use `scripts/manage-devices.sh` to start/stop/view device processes (uses `tmux`).
  - Device output is viewed via `tmux` sessions; detach with `Ctrl+B`, then `D`.
- **USB & Permissions:**
  - Run `scripts/allow-usb.sh` to set up udev rules for device access.

## Developer Workflows
- **Install dependencies:**
  - `bash scripts/install-python-libraries.sh`
  - `bash scripts/install-digilent-software.sh` (for DAQ HAT hardware)
- **Set up environment:**
  - `bash scripts/set-computer-name.sh`
  - Copy `psql_credentials.py` and `telegram_credentials` into the repo root.
- **Run a device script:**
  - `python <device-script>.py` (for manual testing)
  - Or use `bash scripts/manage-devices.sh` for managed background execution.
- **Post-processing:**
  - `python caen-rootpostprocessing.py` (prompts for folder, channel, etc.)
- **Testing/Debugging:**
  - No formal test suite; manual/interactive runs are standard.

## Integration Points
- **Google Sheets:** Used for run/event logging (see `libs/google_sheet_utils.py`).
- **PostgreSQL:** Used for event and file metadata storage (see `caen-rootpostprocessing.py`).
- **Telegram:** Optional notifications via `libs/telegram_notifier.py`.

## Notable Patterns
- **Prompt-driven CLI:** Many scripts prompt for missing arguments interactively.
- **File/Folder Naming:** Run folders are named after the run; end files are `<run_name>.txt`.
- **Initial Scans:** Scripts like `caen-watch_acq_times.py` perform an initial scan and then monitor for new events.
- **Settings Diff:** Parameter diffs are reported using `libs/settings_validator.py`.

## Examples
- To add a new device, create a new script at the repo root, use patterns from existing device scripts, and add to `install-devices.sh` if needed.
- To process CAEN data, follow the workflow in `caen-rootpostprocessing.py` and ensure all required credentials/configs are present.

---

For more details, see `README.md` and scripts in `scripts/` and `libs/`.
