/*
 * Purpose:
 * This script prevents Google Drive file system stalls in Google Colab by periodically
 * refreshing the file explorer and simulates user activity to keep the runtime active.
 * It is designed to run in the browser's Developer Tools console, independent of Colab's
 * Python kernel, ensuring it doesn't block your main Python script (e.g., for processing ROOT files).
 *
 * Usage Instructions:
 * 0. Make sure the Colab File explorer panel is open on the left. You must be able to see a little refresh icon.
 * 1. Open Chrome Developer Tools:
 *    - In your Colab notebook, right-click anywhere and select "Inspect", or press F12 (Windows/Linux)
 *      or Cmd+Option+I (Mac).
 *    - Go to the "Console" tab.
 * 2. Paste and Run:
 *    - Copy this entire script and paste it into the Console.
 *    - Press Enter to start. You'll see logs like:
 *      "File system refresher started with interval 300 seconds"
 *      "Refreshing file system at [timestamp]"
 *      "File system refresh triggered"
 *      "Runtime button clicked for activity"
 *    - The script runs every 5 minutes (300 seconds) to refresh the file system and simulate activity.
 * 3. Run Your Python Script:
 *    - Execute your main Python script (e.g., caen-rootpostprocessing.py) in a Colab cell.
 *    - The JavaScript will continue running in the background, keeping the file system responsive.
 * 4. Monitor Logs:
 *    - Keep the Console open to verify logs appear every 5 minutes.
 *    - If you see "Refresh button not found", the Colab UI may have changed. Contact the repo maintainer
 *      or check the selector (see Debugging below).
 * 5. Stop the Script (Optional):
 *    - To stop, paste the following into the Console and press Enter:
 *      ```
 *      if (window.refresherInterval) {
 *          clearInterval(window.refresherInterval);
 *          console.log("File system refresher stopped");
 *      } else {
 *          console.log("No refresher interval found");
 *      }
 *      ```
 *    - This stops the periodic refresh. Otherwise, it runs until the browser tab closes.
 * 6. Adjust Interval (Optional):
 *    - To change the refresh interval, edit the `300000` (milliseconds) in `setInterval(refreshFileSystem, 300000)`:
 *      - Use `120000` for 2 minutes (more frequent).
 *      - Use `600000` for 10 minutes (less frequent).
 *
 * Debugging:
 * - Test the refresh button selector before running:
 *   ```
 *   document.querySelector('md-icon-button.file-tree-refresh')
 *   ```
 *   Should return an element (not null). If null, try:
 *   ```
 *   document.querySelector('md-icon-button[title="Refresh"]')
 *   ```
 *   Update the script with the working selector.
 * - If the file explorer doesn’t flicker or logs show “Refresh button not found”, the selector needs updating.
 */

(function() {
    function refreshFileSystem() {
        console.log("Refreshing file system at " + new Date().toISOString());
        // Target the refresh button in Colab's file explorer
        let refreshButton = document.querySelector('md-icon-button.file-tree-refresh');
        if (refreshButton) {
            refreshButton.click();
            console.log("File system refresh triggered");
        } else {
            console.log("Refresh button not found - selector may need updating");
        }
        // Simulate clicking the runtime status to keep session active
        let runtimeButton = document.querySelector("#runtime-menu-button");
        if (runtimeButton) {
            runtimeButton.click();
            setTimeout(() => {
                document.body.click(); // Close any opened menu
            }, 500);
            console.log("Runtime button clicked for activity");
        } else {
            console.log("Runtime button not found - selector may need updating");
        }
    }
    // Run immediately and then every 5 minutes (300000 ms)
    refreshFileSystem();
    let refresherInterval = setInterval(refreshFileSystem, 300000);
    console.log("File system refresher started with interval 300 seconds");
    // Store interval ID globally to allow stopping later
    window.refresherInterval = refresherInterval;
})();