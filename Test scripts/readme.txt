Venv Environment activations 

.\smartsheet-env\Scripts\activate


Shell to add token to session: 
$env:SMARTSHEET_ACCESS_TOKEN="your-token-goes-here"

Run instructions: 
python Utilization.py --workspace 5925329467402116 --output wfm_workspace_analysis_report420test.xlsx --max_workers 15 --limit 3

AI NOTES 

# Smartsheet Utilization Audit Tool

This Python script performs a utilization audit of specified Smartsheet workspaces. It analyzes sheets within these workspaces for row, column, cell, and cross-sheet reference usage against Smartsheet limits. The script generates a detailed Excel report and provides a summary in the console.

## Features

*   **Workspace & Folder Traversal:** Recursively scans specified workspaces and their subfolders to find all sheets.
*   **Comprehensive Metrics:** For each sheet, calculates:
    *   Row, column, cell, and cross-sheet reference counts and utilization percentages.
    *   Filled cell counts and utilization.
    *   Owner information, creation/modification dates, version (save count).
    *   Permalink.
    *   Status of Gantt, Dependencies, and Resource Management features.
    *   Whether the sheet was created from a template.
    *   User's access level to the sheet.
*   **Concurrent Analysis:** Uses a thread pool to analyze multiple sheets concurrently, speeding up the process.
*   **Rate Limit Respect:** Relies on the Smartsheet SDK's built-in exponential backoff and retry mechanisms to handle API rate limits.
*   **Detailed Excel Reporting:**
    *   Outputs data to an `.xlsx` file with separate tabs for "Sheet Metrics" and "Errors and Issues".
    *   Formats percentages, dates, and integers for readability.
    *   Auto-adjusts column widths.
*   **Console Summary:** Prints a summary of the analysis, including averages and counts of high-utilization sheets.
*   **Error Handling:** Logs errors encountered during discovery or analysis to both the console and the Excel report.
*   **Interruptible:** Can be safely interrupted (Ctrl+C) and will attempt to save any data processed so far.
*   **Configurable:** Accepts command-line arguments for target workspaces, output file, concurrency level, and processing limits.
*   **.env Support:** Can load the `SMARTSHEET_ACCESS_TOKEN` from a `.env` file.
*   **Logging:** Uses Python's `logging` module for structured output.

## Prerequisites

1.  **Python:** Python 3.7+ is recommended.
2.  **Smartsheet Account:** A Smartsheet account with API access.
3.  **Smartsheet API Access Token:** You need an API access token with appropriate permissions to read the workspaces and sheets you intend to audit.
4.  **Required Python Packages:**
    *   `smartsheet-python-sdk`
    *   `pandas`
    *   `tqdm`
    *   `python-dotenv`
    *   `XlsxWriter` (for Excel formatting)

## Setup

1.  **Clone the Repository (if applicable) or Download the Script:**
    Get the `Utilization.py` script onto your local machine.

2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv smartsheet_env
    source smartsheet_env/bin/activate  # On Windows: smartsheet_env\Scripts\activate
    ```

3.  **Install Dependencies:**
    If a `requirements.txt` file is provided:
    ```bash
    pip install -r requirements.txt
    ```
    Otherwise, install them manually:
    ```bash
    pip install smartsheet-python-sdk pandas tqdm python-dotenv XlsxWriter
    ```

4.  **Set up Smartsheet API Access Token:**
    You have two options:
    *   **Environment Variable (Recommended for servers/CI):**
        Set the `SMARTSHEET_ACCESS_TOKEN` environment variable in your system or terminal session:
        ```bash
        export SMARTSHEET_ACCESS_TOKEN="YOUR_API_TOKEN_HERE"  # Linux/macOS
        # set SMARTSHEET_ACCESS_TOKEN="YOUR_API_TOKEN_HERE"    # Windows Command Prompt
        # $env:SMARTSHEET_ACCESS_TOKEN="YOUR_API_TOKEN_HERE"  # Windows PowerShell
        ```
    *   **.env File (Recommended for local development):**
        Create a file named `.env` in the same directory as `Utilization.py` with the following content:
        ```
        SMARTSHEET_ACCESS_TOKEN="YOUR_API_TOKEN_HERE"
        ```
        **Important:** Add `.env` to your `.gitignore` file to avoid committing your token to version control.

## Running the Script

Execute the script from your terminal using `python Utilization.py` followed by the desired arguments.

**Command-Line Arguments:**

*   `--workspace WORKSPACE_ID [WORKSPACE_ID ...]` (Required): One or more space-separated Smartsheet Workspace IDs to audit.
*   `--output OUTPUT_FILE` (Optional): Name of the Excel report file.
    *   Default: `Smartsheet_Utilization_Report.xlsx` (a timestamp will be appended).
*   `--max_workers MAX_WORKERS` (Optional): Maximum number of concurrent threads for analyzing sheets.
    *   Default: `10`.
*   `--limit LIMIT` (Optional): Process only the first `LIMIT` number of discovered sheets. Useful for testing.
    *   Default: Process all discovered sheets.
*   `--quiet` (Optional): Suppress progress bars and info-level log messages. Only warnings and errors will be shown.

**Examples:**

1.  **Audit a single workspace with default settings:**
    ```bash
    python Utilization.py --workspace 1234567890123456
    ```
    *(This will output `Smartsheet_Utilization_Report_YYYYMMDD_HHMMSS.xlsx`)*

2.  **Audit multiple workspaces, specify output file, set max workers, and limit to the first 3 sheets for a quick test:**
    ```bash
    python Utilization.py --workspace 1234567890123456 9876543210987654 --output wfm_workspace_analysis_report_test.xlsx --max_workers 15 --limit 3
    ```
    *(This will output `wfm_workspace_analysis_report_test_YYYYMMDD_HHMMSS.xlsx` containing data for up to 3 sheets)*

3.  **Run quietly for a specific workspace:**
    ```bash
    python Utilization.py --workspace 5925329467402116 --quiet
    ```

## Output

1.  **Console Output:**
    *   Progress of discovery and analysis (unless `--quiet` is used).
    *   Information, warning, and error messages logged via Python's `logging` module.
    *   A final summary of statistics, including total sheets analyzed, errors, and average utilization metrics.

2.  **Excel Report (`.xlsx` file):**
    *   A timestamp and `_partial` (if interrupted) will be appended to the specified output filename.
    *   **Sheet Metrics Tab:** Contains detailed utilization data for each successfully analyzed sheet. Columns are formatted for dates, percentages, and numbers.
    *   **Errors and Issues Tab:** Lists any errors encountered during sheet discovery or analysis, including sheet name/ID and the error message.

## Development Notes & Criticals for Later

*   **Smartsheet API Rate Limits:** The script relies on the SDK's built-in retry logic. If frequent rate limiting occurs, consider reducing `--max_workers`. For very large-scale operations, a more sophisticated queueing system or adaptive rate limiting might be needed.
*   **Memory Usage:** The script currently loads all analyzed sheet metrics into memory before writing to Excel. For extremely large numbers of sheets (tens of thousands), this could become a concern. Future enhancements might involve writing to Excel/CSV in batches.
*   **Error Robustness:** While errors are logged, further refinement could involve more specific error categorization or suggestions for resolution.
*   **Token Security:** Always ensure your `SMARTSHEET_ACCESS_TOKEN` is kept secure and not hardcoded or committed to version control. Use environment variables or `.env` files.
*   **Permissions:** The API token used needs read access to all workspaces, folders, and sheets it's intended to audit. Insufficient permissions will result in API errors logged by the script.
*   **Extensibility (Future):**
    *   The current structure (especially the "PROFESSIONAL GRADE REFACTOR" version) is moving towards a more modular design.
    *   Consider a dedicated Python package/library (`smartsheet_toolkit`) for core Smartsheet operations (discovery, analysis models, client handling) if more scripts are planned.
    *   Scripts like this one (`run_utilization_report.py`) would then become consumers of that library.
    *   This allows for better code reuse, testing, and separation of concerns.
*   **Testing:** For critical tools, implementing unit tests (e.g., for `calculate_sheet_metrics` with mock data) and integration tests would significantly improve reliability.

## Troubleshooting

*   **`SMARTSHEET_ACCESS_TOKEN environment variable not set`:** Ensure your token is correctly set via an environment variable or in a `.env` file in the script's directory.
*   **API Errors (e.g., 403 Forbidden, 404 Not Found):**
    *   Check that your API token is valid and has the necessary permissions for the target workspaces/sheets.
    *   Verify that the Workspace IDs provided are correct.
*   **`ValueError: Excel does not support datetimes with timezones`:** This should be resolved in the current version by converting datetimes to naive UTC. If it reappears, double-check the datetime handling.
*   **Low Performance / Rate Limiting:** Try reducing the value of `--max_workers`.

## Contribution (If applicable)

[Details on how others can contribute, coding standards, pull request process, etc.]

---

