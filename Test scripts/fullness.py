import os
import smartsheet
import pandas as pd
import time
import traceback

# === Helper to unwrap SDK responses ===
unwrap = lambda r: r.data if hasattr(r, "data") else r

# === Config ===
REQUEST_DELAY = 0.5  # To stay under API rate limit
TOKEN_ENV = "SMARTSHEET_ACCESS_TOKEN"

token = os.environ[TOKEN_ENV]
client = smartsheet.Smartsheet(token)

# === Sheet + Error Storage ===
sheet_list_resp = client.Sheets.list_sheets(include_all=True)
sheet_list = unwrap(sheet_list_resp)

sheet_stats = []
error_logs = []

# === Analysis Loop ===
for sheet_info in sheet_list:
    sheet_id = sheet_info.id
    sheet_name = sheet_info.name
    print(f"Processing: {sheet_name} (ID: {sheet_id})")
    time.sleep(REQUEST_DELAY)

    try:
        # Pull sheet + reference data
        sheet_resp = client.Sheets.get_sheet(sheet_id, include="crossSheetReferences")
        sheet = unwrap(sheet_resp)

        refs_resp = client.Sheets.list_cross_sheet_references(sheet_id)
        refs = unwrap(refs_resp)

        row_count = len(sheet.rows)
        col_count = len(sheet.columns)
        total_cells = row_count * col_count

        # Count filled cells
        filled_cells = sum(
            1
            for row in sheet.rows
            for cell in row.cells
            if cell.value not in [None, "", []]
        )

        # Utilization metrics
        row_util = round((row_count / 20000) * 100, 2)
        col_util = round((col_count / 400) * 100, 2)
        max_possible_cells = min(500000, 20000 * col_count, 400 * row_count)
        cell_util = (
            round((total_cells / max_possible_cells) * 100, 2)
            if max_possible_cells
            else 0
        )
        ref_count = len(refs)
        ref_util = round((ref_count / 100) * 100, 2)

        # Append metrics to results
        sheet_stats.append(
            {
                "Sheet Name": sheet_name,
                "Sheet ID": sheet_id,
                "Total Rows": row_count,
                "Row Util %": row_util,
                "Total Columns": col_count,
                "Col Util %": col_util,
                "Total Cells (R×C)": total_cells,
                "Filled Cells": filled_cells,
                "Cell Util %": cell_util,
                "Cross-Sheet Refs": ref_count,
                "Ref Util %": ref_util,
            }
        )

        print(
            f"→ {sheet_name}: {cell_util}% cells, {row_util}% rows, {col_util}% cols, {ref_util}% refs"
        )

    except Exception as e:
        err_msg = f"Error with '{sheet_name}' (ID: {sheet_id}): {e}"
        print(f"❌ {err_msg}")
        traceback.print_exc()

        error_logs.append({
            "Sheet Name": sheet_name,
            "Sheet ID": sheet_id,
            "Error": str(e),
        })

# === Save Report ===
output_file = "Smartsheet_Limits_Report.xlsx"
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    pd.DataFrame(sheet_stats).to_excel(writer, sheet_name="Sheet Metrics", index=False)
    if error_logs:
        pd.DataFrame(error_logs).to_excel(writer, sheet_name="Errors", index=False)

print(f"\n🎉 Done! Report saved as '{output_file}'")
