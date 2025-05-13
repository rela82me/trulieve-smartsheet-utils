"""
STABLE
Utilization.py
Run:
    python Utilization.py --workspace 5925329467402116 --output wfm_workspace_limits.xlsx
"""

import os, argparse, traceback, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import smartsheet
import pandas as pd
from tqdm import tqdm

# ── helper to survive SDK mood-swings ───────────────────────────────────────────
def unwrap(resp):
    """Return resp.data if it exists, otherwise resp itself."""
    return resp.data if hasattr(resp, "data") else resp

# ── CLI args ───────────────────────────────────────────────────────────────────
p = argparse.ArgumentParser(description="Smartsheet utilization audit")
p.add_argument("--workspace", type=int, nargs="+", required=True,
               help="One or more workspace IDs (space-separated)")
p.add_argument("--output", default="Smartsheet_Utilization_Report.xlsx",
               help="Excel file to write")
p.add_argument("--delay", type=float, default=0.08,
               help="Seconds to sleep between API hits (default 0.15)")
args = p.parse_args()

TOKEN = os.environ["SMARTSHEET_ACCESS_TOKEN"]
client = smartsheet.Smartsheet(TOKEN)
REQUEST_DELAY = args.delay

# ── recurse folders ────────────────────────────────────────────────────────────
def collect_sheets_from_folder(folder_id):
    gathered = []
    folder = unwrap(client.Folders.get_folder(folder_id))
    gathered.extend(folder.sheets)
    for sub in getattr(folder, "folders", []):
        gathered.extend(collect_sheets_from_folder(sub.id))
    return gathered

# ── build sheet queue ──────────────────────────────────────────────────────────
sheets_to_scan = []
for wid in args.workspace:
    try:
        ws = unwrap(client.Workspaces.get_workspace(wid))
        print(f"🏢 {ws.name} ({ws.id})")

        sheets_to_scan.extend(getattr(ws, "sheets", []))  # loose sheets

        for fld in ws.folders:
            print(f"   📁 {fld.name}")
            sheets_to_scan.extend(collect_sheets_from_folder(fld.id))

    except Exception as e:
        print(f"❌ couldn’t open workspace {wid}: {e}")
        traceback.print_exc()

print(f"🗂️  {len(sheets_to_scan)} sheets queued for analysis\n")

# ── metrics containers ────────────────────────────────────────────────────────
sheet_stats, error_logs = [], []
tbar = tqdm(total=len(sheets_to_scan), desc="Scanning", unit="sheet")

# ── core analysis function ─────────────────────────────────────────────────────
def analyze(sheet_stub):
    sid, sname = sheet_stub.id, sheet_stub.name
    try:
        # throttle inside worker to respect global rate
        time.sleep(REQUEST_DELAY)

        sheet = unwrap(client.Sheets.get_sheet(
                       sid, include="crossSheetReferences"))
        refs  = unwrap(client.Sheets.list_cross_sheet_references(sid))

        rows, cols   = len(sheet.rows), len(sheet.columns)
        total_cells  = rows * cols
        filled       = sum(1 for r in sheet.rows for c in r.cells
                           if c.value not in (None, "", []))
        row_util     = round(rows / 20000 * 100, 2)
        col_util     = round(cols / 400   * 100, 2)
        max_cells    = min(500_000, 20_000 * cols, 400 * rows)
        cell_util    = round(total_cells / max_cells * 100, 2) if max_cells else 0
        ref_cnt      = len(refs)
        ref_util     = round(ref_cnt / 100 * 100, 2)

        tqdm.write(f"{sname}: {cell_util}% cells | {row_util}% rows | "
                   f"{col_util}% cols | {ref_util}% refs")

        return {"Sheet Name": sname,
                "Sheet ID": sid,
                "Total Rows": rows,
                "Row Util %": row_util,
                "Total Columns": cols,
                "Col Util %": col_util,
                "Total Cells": total_cells,
                "Filled Cells": filled,
                "Cell Util %": cell_util,
                "Cross-Sheet Refs": ref_cnt,
                "Ref Util %": ref_util}

    except Exception as e:
        tqdm.write(f"⚠️  {sname} ({sid}) → {e}")
        traceback.print_exc()
        return {"Sheet Name": sname, "Sheet ID": sid, "Error": str(e)}

# ── threaded execution ────────────────────────────────────────────────────────
with ThreadPoolExecutor(max_workers=12) as pool:
    fut_map = {pool.submit(analyze, s): s for s in sheets_to_scan}
    for fut in as_completed(fut_map):
        res = fut.result()
        (sheet_stats if "Error" not in res else error_logs).append(res)
        tbar.update(1)

tbar.close()

# ── save Excel ────────────────────────────────────────────────────────────────
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
outfile   = args.output.replace(".xlsx", f"_{timestamp}.xlsx")

with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
    pd.DataFrame(sheet_stats).to_excel(writer, "Sheet Metrics", index=False)
    if error_logs:
        pd.DataFrame(error_logs).to_excel(writer, "Errors", index=False)

print(f"\n✅  Report saved to {outfile}")
