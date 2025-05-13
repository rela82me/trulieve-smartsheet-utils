import os, time, argparse, traceback
import smartsheet, pandas as pd

# ── helpers ────────────────────────────────────────────────────────────────────
def unwrap(r):          # handle both wrapper-or-model returns
    return r.data if hasattr(r, "data") else r

def safe_print(*a, **k):
    print(*a, **k, flush=True)

# ── cli args ───────────────────────────────────────────────────────────────────
p = argparse.ArgumentParser(
        description="Sheet-fullness & limit audit (workspace-aware, SDK-proof)")
p.add_argument("--workspace", type=int, nargs="+", required=True,
               help="One or more Smartsheet workspace IDs (space-separated)")
p.add_argument("--delay", type=float, default=0.5,
               help="Seconds to sleep between API hits (default 0.5)")
p.add_argument("--output", default="Smartsheet_Limits_Report.xlsx",
               help="Excel file to write")
args = p.parse_args()

# ── api setup ──────────────────────────────────────────────────────────────────
client = smartsheet.Smartsheet(os.environ["SMARTSHEET_ACCESS_TOKEN"])

REQUEST_DELAY = args.delay
sheet_stats, error_logs = [], []

# ── crawl each workspace ───────────────────────────────────────────────────────
sheets_to_scan = []
for wid in args.workspace:
    try:
        ws = unwrap(client.Workspaces.get_workspace(wid))
        safe_print(f"🏢 {ws.name}  ({ws.id})")

        # top-level sheets (rare in your org, but still grab ’em)
        for sh in getattr(ws, "sheets", []):
            sheets_to_scan.append(sh)

        # sheets inside every folder
        for f in ws.folders:
            safe_print(f"   📁 {f.name}")
            f_full = unwrap(client.Folders.get_folder(f.id))
            for sh in f_full.sheets:
                sheets_to_scan.append(sh)

    except Exception as e:
        safe_print(f"❌ couldn’t open workspace {wid}: {e}")
        traceback.print_exc()

# ── main loop ──────────────────────────────────────────────────────────────────
for s in sheets_to_scan:
    sid, sname = s.id, s.name
    safe_print(f"🔍 {sname}  ({sid})")
    time.sleep(REQUEST_DELAY)

    try:
        sheet = unwrap(client.Sheets.get_sheet(sid, include="crossSheetReferences"))
        refs  = unwrap(client.Sheets.list_cross_sheet_references(sid))

        rows, cols         = len(sheet.rows), len(sheet.columns)
        total_cells        = rows * cols
        filled             = sum(1 for r in sheet.rows for c in r.cells
                                 if c.value not in (None, "", []))
        row_util           = round(rows / 20000 * 100, 2)
        col_util           = round(cols / 400   * 100, 2)
        max_cells          = min(500_000, 20_000 * cols, 400 * rows)
        cell_util          = round(total_cells / max_cells * 100, 2) if max_cells else 0
        ref_cnt            = len(refs)
        ref_util           = round(ref_cnt / 100 * 100, 2)

        sheet_stats.append({
            "Sheet Name": sname, "Sheet ID": sid,
            "Total Rows": rows,  "Row Util %": row_util,
            "Total Columns": cols, "Col Util %": col_util,
            "Total Cells": total_cells, "Filled Cells": filled,
            "Cell Util %": cell_util,
            "Cross-Sheet Refs": ref_cnt, "Ref Util %": ref_util
        })

        safe_print(f"   ↳ {cell_util}% cells | {row_util}% rows | {col_util}% cols | {ref_util}% refs")

    except Exception as e:
        safe_print(f"❌ {sname} ({sid}): {e}")
        traceback.print_exc()
        error_logs.append({"Sheet Name": sname, "Sheet ID": sid, "Error": str(e)})

# ── save ───────────────────────────────────────────────────────────────────────
with pd.ExcelWriter(args.output, engine="openpyxl") as w:
    pd.DataFrame(sheet_stats).to_excel(w, sheet_name="Sheet Metrics", index=False)
    if error_logs:
        pd.DataFrame(error_logs).to_excel(w, sheet_name="Errors", index=False)

safe_print(f"\n✅ report saved to {args.output}")
