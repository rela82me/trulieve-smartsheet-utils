"""
PROFESSIONAL GRADE REFACTOR + .env Support + Fixes v3 (Datetime Focus)
Utilization.py
"""
# This script analyzes Smartsheet workspaces and sheets for utilization metrics.
# It fetches sheet data, calculates various metrics, and saves the results to an Excel file.

# --- Script library imports ---
import os
import argparse
import traceback
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime as python_datetime, timezone # Renamed to avoid clash
import sys
from typing import List, Dict, Any, Optional, NamedTuple

import smartsheet
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

# --- Constants & Configuration ---
SMARTSHEET_MAX_ROWS = 20000
SMARTSHEET_MAX_COLS = 400
SMARTSHEET_MAX_CELLS_TOTAL = 5_000_000
SMARTSHEET_MAX_INBOUND_REFS_PER_SHEET = 100

# --- Data Structures ---
class SheetInfo(NamedTuple):
    stub: Any
    container_path: str
    access_level: Optional[str]

class SheetMetrics(NamedTuple):
    sheet_name: str
    sheet_id: int
    container_path: str
    access_level: Optional[str]
    owner: Optional[str]
    owner_id: Optional[int]
    created_at: Optional[python_datetime] # Naive datetime
    last_modified_at: Optional[python_datetime] # Naive datetime
    version_saves: Optional[int]
    permalink: Optional[str]
    total_allocated_rows: int
    rows_with_data: int
    row_util_percent: float
    total_columns: int
    col_util_percent: float
    total_cells_current_shape: int
    filled_cells: int
    filled_cell_util_percent: float
    shape_cell_util_percent: float
    cross_sheet_refs: int
    ref_util_percent: float
    effective_attachments: Optional[int]
    gantt_enabled: Optional[bool]
    dependencies_enabled: Optional[bool]
    resource_management_enabled: Optional[bool]
    from_template: Optional[bool]
    error: Optional[str] = None

# --- Processing Context --- 
# This class encapsulates the context for processing, including the Smartsheet client and shutdown event.
# It also includes a flag for quiet mode and a list for error logs.

class ProcessingContext:
    def __init__(self, client: smartsheet.Smartsheet, quiet_mode: bool = False):
        self.client = client
        self.shutdown_event = threading.Event()
        self.quiet_mode = quiet_mode
        self.error_logs: List[Dict[str, Any]] = []

# --- Logging Setup ---
def setup_logging(quiet_mode: bool):
    log_level = logging.WARNING if quiet_mode else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("smartsheet.smartsheet").setLevel(logging.WARNING)
    logging.getLogger("smartsheet.client").setLevel(logging.WARNING)


# --- Helper Functions ---
def unwrap(resp: Any) -> Any:
    return resp.data if hasattr(resp, "data") else resp

def make_naive_utc_datetime(dt_aware: Optional[python_datetime]) -> Optional[python_datetime]:
    """Converts a timezone-aware datetime to a naive datetime in UTC."""
    if dt_aware is None:
        return None
    if dt_aware.tzinfo is None: # Already naive
        return dt_aware
    # Convert to UTC, then make naive
    return dt_aware.astimezone(timezone.utc).replace(tzinfo=None)

# --- Smartsheet API Interaction ---
def fetch_smartsheet_data_for_analysis(context: ProcessingContext, sheet_id: int) -> Optional[Any]:
    if context.shutdown_event.is_set():
        return None
    try:
        return unwrap(context.client.Sheets.get_sheet(
            sheet_id,
            include="crossSheetReferences,summary,ownerInfo,sourceInfo"
        ))
    except smartsheet.exceptions.SmartsheetException as e:
        logging.warning(f"API Error fetching sheet {sheet_id}: Code {e.error_code} - {e.message}")
        context.error_logs.append({
            "Sheet ID": sheet_id, "Item Type": "Sheet",
            "Error": f"API Error (code {e.error_code}): {e.message}"
        })
        return None

def calculate_sheet_metrics(sheet_obj: Any, sheet_info: SheetInfo) -> SheetMetrics:
    sname = sheet_obj.name if hasattr(sheet_obj, 'name') else sheet_info.stub.name
    sid = sheet_obj.id if hasattr(sheet_obj, 'id') else sheet_info.stub.id

    rows_with_data = len(sheet_obj.rows)
    cols = len(sheet_obj.columns)
    total_allocated_rows = rows_with_data
    if hasattr(sheet_obj, 'summary') and hasattr(sheet_obj.summary, 'total_row_count'):
        total_allocated_rows = sheet_obj.summary.total_row_count
    current_total_cells = rows_with_data * cols
    filled_cells = sum(1 for r in sheet_obj.rows for c in r.cells if c.value not in (None, "", []))
    row_util = (total_allocated_rows / SMARTSHEET_MAX_ROWS) if SMARTSHEET_MAX_ROWS > 0 else 0.0
    col_util = (cols / SMARTSHEET_MAX_COLS) if SMARTSHEET_MAX_COLS > 0 else 0.0
    max_cells_for_shape = min(SMARTSHEET_MAX_CELLS_TOTAL, SMARTSHEET_MAX_ROWS * cols, SMARTSHEET_MAX_COLS * total_allocated_rows)
    max_cells_for_shape = max(max_cells_for_shape, current_total_cells)
    shape_cell_util = (current_total_cells / max_cells_for_shape) if max_cells_for_shape > 0 else 0.0
    filled_cell_util = (filled_cells / current_total_cells) if current_total_cells > 0 else 0.0
    ref_cnt = len(sheet_obj.cross_sheet_references) if hasattr(sheet_obj, 'cross_sheet_references') else 0
    ref_util = (ref_cnt / SMARTSHEET_MAX_INBOUND_REFS_PER_SHEET) if SMARTSHEET_MAX_INBOUND_REFS_PER_SHEET > 0 else 0.0
    
    created_at_raw = getattr(sheet_obj, 'created_at', None)
    modified_at_raw = getattr(sheet_obj, 'modified_at', None)

    created_at_dt = make_naive_utc_datetime(created_at_raw)
    modified_at_dt = make_naive_utc_datetime(modified_at_raw)

    return SheetMetrics(
        sheet_name=sname, sheet_id=sid, container_path=sheet_info.container_path, access_level=sheet_info.access_level,
        owner=getattr(sheet_obj, 'owner', None), owner_id=getattr(sheet_obj, 'owner_id', None),
        created_at=created_at_dt, last_modified_at=modified_at_dt,
        version_saves=getattr(sheet_obj, 'version', None), permalink=getattr(sheet_obj, 'permalink', None),
        total_allocated_rows=total_allocated_rows, rows_with_data=rows_with_data, row_util_percent=row_util,
        total_columns=cols, col_util_percent=col_util,
        total_cells_current_shape=current_total_cells, filled_cells=filled_cells,
        filled_cell_util_percent=filled_cell_util, shape_cell_util_percent=shape_cell_util,
        cross_sheet_refs=ref_cnt, ref_util_percent=ref_util,
        effective_attachments=getattr(sheet_obj.summary, 'effective_attachment_count', None) if hasattr(sheet_obj, 'summary') else None,
        gantt_enabled=getattr(sheet_obj, 'gantt_enabled', None),
        dependencies_enabled=getattr(sheet_obj, 'dependencies_enabled', None),
        resource_management_enabled=getattr(sheet_obj, 'resource_management_enabled', None),
        from_template=(getattr(sheet_obj.source, 'type', None) == 'template') if hasattr(sheet_obj, 'source') and sheet_obj.source else None
    )

def worker_analyze_sheet(context: ProcessingContext, sheet_info: SheetInfo) -> SheetMetrics:
    error_metrics_payload = {
        field: None for field in SheetMetrics._fields
    }
    error_metrics_payload.update({
        "sheet_name": sheet_info.stub.name, "sheet_id": sheet_info.stub.id,
        "container_path": sheet_info.container_path, "access_level": sheet_info.access_level,
        "total_allocated_rows":0, "rows_with_data":0, "row_util_percent":0.0,
        "total_columns":0, "col_util_percent":0.0, "total_cells_current_shape":0, "filled_cells":0,
        "filled_cell_util_percent":0.0, "shape_cell_util_percent":0.0, "cross_sheet_refs":0,
        "ref_util_percent":0.0
    })

    if context.shutdown_event.is_set():
        return SheetMetrics(**error_metrics_payload, error="Analysis cancelled by user.")

    sheet_obj = fetch_smartsheet_data_for_analysis(context, sheet_info.stub.id)

    if context.shutdown_event.is_set():
        return SheetMetrics(**error_metrics_payload, error="Analysis cancelled post-API call.")

    if sheet_obj:
        try:
            metrics = calculate_sheet_metrics(sheet_obj, sheet_info)
            if not context.quiet_mode:
                tqdm.write(
                    f"📊 {metrics.sheet_name[:25].ljust(25)} ({metrics.container_path[:25].ljust(25)}): "
                    f"Cells {metrics.shape_cell_util_percent:.0%} | Filled {metrics.filled_cell_util_percent:.0%} | "
                    f"Rows {metrics.row_util_percent:.0%} | Refs {metrics.ref_util_percent:.0%}"
                )
            return metrics
        except Exception as e:
            logging.error(f"Error calculating metrics for sheet {sheet_info.stub.name} ({sheet_info.stub.id}): {e}\n{traceback.format_exc()}")
            return SheetMetrics(**error_metrics_payload, error=f"Metric calculation error: {e}")
    
    return SheetMetrics(**error_metrics_payload, error="Failed to fetch sheet data.")


# --- Sheet Discovery ---
def fetch_sheets_from_folder_recursive(context: ProcessingContext, folder_id: int, current_path_prefix: str, pbar_discovery: tqdm) -> List[SheetInfo]:
    if context.shutdown_event.is_set(): return []
    gathered_sheets: List[SheetInfo] = []
    try:
        folder = unwrap(context.client.Folders.get_folder(folder_id))
        folder_path = f"{current_path_prefix} / {folder.name}"
        if not context.quiet_mode:
            pbar_discovery.set_description(f"   Folder: {folder.name[:30]}...")

        for sheet_stub in getattr(folder, "sheets", []):
            if context.shutdown_event.is_set(): break
            gathered_sheets.append(SheetInfo(
                stub=sheet_stub, container_path=folder_path,
                access_level=str(getattr(sheet_stub, 'access_level', None))
            ))

        for sub_folder_stub in getattr(folder, "folders", []):
            if context.shutdown_event.is_set(): break
            gathered_sheets.extend(fetch_sheets_from_folder_recursive(
                context, sub_folder_stub.id, folder_path, pbar_discovery
            ))
        if not context.shutdown_event.is_set() and not context.quiet_mode:
            pbar_discovery.update(1)
    except smartsheet.exceptions.SmartsheetException as e:
        if not context.shutdown_event.is_set():
            logging.warning(f"Cannot access folder {folder_id} (path: {current_path_prefix}): {e.error_code} - {e.message}")
            context.error_logs.append({"Item ID": folder_id, "Item Type": "Folder", "Container Path": current_path_prefix, "Error": str(e)})
    return gathered_sheets

def discover_sheets_in_workspaces(context: ProcessingContext, workspace_ids: List[int], discovery_limit: Optional[int]) -> List[SheetInfo]:
    all_discovered_sheets: List[SheetInfo] = []
    logging.info("Starting sheet discovery... (Press Ctrl+C to interrupt)")

    initial_pbar_total = len(workspace_ids)
    pbar_kwargs = {"disable": context.quiet_mode, "total": initial_pbar_total, "desc":"Discovering items", "unit":"item"}

    try:
        with tqdm(**pbar_kwargs) as pbar_discovery:
            for widx, wid in enumerate(workspace_ids):
                if context.shutdown_event.is_set() or (discovery_limit is not None and len(all_discovered_sheets) >= discovery_limit):
                    break
                try:
                    ws = unwrap(context.client.Workspaces.get_workspace(wid))
                    ws_path = ws.name
                    logging.info(f"Scanning Workspace: {ws_path} ({ws.id})")
                    if not context.quiet_mode: pbar_discovery.update(1)

                    for sheet_stub in getattr(ws, "sheets", []):
                        if context.shutdown_event.is_set() or (discovery_limit is not None and len(all_discovered_sheets) >= discovery_limit): break
                        all_discovered_sheets.append(SheetInfo(
                            stub=sheet_stub, container_path=ws_path,
                            access_level=str(getattr(sheet_stub, 'access_level', None))
                        ))
                    if context.shutdown_event.is_set() or (discovery_limit is not None and len(all_discovered_sheets) >= discovery_limit): break
                    
                    num_folders_in_ws = len(getattr(ws, "folders", []))
                    if num_folders_in_ws > 0 and not context.quiet_mode:
                        pbar_discovery.total += num_folders_in_ws 

                    for fld_stub in getattr(ws, "folders", []):
                        if context.shutdown_event.is_set() or (discovery_limit is not None and len(all_discovered_sheets) >= discovery_limit): break
                        all_discovered_sheets.extend(fetch_sheets_from_folder_recursive(
                            context, fld_stub.id, ws_path, pbar_discovery
                        ))
                        if discovery_limit is not None and len(all_discovered_sheets) >= discovery_limit:
                            all_discovered_sheets = all_discovered_sheets[:discovery_limit]
                            break
                except smartsheet.exceptions.SmartsheetException as e:
                    if not context.shutdown_event.is_set():
                        logging.error(f"Couldn’t open workspace {wid}: {e.error_code} - {e.message}")
                        context.error_logs.append({"Item ID": wid, "Item Type": "Workspace", "Error": str(e)})
                
            if not context.quiet_mode:
                pbar_desc = "Discovery complete" if not context.shutdown_event.is_set() else "Discovery interrupted"
                pbar_discovery.set_description(pbar_desc)
                if pbar_discovery.n < pbar_discovery.total: 
                    if pbar_discovery.total > pbar_discovery.n :
                        pbar_discovery.update(pbar_discovery.total - pbar_discovery.n)
    except KeyboardInterrupt:
        logging.info("Discovery phase interrupted by user.")
        context.shutdown_event.set()
    
    if discovery_limit is not None and len(all_discovered_sheets) > discovery_limit:
        logging.info(f"Discovery limited to first {discovery_limit} sheets.")
        return all_discovered_sheets[:discovery_limit]
    return all_discovered_sheets

# --- Concurrent Processing ---
def process_sheets_concurrently(context: ProcessingContext, sheets_to_analyze: List[SheetInfo], max_workers: int) -> List[SheetMetrics]:
    analyzed_results: List[SheetMetrics] = []
    logging.info(f"Analyzing {len(sheets_to_analyze)} sheets using up to {max_workers} workers...")
    
    pbar_kwargs = {"disable": context.quiet_mode, "total": len(sheets_to_analyze), "desc":"Analyzing Sheets", "unit":"sheet"}

    try:
        with tqdm(**pbar_kwargs) as analysis_pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                future_to_sheet_info = {
                    pool.submit(worker_analyze_sheet, context, s_info): s_info
                    for s_info in sheets_to_analyze
                }
                for future in as_completed(future_to_sheet_info):
                    if context.shutdown_event.is_set():
                        if sys.version_info >= (3, 9):
                            for f_cancel in future_to_sheet_info:
                                if not f_cancel.done(): f_cancel.cancel()
                        break
                    try:
                        res = future.result() 
                        analyzed_results.append(res)
                        if res.error and "cancelled by user" in res.error and not context.quiet_mode:
                               tqdm.write(f"🚫 Sheet {res.sheet_name} analysis cancelled.")
                    except Exception as exc:
                        s_info_item = future_to_sheet_info[future]
                        logging.critical(f"Critical error processing future for sheet {s_info_item.stub.name}: {exc}\n{traceback.format_exc()}")
                        error_metrics_payload = {
                            field: None for field in SheetMetrics._fields
                        }
                        error_metrics_payload.update({
                            "sheet_name": s_info_item.stub.name, "sheet_id": s_info_item.stub.id,
                            "container_path": s_info_item.container_path, "access_level": s_info_item.access_level,
                            "error": f"Future execution failed: {exc}",
                            "total_allocated_rows":0, "rows_with_data":0, "row_util_percent":0.0,
                            "total_columns":0, "col_util_percent":0.0, "total_cells_current_shape":0, "filled_cells":0,
                            "filled_cell_util_percent":0.0, "shape_cell_util_percent":0.0, "cross_sheet_refs":0,
                            "ref_util_percent":0.0
                        })
                        analyzed_results.append(SheetMetrics(**error_metrics_payload))

                    if not context.quiet_mode: analysis_pbar.update(1)
    except KeyboardInterrupt:
        logging.info("Analysis interrupted by user. Shutting down workers...")
        context.shutdown_event.set()
    return analyzed_results

# --- Excel Output ---
def apply_excel_formats_and_widths(worksheet: Any, df: pd.DataFrame, format_config: Dict[str, List[str]], workbook_formats: Dict[str, Any]):
    for fmt_name, columns in format_config.items():
        excel_format = workbook_formats.get(fmt_name)
        if excel_format:
            for col_name in columns:
                if col_name in df.columns:
                    col_idx = df.columns.get_loc(col_name)
                    worksheet.set_column(col_idx, col_idx, None, excel_format)

    for col_idx, col_name in enumerate(df.columns):
        series = df[col_name]
        if not series.empty:
            max_len_data = series.astype(str).fillna('').map(len).max()
        else:
            max_len_data = 0
        max_len = max(max_len_data, len(str(col_name)))
        worksheet.set_column(col_idx, col_idx, max_len + 2)


def save_report_to_excel(context: ProcessingContext, output_filename: str, metrics_results: List[SheetMetrics]):
    timestamp = python_datetime.now().strftime("%Y%m%d_%H%M%S") # Use aliased datetime
    outfile_name_parts = os.path.splitext(output_filename)
    status_suffix = "_partial" if context.shutdown_event.is_set() and (metrics_results or context.error_logs) else ""
    final_outfile = f"{outfile_name_parts[0]}{status_suffix}_{timestamp}{outfile_name_parts[1]}"

    logging.info(f"Saving report to {final_outfile}...")

    valid_metrics_dicts = [m._asdict() for m in metrics_results if m.error is None]
    all_errors_for_report = list(context.error_logs)
    for m in metrics_results:
        if m.error:
            all_errors_for_report.append({
                "Sheet Name": m.sheet_name, "Sheet ID": m.sheet_id,
                "Container Path": m.container_path, "Error": m.error,
                "Item Type": "Sheet" 
            })
    
    df_metrics = pd.DataFrame(valid_metrics_dicts)
    df_errors = pd.DataFrame(all_errors_for_report)

    try:
        with pd.ExcelWriter(final_outfile, engine='xlsxwriter') as writer:
            workbook = writer.book
            workbook_formats = {
                'percent': workbook.add_format({'num_format': '0.00%'}),
                'integer': workbook.add_format({'num_format': '0'}),
                'datetime': workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss'}) 
            }

            metric_column_order = [field_name for field_name in SheetMetrics._fields if field_name != 'error']
            if not df_metrics.empty:
                df_metrics = df_metrics.reindex(columns=[col for col in metric_column_order if col in df_metrics.columns] +
                                                         [col for col in df_metrics.columns if col not in metric_column_order])
                df_metrics.to_excel(writer, sheet_name="Sheet Metrics", index=False)
                worksheet_metrics = writer.sheets["Sheet Metrics"]
                format_config_metrics = {
                    'percent': ['row_util_percent', 'col_util_percent', 'filled_cell_util_percent', 'shape_cell_util_percent', 'ref_util_percent'],
                    'integer': ['total_allocated_rows', 'rows_with_data', 'total_columns', 'total_cells_current_shape', 'filled_cells', 'cross_sheet_refs', 'version_saves', 'effective_attachments', 'owner_id', 'sheet_id'],
                    'datetime': ['created_at', 'last_modified_at']
                }
                apply_excel_formats_and_widths(worksheet_metrics, df_metrics, format_config_metrics, workbook_formats)
            else:
                 pd.DataFrame(columns=metric_column_order).to_excel(writer, sheet_name="Sheet Metrics", index=False)


            error_column_order = ["Sheet Name", "Sheet ID", "Item ID", "Item Type", "Container Path", "Error"]
            if not df_errors.empty:
                for col in error_column_order:
                    if col not in df_errors.columns:
                        df_errors[col] = None
                df_errors = df_errors.reindex(columns=[col for col in error_column_order if col in df_errors.columns] +
                                                      [col for col in df_errors.columns if col not in error_column_order])
                df_errors.to_excel(writer, sheet_name="Errors and Issues", index=False)
                apply_excel_formats_and_widths(writer.sheets["Errors and Issues"], df_errors, {}, workbook_formats)
                logging.warning(f"{len(df_errors)} errors/issues logged. Check 'Errors and Issues' tab.")
            else:
                pd.DataFrame(columns=error_column_order).to_excel(writer, sheet_name="Errors and Issues", index=False)
        logging.info(f"Report {'partially ' if context.shutdown_event.is_set() else ''}saved to {final_outfile}")
    except Exception as e:
        logging.error(f"Failed to save Excel report: {e}\n{traceback.format_exc()}")

# --- Summary Statistics ---
def print_summary_report(metrics_results: List[SheetMetrics], context: ProcessingContext):
    valid_metrics_df = pd.DataFrame([m._asdict() for m in metrics_results if m.error is None])
    num_sheets_analyzed = len(valid_metrics_df)
    
    num_discovery_api_errors = len(context.error_logs)
    num_analysis_errors_in_metrics = sum(1 for m in metrics_results if m.error and "cancelled by user" not in m.error.lower())
    num_total_non_cancellation_errors = num_discovery_api_errors + num_analysis_errors_in_metrics
    num_cancelled = sum(1 for m in metrics_results if m.error and "cancelled by user" in m.error.lower())

    summary = ["\n--- 📊 Execution Summary ---"]
    summary.append(f"Sheets Successfully Analyzed: {num_sheets_analyzed}")
    summary.append(f"Errors Encountered (excluding cancellations): {num_total_non_cancellation_errors}")
    if context.shutdown_event.is_set() or num_cancelled > 0 :
        summary.append(f"Analyses Cancelled by User: {num_cancelled}")

    if num_sheets_analyzed > 0:
        summary.extend([
            "\nUtilization Averages (for successfully analyzed sheets):",
            f"  Average Row Utilization: {valid_metrics_df['row_util_percent'].mean():.2%}",
            f"  Average Column Utilization: {valid_metrics_df['col_util_percent'].mean():.2%}",
            f"  Average Shape Cell Utilization: {valid_metrics_df['shape_cell_util_percent'].mean():.2%}",
            f"  Average Filled Cell Utilization: {valid_metrics_df['filled_cell_util_percent'].mean():.2%}",
            f"  Average Cross-Sheet Ref Utilization: {valid_metrics_df['ref_util_percent'].mean():.2%}"
        ])
        high_row_util_sheets = valid_metrics_df[valid_metrics_df['row_util_percent'] > 0.80]
        summary.append(f"\nSheets with >80% Row Utilization: {len(high_row_util_sheets)}")
        if 0 < len(high_row_util_sheets) <= 10:
            for _, row in high_row_util_sheets.iterrows():
                summary.append(f"  - {row['sheet_name']} ({row['row_util_percent']:.2%})")
    summary.append("-----------------------------")
    logging.info("\n".join(summary))

# --- Main Application Logic ---
def main():
    load_dotenv()
    start_script_time = time.time()

    parser = argparse.ArgumentParser(description="Smartsheet utilization audit tool.")
    parser.add_argument("--workspace", type=int, nargs="+", required=True, help="One or more workspace IDs.")
    parser.add_argument("--output", default="Smartsheet_Utilization_Report.xlsx", help="Excel output file name.")
    parser.add_argument("--max_workers", type=int, default=10, help="Max concurrent analysis threads.")
    parser.add_argument("--limit", type=int, default=None, help="Limit discovery to the first N sheets (for testing).")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress bars and info-level logs.")
    cli_args = parser.parse_args()

    setup_logging(cli_args.quiet)

    smartsheet_access_token = os.environ.get("SMARTSHEET_ACCESS_TOKEN")
    if not smartsheet_access_token:
        logging.critical("SMARTSHEET_ACCESS_TOKEN environment variable not set. Ensure it's in your environment or .env file.")
        return

    try:
        ss_client = smartsheet.Smartsheet(smartsheet_access_token)
        ss_client.Users.get_current_user()
    except Exception as e:
        logging.critical(f"Failed to initialize Smartsheet client or invalid token: {e}")
        return

    processing_ctx = ProcessingContext(client=ss_client, quiet_mode=cli_args.quiet)

    try:
        sheets_to_analyze = discover_sheets_in_workspaces(processing_ctx, cli_args.workspace, cli_args.limit)
        
        if processing_ctx.shutdown_event.is_set() and not sheets_to_analyze:
            logging.info("No sheets discovered before interruption. Exiting.")
            if processing_ctx.error_logs: save_report_to_excel(processing_ctx, cli_args.output, [])
            return
        elif not sheets_to_analyze:
            logging.info("No sheets found to analyze based on criteria. Exiting.")
            if processing_ctx.error_logs: save_report_to_excel(processing_ctx, cli_args.output, [])
            return
        
        logging.info(f"{len(sheets_to_analyze)} sheets queued for analysis.")

        analysis_start_time = time.time()
        analyzed_sheet_metrics = process_sheets_concurrently(processing_ctx, sheets_to_analyze, cli_args.max_workers)
        analysis_duration = time.time() - analysis_start_time
        status_msg = "interrupted" if processing_ctx.shutdown_event.is_set() else "finished"
        logging.info(f"Analysis {status_msg} in {analysis_duration:.2f} seconds.")
        
        if analyzed_sheet_metrics or processing_ctx.error_logs:
             save_report_to_excel(processing_ctx, cli_args.output, analyzed_sheet_metrics)
        else:
            if processing_ctx.shutdown_event.is_set(): logging.info("Process interrupted, no data collected.")
            else: logging.info("No data collected, no errors. Nothing to save.")
        
        print_summary_report(analyzed_sheet_metrics, processing_ctx)

    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in main: {e}\n{traceback.format_exc()}")
    finally:
        total_script_duration = time.time() - start_script_time
        logging.info(f"Total script execution time: {total_script_duration:.2f} seconds.")

if __name__ == "__main__":
    main()