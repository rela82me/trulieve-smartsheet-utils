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
from datetime import datetime as python_datetime, timezone, timedelta  # Added timedelta
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


class AssetInfo(NamedTuple):  # Renamed to be more general
    stub: Any
    asset_type: str  # New field for asset type
    container_path: str
    access_level: Optional[str]


class AssetMetrics(NamedTuple):  # Renamed to be more general
    asset_name: str
    asset_id: int
    asset_type: str  # Added asset type here too
    container_path: str
    access_level: Optional[str]
    owner: Optional[str]
    owner_id: Optional[int]
    created_at: Optional[python_datetime]  # Naive datetime
    last_modified_at: Optional[python_datetime]  # Naive datetime
    # New field for days since last modified
    days_since_modified: Optional[int]
    version_saves: Optional[int]
    permalink: Optional[str]
    total_allocated_rows: Any
    rows_with_data: Any
    row_util_percent: Any
    total_columns: Any
    col_util_percent: Any
    total_cells_current_shape: Any
    filled_cells: Any
    filled_cell_util_percent: Any
    shape_cell_util_percent: Any
    cross_sheet_refs: Any
    ref_util_percent: Any
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
    if dt_aware.tzinfo is None:  # Already naive
        return dt_aware
    # Convert to UTC, then make naive
    return dt_aware.astimezone(timezone.utc).replace(tzinfo=None)


def calculate_days_since(dt: Optional[python_datetime]) -> Optional[int]:
    """Calculates days from a datetime object until now."""
    if dt is None:
        return None
    now_utc = python_datetime.now(timezone.utc).replace(tzinfo=None)
    time_diff = now_utc - dt
    return time_diff.days

# --- Smartsheet API Interaction ---


def fetch_smartsheet_data_for_analysis(context: ProcessingContext, asset_type: str, asset_id: int) -> Optional[Any]:
    if context.shutdown_event.is_set():
        return None
    try:
        if asset_type in ['Sheet', 'Report']:
            return unwrap(context.client.Sheets.get_sheet(
                asset_id,
                include="crossSheetReferences,summary,ownerInfo,sourceInfo"
            ))
        elif asset_type == 'Dashboard':
            # Dashboard stub already has all we can get
            return None
        else:
            return None
    except smartsheet.exceptions.SmartsheetException as e:
        logging.warning(
            f"API Error fetching {asset_type} {asset_id}: Code {e.error_code} - {e.message}")
        context.error_logs.append({
            "Asset ID": asset_id, "Item Type": asset_type,
            "Error": f"API Error (code {e.error_code}): {e.message}"
        })
        return None


def calculate_asset_metrics(asset_obj: Any, asset_info: AssetInfo) -> AssetMetrics:
    sname = asset_obj.name if hasattr(
        asset_obj, 'name') else asset_info.stub.name
    sid = asset_obj.id if hasattr(asset_obj, 'id') else asset_info.stub.id

    # Common fields for all asset types
    created_at_raw = getattr(asset_obj, 'createdAt', None) or getattr(
        asset_info.stub, 'createdAt', None)
    modified_at_raw = getattr(asset_obj, 'modifiedAt', None) or getattr(
        asset_info.stub, 'modifiedAt', None)

    created_at_dt = make_naive_utc_datetime(created_at_raw)
    modified_at_dt = make_naive_utc_datetime(modified_at_raw)
    days_since_mod = calculate_days_since(modified_at_dt)

    # Default values for dashboard-like assets
    rows_with_data = 'N/A'
    total_allocated_rows = 'N/A'
    cols = 'N/A'
    current_total_cells = 'N/A'
    filled_cells = 'N/A'
    row_util = 'N/A'
    col_util = 'N/A'
    shape_cell_util = 'N/A'
    filled_cell_util = 'N/A'
    ref_cnt = 'N/A'
    ref_util = 'N/A'
    gantt_enabled = 'N/A'
    dependencies_enabled = 'N/A'
    resource_management_enabled = 'N/A'
    from_template = 'N/A'

    if asset_info.asset_type in ['Sheet', 'Report']:
        rows_with_data = len(asset_obj.rows)
        cols = len(asset_obj.columns)
        total_allocated_rows = rows_with_data
        if hasattr(asset_obj, 'summary') and hasattr(asset_obj.summary, 'total_row_count'):
            total_allocated_rows = asset_obj.summary.total_row_count
        current_total_cells = rows_with_data * cols
        filled_cells = sum(
            1 for r in asset_obj.rows for c in r.cells if c.value not in (None, "", []))
        row_util = (total_allocated_rows /
                    SMARTSHEET_MAX_ROWS) if SMARTSHEET_MAX_ROWS > 0 else 0.0
        col_util = (
            cols / SMARTSHEET_MAX_COLS) if SMARTSHEET_MAX_COLS > 0 else 0.0
        max_cells_for_shape = min(SMARTSHEET_MAX_CELLS_TOTAL, SMARTSHEET_MAX_ROWS *
                                  cols, SMARTSHEET_MAX_COLS * total_allocated_rows)
        max_cells_for_shape = max(max_cells_for_shape, current_total_cells)
        shape_cell_util = (current_total_cells /
                           max_cells_for_shape) if max_cells_for_shape > 0 else 0.0
        filled_cell_util = (
            filled_cells / current_total_cells) if current_total_cells > 0 else 0.0
        ref_cnt = len(asset_obj.cross_sheet_references) if hasattr(
            asset_obj, 'cross_sheet_references') else 0
        ref_util = (
            ref_cnt / SMARTSHEET_MAX_INBOUND_REFS_PER_SHEET) if SMARTSHEET_MAX_INBOUND_REFS_PER_SHEET > 0 else 0.0
        gantt_enabled = getattr(asset_obj, 'gantt_enabled', None)
        dependencies_enabled = getattr(asset_obj, 'dependencies_enabled', None)
        resource_management_enabled = getattr(
            asset_obj, 'resource_management_enabled', None)
        from_template = (getattr(asset_obj.source, 'type', None) == 'template') if hasattr(
            asset_obj, 'source') and asset_obj.source else None

    return AssetMetrics(
        asset_name=sname, asset_id=sid, asset_type=asset_info.asset_type,
        container_path=asset_info.container_path, access_level=asset_info.access_level,
        owner=getattr(asset_obj, 'owner', None), owner_id=getattr(asset_obj, 'ownerId', None),
        created_at=created_at_dt, last_modified_at=modified_at_dt,
        days_since_modified=days_since_mod,
        version_saves=getattr(asset_obj, 'version', None), permalink=getattr(asset_obj, 'permalink', None),
        total_allocated_rows=total_allocated_rows, rows_with_data=rows_with_data, row_util_percent=row_util,
        total_columns=cols, col_util_percent=col_util,
        total_cells_current_shape=current_total_cells, filled_cells=filled_cells,
        filled_cell_util_percent=filled_cell_util, shape_cell_util_percent=shape_cell_util,
        cross_sheet_refs=ref_cnt, ref_util_percent=ref_util,
        effective_attachments=getattr(
            asset_obj, 'effectiveAttachmentCount', None),
        gantt_enabled=gantt_enabled, dependencies_enabled=dependencies_enabled,
        resource_management_enabled=resource_management_enabled, from_template=from_template
    )


def worker_analyze_asset(context: ProcessingContext, asset_info: AssetInfo) -> AssetMetrics:
    error_metrics_payload = {
        field: None for field in AssetMetrics._fields
    }
    error_metrics_payload.update({
        "asset_name": asset_info.stub.name, "asset_id": asset_info.stub.id,
        "asset_type": asset_info.asset_type,
        "container_path": asset_info.container_path, "access_level": asset_info.access_level,
        "total_allocated_rows": "N/A", "rows_with_data": "N/A", "row_util_percent": "N/A",
        "total_columns": "N/A", "col_util_percent": "N/A", "total_cells_current_shape": "N/A", "filled_cells": "N/A",
        "filled_cell_util_percent": "N/A", "shape_cell_util_percent": "N/A", "cross_sheet_refs": "N/A",
        "ref_util_percent": "N/A"
    })

    if context.shutdown_event.is_set():
        return AssetMetrics(**error_metrics_payload, error="Analysis cancelled by user.")

    # Dashboards don't need a separate API call, we have what we need from the stub
    if asset_info.asset_type == 'Dashboard':
        return calculate_asset_metrics(asset_info.stub, asset_info)

    asset_obj = fetch_smartsheet_data_for_analysis(
        context, asset_info.asset_type, asset_info.stub.id)

    if context.shutdown_event.is_set():
        return AssetMetrics(**error_metrics_payload, error="Analysis cancelled post-API call.")

    if asset_obj:
        try:
            metrics = calculate_asset_metrics(asset_obj, asset_info)
            if not context.quiet_mode:
                tqdm.write(
                    f"📊 {metrics.asset_name[:25].ljust(25)} ({metrics.container_path[:25].ljust(25)}): "
                    f"Last Mod: {metrics.days_since_modified} days ago."
                )
            return metrics
        except Exception as e:
            logging.error(
                f"Error calculating metrics for asset {asset_info.stub.name} ({asset_info.stub.id}): {e}\n{traceback.format_exc()}")
            return AssetMetrics(**error_metrics_payload, error=f"Metric calculation error: {e}")

    return AssetMetrics(**error_metrics_payload, error="Failed to fetch asset data.")


# --- Asset Discovery ---

def fetch_assets_from_folder_recursive(context: ProcessingContext, folder_id: int, current_path_prefix: str, pbar_discovery: tqdm) -> List[AssetInfo]:
    if context.shutdown_event.is_set():
        return []
    gathered_assets: List[AssetInfo] = []
    try:
        folder = unwrap(context.client.Folders.get_folder(folder_id))
        folder_path = f"{current_path_prefix} / {folder.name}"
        if not context.quiet_mode:
            pbar_discovery.set_description(f"   Folder: {folder.name[:30]}...")

        for asset_stub in getattr(folder, "sheets", []):
            if context.shutdown_event.is_set():
                break

            asset_type = 'Report' if '/reports/' in getattr(
                asset_stub, 'permalink', '') else 'Sheet'
            gathered_assets.append(AssetInfo(
                stub=asset_stub, container_path=folder_path,
                access_level=str(getattr(asset_stub, 'access_level', None)),
                asset_type=asset_type
            ))

        for asset_stub in getattr(folder, "dashboards", []):
            if context.shutdown_event.is_set():
                break
            gathered_assets.append(AssetInfo(
                stub=asset_stub, container_path=folder_path,
                access_level=str(getattr(asset_stub, 'access_level', None)),
                asset_type='Dashboard'
            ))

        for sub_folder_stub in getattr(folder, "folders", []):
            if context.shutdown_event.is_set():
                break
            gathered_assets.extend(fetch_assets_from_folder_recursive(
                context, sub_folder_stub.id, folder_path, pbar_discovery
            ))
        if not context.shutdown_event.is_set() and not context.quiet_mode:
            pbar_discovery.update(1)
    except smartsheet.exceptions.SmartsheetException as e:
        if not context.shutdown_event.is_set():
            logging.warning(
                f"Cannot access folder {folder_id} (path: {current_path_prefix}): {e.error_code} - {e.message}")
            context.error_logs.append(
                {"Item ID": folder_id, "Item Type": "Folder", "Container Path": current_path_prefix, "Error": str(e)})
    return gathered_assets


def discover_assets_in_workspaces(context: ProcessingContext, workspace_ids: List[int], discovery_limit: Optional[int]) -> List[AssetInfo]:
    all_discovered_assets: List[AssetInfo] = []
    logging.info("Starting asset discovery... (Press Ctrl+C to interrupt)")

    initial_pbar_total = len(workspace_ids)
    pbar_kwargs = {"disable": context.quiet_mode, "total": initial_pbar_total,
                   "desc": "Discovering items", "unit": "item"}

    try:
        with tqdm(**pbar_kwargs) as pbar_discovery:
            for widx, wid in enumerate(workspace_ids):
                if context.shutdown_event.is_set() or (discovery_limit is not None and len(all_discovered_assets) >= discovery_limit):
                    break
                try:
                    ws = unwrap(context.client.Workspaces.get_workspace(
                        wid, include="sheets,dashboards,folders"))
                    ws_path = ws.name
                    logging.info(f"Scanning Workspace: {ws_path} ({ws.id})")
                    if not context.quiet_mode:
                        pbar_discovery.update(1)

                    for asset_stub in getattr(ws, "sheets", []):
                        if context.shutdown_event.is_set() or (discovery_limit is not None and len(all_discovered_assets) >= discovery_limit):
                            break
                        asset_type = 'Report' if '/reports/' in getattr(
                            asset_stub, 'permalink', '') else 'Sheet'
                        all_discovered_assets.append(AssetInfo(
                            stub=asset_stub, container_path=ws_path,
                            access_level=str(
                                getattr(asset_stub, 'access_level', None)),
                            asset_type=asset_type
                        ))

                    for asset_stub in getattr(ws, "dashboards", []):
                        if context.shutdown_event.is_set() or (discovery_limit is not None and len(all_discovered_assets) >= discovery_limit):
                            break
                        all_discovered_assets.append(AssetInfo(
                            stub=asset_stub, container_path=ws_path,
                            access_level=str(
                                getattr(asset_stub, 'access_level', None)),
                            asset_type='Dashboard'
                        ))

                    if context.shutdown_event.is_set() or (discovery_limit is not None and len(all_discovered_assets) >= discovery_limit):
                        break

                    num_folders_in_ws = len(getattr(ws, "folders", []))
                    if num_folders_in_ws > 0 and not context.quiet_mode:
                        pbar_discovery.total += num_folders_in_ws

                    for fld_stub in getattr(ws, "folders", []):
                        if context.shutdown_event.is_set() or (discovery_limit is not None and len(all_discovered_assets) >= discovery_limit):
                            break
                        all_discovered_assets.extend(fetch_assets_from_folder_recursive(
                            context, fld_stub.id, ws_path, pbar_discovery
                        ))
                        if discovery_limit is not None and len(all_discovered_assets) >= discovery_limit:
                            all_discovered_assets = all_discovered_assets[:discovery_limit]
                            break
                except smartsheet.exceptions.SmartsheetException as e:
                    if not context.shutdown_event.is_set():
                        logging.error(
                            f"Couldn’t open workspace {wid}: {e.error_code} - {e.message}")
                        context.error_logs.append(
                            {"Item ID": wid, "Item Type": "Workspace", "Error": str(e)})

            if not context.quiet_mode:
                pbar_desc = "Discovery complete" if not context.shutdown_event.is_set(
                ) else "Discovery interrupted"
                pbar_discovery.set_description(pbar_desc)
                if pbar_discovery.n < pbar_discovery.total:
                    if pbar_discovery.total > pbar_discovery.n:
                        pbar_discovery.update(
                            pbar_discovery.total - pbar_discovery.n)
    except KeyboardInterrupt:
        logging.info("Discovery phase interrupted by user.")
        context.shutdown_event.set()

    if discovery_limit is not None and len(all_discovered_assets) > discovery_limit:
        logging.info(f"Discovery limited to first {discovery_limit} sheets.")
        return all_discovered_assets[:discovery_limit]
    return all_discovered_assets

# --- Concurrent Processing ---


def process_assets_concurrently(context: ProcessingContext, assets_to_analyze: List[AssetInfo], max_workers: int) -> List[AssetMetrics]:
    analyzed_results: List[AssetMetrics] = []
    logging.info(
        f"Analyzing {len(assets_to_analyze)} assets using up to {max_workers} workers...")

    pbar_kwargs = {"disable": context.quiet_mode, "total": len(
        assets_to_analyze), "desc": "Analyzing Assets", "unit": "asset"}

    try:
        with tqdm(**pbar_kwargs) as analysis_pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                future_to_asset_info = {
                    pool.submit(worker_analyze_asset, context, s_info): s_info
                    for s_info in assets_to_analyze
                }
                for future in as_completed(future_to_asset_info):
                    if context.shutdown_event.is_set():
                        if sys.version_info >= (3, 9):
                            for f_cancel in future_to_asset_info:
                                if not f_cancel.done():
                                    f_cancel.cancel()
                        break
                    try:
                        res = future.result()
                        analyzed_results.append(res)
                        if res.error and "cancelled by user" in res.error and not context.quiet_mode:
                            tqdm.write(
                                f"🚫 Asset {res.asset_name} analysis cancelled.")
                    except Exception as exc:
                        s_info_item = future_to_asset_info[future]
                        logging.critical(
                            f"Critical error processing future for asset {s_info_item.stub.name}: {exc}\n{traceback.format_exc()}")
                        error_metrics_payload = {
                            field: None for field in AssetMetrics._fields
                        }
                        error_metrics_payload.update({
                            "asset_name": s_info_item.stub.name, "asset_id": s_info_item.stub.id,
                            "asset_type": s_info_item.asset_type,
                            "container_path": s_info_item.container_path, "access_level": s_info_item.access_level,
                            "error": f"Future execution failed: {exc}",
                            "total_allocated_rows": "N/A", "rows_with_data": "N/A", "row_util_percent": "N/A",
                            "total_columns": "N/A", "col_util_percent": "N/A", "total_cells_current_shape": "N/A", "filled_cells": "N/A",
                            "filled_cell_util_percent": "N/A", "shape_cell_util_percent": "N/A", "cross_sheet_refs": "N/A",
                            "ref_util_percent": "N/A"
                        })
                        analyzed_results.append(
                            AssetMetrics(**error_metrics_payload))

                    if not context.quiet_mode:
                        analysis_pbar.update(1)
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


def save_report_to_excel(context: ProcessingContext, output_filename: str, metrics_results: List[AssetMetrics]):
    timestamp = python_datetime.now().strftime(
        "%Y%m%d_%H%M%S")  # Use aliased datetime
    outfile_name_parts = os.path.splitext(output_filename)
    status_suffix = "_partial" if context.shutdown_event.is_set() and (
        metrics_results or context.error_logs) else ""
    final_outfile = f"{outfile_name_parts[0]}{status_suffix}_{timestamp}{outfile_name_parts[1]}"

    logging.info(f"Saving report to {final_outfile}...")

    valid_metrics_dicts = [m._asdict()
                           for m in metrics_results if m.error is None]
    all_errors_for_report = list(context.error_logs)
    for m in metrics_results:
        if m.error:
            all_errors_for_report.append({
                "Asset Name": m.asset_name, "Asset ID": m.asset_id,
                "Container Path": m.container_path, "Error": m.error,
                "Item Type": m.asset_type
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

            metric_column_order = [
                field_name for field_name in AssetMetrics._fields if field_name != 'error']
            if not df_metrics.empty:
                df_metrics = df_metrics.reindex(columns=[col for col in metric_column_order if col in df_metrics.columns] +
                                                [col for col in df_metrics.columns if col not in metric_column_order])
                df_metrics.to_excel(
                    writer, sheet_name="Asset Metrics", index=False)
                worksheet_metrics = writer.sheets["Asset Metrics"]
                format_config_metrics = {
                    'percent': ['row_util_percent', 'col_util_percent', 'filled_cell_util_percent', 'shape_cell_util_percent', 'ref_util_percent'],
                    'integer': ['total_allocated_rows', 'rows_with_data', 'total_columns', 'total_cells_current_shape', 'filled_cells', 'cross_sheet_refs', 'version_saves', 'effective_attachments', 'owner_id', 'asset_id', 'days_since_modified'],
                    'datetime': ['created_at', 'last_modified_at']
                }
                apply_excel_formats_and_widths(
                    worksheet_metrics, df_metrics, format_config_metrics, workbook_formats)
            else:
                pd.DataFrame(columns=metric_column_order).to_excel(
                    writer, sheet_name="Asset Metrics", index=False)

            error_column_order = ["Asset Name", "Asset ID",
                                  "Item ID", "Item Type", "Container Path", "Error"]
            if not df_errors.empty:
                for col in error_column_order:
                    if col not in df_errors.columns:
                        df_errors[col] = None
                df_errors = df_errors.reindex(columns=[col for col in error_column_order if col in df_errors.columns] +
                                              [col for col in df_errors.columns if col not in error_column_order])
                df_errors.to_excel(
                    writer, sheet_name="Errors and Issues", index=False)
                apply_excel_formats_and_widths(
                    writer.sheets["Errors and Issues"], df_errors, {}, workbook_formats)
                logging.warning(
                    f"{len(df_errors)} errors/issues logged. Check 'Errors and Issues' tab.")
            else:
                pd.DataFrame(columns=error_column_order).to_excel(
                    writer, sheet_name="Errors and Issues", index=False)
        logging.info(
            f"Report {'partially ' if context.shutdown_event.is_set() else ''}saved to {final_outfile}")
    except Exception as e:
        logging.error(
            f"Failed to save Excel report: {e}\n{traceback.format_exc()}")

# --- Summary Statistics ---


def print_summary_report(metrics_results: List[AssetMetrics], context: ProcessingContext):
    valid_metrics_df = pd.DataFrame(
        [m._asdict() for m in metrics_results if m.error is None])
    num_assets_analyzed = len(valid_metrics_df)

    num_discovery_api_errors = len(context.error_logs)
    num_analysis_errors_in_metrics = sum(
        1 for m in metrics_results if m.error and "cancelled by user" not in m.error.lower())
    num_total_non_cancellation_errors = num_discovery_api_errors + \
        num_analysis_errors_in_metrics
    num_cancelled = sum(
        1 for m in metrics_results if m.error and "cancelled by user" in m.error.lower())

    summary = ["\n--- 📊 Execution Summary ---"]
    summary.append(f"Assets Successfully Analyzed: {num_assets_analyzed}")
    summary.append(
        f"Errors Encountered (excluding cancellations): {num_total_non_cancellation_errors}")
    if context.shutdown_event.is_set() or num_cancelled > 0:
        summary.append(f"Analyses Cancelled by User: {num_cancelled}")

    if num_assets_analyzed > 0:
        summary.extend([
            "\nUtilization Averages (for successfully analyzed sheets):",
            f"  Average Row Utilization: {valid_metrics_df.get('row_util_percent', pd.Series()).mean():.2%}",
            f"  Average Column Utilization: {valid_metrics_df.get('col_util_percent', pd.Series()).mean():.2%}",
            f"  Average Shape Cell Utilization: {valid_metrics_df.get('shape_cell_util_percent', pd.Series()).mean():.2%}",
            f"  Average Filled Cell Utilization: {valid_metrics_df.get('filled_cell_util_percent', pd.Series()).mean():.2%}",
            f"  Average Cross-Sheet Ref Utilization: {valid_metrics_df.get('ref_util_percent', pd.Series()).mean():.2%}"
        ])
        if 'row_util_percent' in valid_metrics_df.columns:
            high_row_util_sheets = valid_metrics_df[valid_metrics_df['row_util_percent'] > 0.80]
            summary.append(
                f"\nSheets with >80% Row Utilization: {len(high_row_util_sheets)}")
            if 0 < len(high_row_util_sheets) <= 10:
                for _, row in high_row_util_sheets.iterrows():
                    summary.append(
                        f"  - {row['asset_name']} ({row['row_util_percent']:.2%})")
    summary.append("-----------------------------")
    logging.info("\n".join(summary))

# --- Main Application Logic ---


def main():
    # Correctly load .env from the parent directory
    # Assumes .env is in C:\...\SmartsheetUtilities\
    load_dotenv(os.path.join(os.path.dirname(
        os.path.abspath(__file__)), '.env'))
    start_script_time = time.time()

    parser = argparse.ArgumentParser(
        description="Smartsheet utilization audit tool.")
    parser.add_argument("--workspace", type=int, nargs="+",
                        required=True, help="One or more workspace IDs.")
    parser.add_argument(
        "--output", default="Smartsheet_Utilization_Report.xlsx", help="Excel output file name.")
    parser.add_argument("--max_workers", type=int, default=10,
                        help="Max concurrent analysis threads.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit discovery to the first N sheets (for testing).")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress progress bars and info-level logs.")
    cli_args = parser.parse_args()

    setup_logging(cli_args.quiet)

    smartsheet_access_token = os.environ.get("SMARTSHEET_ACCESS_TOKEN")
    if not smartsheet_access_token:
        logging.critical(
            "SMARTSHEET_ACCESS_TOKEN environment variable not set. Ensure it's in your environment or .env file.")
        return

    try:
        ss_client = smartsheet.Smartsheet(smartsheet_access_token)
        ss_client.Users.get_current_user()
    except Exception as e:
        logging.critical(
            f"Failed to initialize Smartsheet client or invalid token: {e}")
        return

    processing_ctx = ProcessingContext(
        client=ss_client, quiet_mode=cli_args.quiet)

    try:
        assets_to_analyze = discover_assets_in_workspaces(
            processing_ctx, cli_args.workspace, cli_args.limit)

        if processing_ctx.shutdown_event.is_set() and not assets_to_analyze:
            logging.info("No assets discovered before interruption. Exiting.")
            if processing_ctx.error_logs:
                save_report_to_excel(processing_ctx, cli_args.output, [])
            return
        elif not assets_to_analyze:
            logging.info(
                "No assets found to analyze based on criteria. Exiting.")
            if processing_ctx.error_logs:
                save_report_to_excel(processing_ctx, cli_args.output, [])
            return

        logging.info(f"{len(assets_to_analyze)} assets queued for analysis.")

        analysis_start_time = time.time()
        analyzed_asset_metrics = process_assets_concurrently(
            processing_ctx, assets_to_analyze, cli_args.max_workers)
        analysis_duration = time.time() - analysis_start_time
        status_msg = "interrupted" if processing_ctx.shutdown_event.is_set() else "finished"
        logging.info(
            f"Analysis {status_msg} in {analysis_duration:.2f} seconds.")

        if analyzed_asset_metrics or processing_ctx.error_logs:
            save_report_to_excel(
                processing_ctx, cli_args.output, analyzed_asset_metrics)
        else:
            if processing_ctx.shutdown_event.is_set():
                logging.info("Process interrupted, no data collected.")
            else:
                logging.info("No data collected, no errors. Nothing to save.")

        print_summary_report(analyzed_asset_metrics, processing_ctx)

    except Exception as e:
        logging.critical(
            f"An unexpected critical error occurred in main: {e}\n{traceback.format_exc()}")
    finally:
        total_script_duration = time.time() - start_script_time
        logging.info(
            f"Total script execution time: {total_script_duration:.2f} seconds.")


if __name__ == "__main__":
    main()
