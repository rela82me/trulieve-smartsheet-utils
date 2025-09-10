"""
Smartsheet Utilization & Age Analyzer
A clean, efficient script that doesn't over-engineer the simple task of checking
how full your sheets are and when they were last touched.

Usage:
    python analyzer.py --workspace 1234567890 --output report.xlsx
"""

import os
import argparse
import time
import traceback
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any

import smartsheet
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

# Constants - because Smartsheet has opinions about limits
LIMITS = {
    'max_rows': 20_000,
    'max_cols': 400,
    'max_cells': 5_000_000,
    'max_refs': 100
}


@dataclass
class SheetAnalysis:
    """Clean data structure for sheet metrics - no overengineering here"""
    name: str
    id: int
    container: str
    owner: Optional[str] = None
    created_date: Optional[str] = None
    modified_date: Optional[str] = None
    days_since_modified: Optional[int] = None
    rows_used: int = 0
    rows_allocated: int = 0
    columns: int = 0
    filled_cells: int = 0
    total_cells: int = 0
    cross_refs: int = 0
    row_utilization: float = 0.0
    column_utilization: float = 0.0
    cell_utilization: float = 0.0
    filled_cell_ratio: float = 0.0
    ref_utilization: float = 0.0
    gantt_enabled: Optional[bool] = None
    dependencies_enabled: Optional[bool] = None
    permalink: Optional[str] = None
    error: Optional[str] = None


class SmartsheetAnalyzer:
    """Does one thing well: analyzes Smartsheet utilization and age"""

    def __init__(self, token: str, delay: float = 0.1, quiet: bool = False):
        self.client = smartsheet.Smartsheet(token)
        self.delay = delay
        self.quiet = quiet
        self.sheets_found = []
        self.errors = []

        # Test the connection
        try:
            self.client.Users.get_current_user()
            if not self.quiet:
                print("✅ Connected to Smartsheet")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Smartsheet: {e}")

    def unwrap(self, response):
        """Handle SDK's inconsistent response wrapping"""
        return response.data if hasattr(response, 'data') else response

    def calculate_days_since(self, date_str: Optional[str]) -> Optional[int]:
        """Calculate days since a given date"""
        if not date_str:
            return None
        try:
            # Parse the date (Smartsheet uses ISO format)
            date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            return (now - date_obj).days
        except (ValueError, AttributeError):
            return None

    def discover_sheets(self, workspace_ids: List[int], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Find sheets in workspaces - stops early if limit is hit"""
        discovered = []

        # Progress bar for discovery
        desc = f"🔍 Discovering sheets (limit: {limit or 'none'})"
        with tqdm(total=len(workspace_ids), desc=desc, disable=self.quiet,
                  bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt}') as pbar:

            for wid in workspace_ids:
                if limit and len(discovered) >= limit:
                    break

                try:
                    ws = self.unwrap(self.client.Workspaces.get_workspace(wid))
                    ws_name = ws.name

                    # Sheets directly in workspace
                    for sheet in getattr(ws, 'sheets', []):
                        if limit and len(discovered) >= limit:
                            break
                        discovered.append({
                            'stub': sheet,
                            'container': ws_name,
                            'type': 'sheet'
                        })

                    # Sheets in folders (but stop if we hit limit)
                    if not limit or len(discovered) < limit:
                        for folder in getattr(ws, 'folders', []):
                            if limit and len(discovered) >= limit:
                                break
                            discovered.extend(self._discover_in_folder(
                                folder.id, ws_name, limit, len(discovered)
                            ))

                except Exception as e:
                    if not self.quiet:
                        tqdm.write(f"⚠️  Workspace {wid}: {str(e)[:50]}...")
                    self.errors.append({
                        'item_type': 'workspace',
                        'item_id': wid,
                        'error': str(e)
                    })

                pbar.update(1)

        if limit and len(discovered) > limit:
            discovered = discovered[:limit]

        if not self.quiet:
            print(f"📋 Found {len(discovered)} sheets to analyze")

        return discovered

    def _discover_in_folder(self, folder_id: int, path_prefix: str,
                            limit: Optional[int] = None, current_count: int = 0) -> List[Dict[str, Any]]:
        """Recursively discover sheets in folders - respects global limit"""
        if limit and current_count >= limit:
            return []

        discovered = []
        try:
            folder = self.unwrap(self.client.Folders.get_folder(folder_id))
            folder_path = f"{path_prefix} / {folder.name}"

            # Sheets in this folder
            for sheet in getattr(folder, 'sheets', []):
                if limit and (current_count + len(discovered)) >= limit:
                    break
                discovered.append({
                    'stub': sheet,
                    'container': folder_path,
                    'type': 'sheet'
                })

            # Recurse into subfolders if we haven't hit limit
            if not limit or (current_count + len(discovered)) < limit:
                for subfolder in getattr(folder, 'folders', []):
                    if limit and (current_count + len(discovered)) >= limit:
                        break
                    discovered.extend(self._discover_in_folder(
                        subfolder.id, folder_path, limit, current_count +
                        len(discovered)
                    ))

        except Exception as e:
            # Silent failure for folders - don't spam the user
            self.errors.append({
                'item_type': 'folder',
                'item_id': folder_id,
                'error': str(e)
            })

        return discovered

    def analyze_sheet(self, sheet_info: Dict[str, Any]) -> SheetAnalysis:
        """Analyze a single sheet for utilization and age metrics"""
        stub = sheet_info['stub']

        try:
            time.sleep(self.delay)  # Respect rate limits

            # Get the full sheet data
            sheet = self.unwrap(self.client.Sheets.get_sheet(
                stub.id,
                include="crossSheetReferences,summary,ownerInfo"
            ))

            # Basic counts
            rows_used = len(sheet.rows)
            columns = len(sheet.columns)
            total_cells = rows_used * columns

            # Count actually filled cells
            filled_cells = sum(
                1 for row in sheet.rows
                for cell in row.cells
                if cell.value not in (None, "", [])
            )

            # Cross-sheet references
            cross_refs = len(getattr(sheet, 'cross_sheet_references', []))

            # Calculate utilizations
            row_util = (rows_used / LIMITS['max_rows']) * 100
            col_util = (columns / LIMITS['max_cols']) * 100
            ref_util = (cross_refs / LIMITS['max_refs']) * 100

            # Cell utilization is trickier due to Smartsheet's complex limits
            max_possible_cells = min(
                LIMITS['max_cells'],
                LIMITS['max_rows'] * columns,
                LIMITS['max_cols'] * rows_used
            )
            cell_util = (total_cells / max_possible_cells) * \
                100 if max_possible_cells > 0 else 0
            filled_ratio = (filled_cells / total_cells) * \
                100 if total_cells > 0 else 0

            # Date handling
            created_date = getattr(sheet, 'created_at', None)
            modified_date = getattr(sheet, 'modified_at', None)

            if created_date:
                created_date = created_date.isoformat() if hasattr(
                    created_date, 'isoformat') else str(created_date)
            if modified_date:
                modified_date = modified_date.isoformat() if hasattr(
                    modified_date, 'isoformat') else str(modified_date)

            days_since = self.calculate_days_since(modified_date)

            return SheetAnalysis(
                name=sheet.name,
                id=sheet.id,
                container=sheet_info['container'],
                owner=getattr(sheet, 'owner', None),
                created_date=created_date,
                modified_date=modified_date,
                days_since_modified=days_since,
                rows_used=rows_used,
                rows_allocated=rows_used,  # Smartsheet doesn't pre-allocate like Excel
                columns=columns,
                filled_cells=filled_cells,
                total_cells=total_cells,
                cross_refs=cross_refs,
                row_utilization=round(row_util, 2),
                column_utilization=round(col_util, 2),
                cell_utilization=round(cell_util, 2),
                filled_cell_ratio=round(filled_ratio, 2),
                ref_utilization=round(ref_util, 2),
                gantt_enabled=getattr(sheet, 'gantt_enabled', None),
                dependencies_enabled=getattr(
                    sheet, 'dependencies_enabled', None),
                permalink=getattr(sheet, 'permalink', None)
            )

        except Exception as e:
            error_msg = f"Analysis failed: {e}"

            return SheetAnalysis(
                name=stub.name,
                id=stub.id,
                container=sheet_info['container'],
                error=error_msg
            )

    def analyze_concurrent(self, sheet_infos: List[Dict[str, Any]], max_workers: int = 10) -> List[SheetAnalysis]:
        """Analyze multiple sheets concurrently"""
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_info = {
                executor.submit(self.analyze_sheet, info): info
                for info in sheet_infos
            }

            # Process results with progress bar
            desc = f"📊 Analyzing {len(sheet_infos)} sheets"
            with tqdm(total=len(sheet_infos), desc=desc, disable=self.quiet,
                      bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:

                for future in as_completed(future_to_info):
                    try:
                        result = future.result()
                        results.append(result)

                        # Clean, minimal output
                        if not self.quiet and not result.error:
                            age_str = f"{result.days_since_modified}d" if result.days_since_modified else "?"
                            tqdm.write(
                                f"✓ {result.name[:40]:<40} | {result.cell_utilization:5.1f}% | {age_str}")

                    except Exception as e:
                        info = future_to_info[future]
                        results.append(SheetAnalysis(
                            name=info['stub'].name,
                            id=info['stub'].id,
                            container=info['container'],
                            error=f"Processing failed: {e}"
                        ))

                    pbar.update(1)

        return results

    def save_results(self, results: List[SheetAnalysis], output_file: str):
        """Save results to Excel with proper formatting"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base, ext = os.path.splitext(output_file)
        final_file = f"{base}_{timestamp}{ext}"

        # Separate successful analyses from errors
        successful = [r for r in results if not r.error]
        failed = [r for r in results if r.error]

        with pd.ExcelWriter(final_file, engine='xlsxwriter') as writer:
            workbook = writer.book

            # Formats
            percent_fmt = workbook.add_format({'num_format': '0.00%'})
            date_fmt = workbook.add_format(
                {'num_format': 'yyyy-mm-dd hh:mm:ss'})
            int_fmt = workbook.add_format({'num_format': '0'})

            # Main results sheet
            if successful:
                df = pd.DataFrame([asdict(r) for r in successful])
                df.to_excel(writer, sheet_name='Sheet Analysis', index=False)

                worksheet = writer.sheets['Sheet Analysis']

                # Apply formatting
                percent_cols = ['row_utilization', 'column_utilization', 'cell_utilization',
                                'filled_cell_ratio', 'ref_utilization']
                date_cols = ['created_date', 'modified_date']
                int_cols = ['id', 'rows_used', 'rows_allocated', 'columns', 'filled_cells',
                            'total_cells', 'cross_refs', 'days_since_modified']

                for col in percent_cols:
                    if col in df.columns:
                        col_idx = df.columns.get_loc(col)
                        worksheet.set_column(col_idx, col_idx, 12, percent_fmt)

                for col in int_cols:
                    if col in df.columns:
                        col_idx = df.columns.get_loc(col)
                        worksheet.set_column(col_idx, col_idx, 12, int_fmt)

                # Auto-adjust column widths
                for i, col in enumerate(df.columns):
                    max_len = max(df[col].astype(
                        str).apply(len).max(), len(col)) + 2
                    worksheet.set_column(i, i, min(max_len, 50))

            # Errors sheet
            if failed or self.errors:
                error_data = []

                # Analysis errors
                for r in failed:
                    error_data.append({
                        'Sheet Name': r.name,
                        'Sheet ID': r.id,
                        'Container': r.container,
                        'Error Type': 'Analysis Error',
                        'Error Message': r.error
                    })

                # Discovery errors
                for e in self.errors:
                    error_data.append({
                        'Sheet Name': 'N/A',
                        'Sheet ID': e.get('item_id', 'N/A'),
                        'Container': 'N/A',
                        'Error Type': f"{e['item_type'].title()} Error",
                        'Error Message': e['error']
                    })

                if error_data:
                    pd.DataFrame(error_data).to_excel(
                        writer, sheet_name='Errors', index=False)

        if not self.quiet:
            print(f"💾 Report saved: {final_file}")
        return final_file

    def print_summary(self, results: List[SheetAnalysis]):
        """Print a helpful summary of the analysis"""
        successful = [r for r in results if not r.error]
        failed = [r for r in results if r.error]

        print(f"\n{'='*60}")
        print(f"📊 ANALYSIS SUMMARY")
        print(f"{'='*60}")
        print(f"✅ Sheets analyzed successfully: {len(successful)}")
        print(f"❌ Sheets with errors: {len(failed)}")
        print(f"⚠️  Discovery errors: {len(self.errors)}")

        if successful:
            df = pd.DataFrame([asdict(r) for r in successful])

            print(f"\n📈 UTILIZATION AVERAGES:")
            print(f"   Rows: {df['row_utilization'].mean():.1f}%")
            print(f"   Columns: {df['column_utilization'].mean():.1f}%")
            print(f"   Cells: {df['cell_utilization'].mean():.1f}%")
            print(f"   Filled: {df['filled_cell_ratio'].mean():.1f}%")
            print(f"   Cross-refs: {df['ref_utilization'].mean():.1f}%")

            print(f"\n📅 AGE STATISTICS:")
            valid_ages = df['days_since_modified'].dropna()
            if len(valid_ages) > 0:
                print(f"   Average age: {valid_ages.mean():.0f} days")
                print(f"   Oldest: {valid_ages.max():.0f} days")
                print(f"   Newest: {valid_ages.min():.0f} days")

            # High utilization warnings
            high_util = df[df['cell_utilization'] > 80]
            if len(high_util) > 0:
                print(f"\n⚠️  HIGH UTILIZATION (>80% cells):")
                for _, row in high_util.head(5).iterrows():
                    print(
                        f"   • {row['name'][:45]:<45} {row['cell_utilization']:5.1f}%")

            # Old sheets
            old_sheets = df[df['days_since_modified'] > 365]
            if len(old_sheets) > 0:
                print(f"\n🕰️  STALE SHEETS (>1 year):")
                for _, row in old_sheets.head(5).iterrows():
                    print(
                        f"   • {row['name'][:45]:<45} {row['days_since_modified']:4.0f} days")


def main():
    start_time = time.time()

    # Load environment variables
    load_dotenv()

    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="Analyze Smartsheet utilization and age")
    parser.add_argument("--workspace", type=int, nargs="+", required=True,
                        help="Workspace ID(s) to analyze")
    parser.add_argument("--output", default="smartsheet_analysis.xlsx",
                        help="Output Excel file")
    parser.add_argument("--workers", type=int, default=10,
                        help="Number of concurrent workers")
    parser.add_argument("--delay", type=float, default=0.1,
                        help="Delay between API calls in seconds")
    parser.add_argument("--limit", type=int,
                        help="Limit analysis to first N sheets (for testing)")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress progress output")

    args = parser.parse_args()

    # Set up logging - silence the chatty SDK
    if args.quiet:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(level=logging.WARNING)

    # Shut up the Smartsheet SDK's verbose logging
    logging.getLogger("smartsheet").setLevel(logging.WARNING)
    logging.getLogger("smartsheet.smartsheet").setLevel(logging.WARNING)
    logging.getLogger("smartsheet.client").setLevel(logging.WARNING)

    # Get API token
    token = os.environ.get("SMARTSHEET_ACCESS_TOKEN")
    if not token:
        print("❌ SMARTSHEET_ACCESS_TOKEN environment variable not set")
        return 1

    try:
        # Initialize analyzer
        analyzer = SmartsheetAnalyzer(
            token, delay=args.delay, quiet=args.quiet)

        # Discover sheets (respects limit from the start)
        sheet_infos = analyzer.discover_sheets(
            args.workspace, limit=args.limit)

        if not sheet_infos:
            print("❌ No sheets found to analyze")
            return 1

        # Analyze sheets
        results = analyzer.analyze_concurrent(
            sheet_infos, max_workers=args.workers)

        # Save and summarize
        analyzer.save_results(results, args.output)
        analyzer.print_summary(results)

        elapsed = time.time() - start_time
        print(f"\n⏱️  Total time: {elapsed:.1f} seconds")
        print(f"✅ Analysis complete!")

    except KeyboardInterrupt:
        print("\n🛑 Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
