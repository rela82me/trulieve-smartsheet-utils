from smartsheet import Smartsheet
import os

client = Smartsheet(os.environ["SMARTSHEET_ACCESS_TOKEN"])
workspace_id = 5925329467402116

try:
    ws = client.Workspaces.get_workspace(workspace_id)
    print("✔ Workspace response object received")
    print(f"Raw response: {ws}")
    print(f" type: {type(ws)}")
    print(f"Has 'data'? { 'data' in dir(ws) }")
    print(f"Sheets in workspace: {len(ws.data.sheets)}")
except Exception as e:
    print("❌ Workspace failed to load")
    print(type(e), e)
