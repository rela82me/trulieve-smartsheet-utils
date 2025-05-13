from smartsheet import Smartsheet
import os

client = Smartsheet(os.environ["SMARTSHEET_ACCESS_TOKEN"])

workspaces = client.Workspaces.list_workspaces().data
for ws in workspaces:
    print(f"Workspace Name: {ws.name}, ID: {ws.id}")
