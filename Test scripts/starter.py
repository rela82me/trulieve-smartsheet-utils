import os
import smartsheet

# Grab your token from the environment
token = os.environ["SMARTSHEET_ACCESS_TOKEN"]

# Connect to Smartsheet
client = smartsheet.Smartsheet(token)

# Grab all your sheets
sheets_response = client.Sheets.list_sheets(include_all=True)
sheets = sheets_response.data

# Display a simple list
print("Here are your sheets:")
for sheet in sheets:
    print(f"- {sheet.name} (ID: {sheet.id})")
