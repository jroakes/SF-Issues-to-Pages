import gspread
import datetime
from tqdm.auto import tqdm
from config import credentials


gc = gspread.authorize(credentials)

def create_folder(folder_name):
    """Create a folder in the root of the drive"""
    
    DRIVE_FILES_API_V3_URL = gspread.urls.DRIVE_FILES_API_V3_URL

    payload = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }

    params = {
        "supportsAllDrives": True,
    }
    
    r = gc.request("post", DRIVE_FILES_API_V3_URL, json=payload, params=params)
    folder_id = r.json()["id"]
    
    return folder_id


def get_folder_id(folder_name):
    """Get the folder id of a folder in the root of the drive"""
    
    DRIVE_FILES_API_V3_URL = gspread.urls.DRIVE_FILES_API_V3_URL

    params = {
        "supportsAllDrives": True,
        "q": f"name = '{folder_name}'"
    }
    
    r = gc.request("get", DRIVE_FILES_API_V3_URL, params=params)
    folder_id = r.json()["files"][0]["id"]
    
    return folder_id


def create_folder_name(url, version=None):
    """Create a folder name from a url and today's date"""

    today = datetime.date.today().strftime("%Y-%m-%d")
    folder_name = f"{today} - {url}"

    if isinstance(version, str):
        folder_name = f"{folder_name} - {version}"
    
    
    return folder_name


def df_to_sheets(df, folder_id, sheet_name):
    """Upload a dataframe to a google sheet"""

    # This is important to avoid a errors in gspread
    df = df.fillna("")
    
    # Create a new sheet
    sheet = gc.create(sheet_name, folder_id=folder_id)
    
    # Update the sheet with the dataframe. .sheet1 is the first sheet.
    sheet.sheet1.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option="USER_ENTERED")
    
    return sheet


def get_sheet_url(sheet):
    """Get the url of a google sheet"""
    
    return sheet.url


def add_sheet_to_sheet(sheet, df, sheet_name):
    """Add a sheet to a google sheet"""
    
    df = df.fillna("")
    rows, cols = df.shape

    # Create a new sheet
    new_sheet = sheet.add_worksheet(sheet_name, rows=rows, cols=cols)
    
    # Update the sheet with the dataframe
    new_sheet.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option="USER_ENTERED")
    
    return new_sheet


def load_all_issues_to_sheets(issues, url, version=None):
    """Loads all issues from dict containing data[issue_name] = df
    to individual google sheets. Returns a dict containing issue_name = sheet_url"""

    # Create a folder for the crawl
    folder_name = create_folder_name(url, version=version)
    folder_id = create_folder(folder_name)

    # Create a dict to store the sheet urls
    sheet_urls = {}

    # Loop through the issues
    for issue_name, df in tqdm(issues.items(), desc="Uploading Issues to Google Sheets"):
        # Create a sheet for the issue
        sheet = df_to_sheets(df, folder_id, issue_name)
        # Get the sheet url
        sheet_url = get_sheet_url(sheet)
        # Add the sheet url to the dict
        sheet_urls[issue_name] = sheet_url
    
    return sheet_urls


def load_ouput_data_to_sheet(output_name, output_data, url, version=None):
    """Loads all output data from dict containing data[issue_name] = df
    to a single google sheet. Returns the sheet url"""

    # Create a folder for the crawl
    folder_name = create_folder_name(url, version=version)
    folder_id = get_folder_id(folder_name)

    sheet = gc.create(output_name, folder_id=folder_id)

    for report_name, df in tqdm(output_data.items(), desc="Building Report"):
        # Create a sheet for the issue
        new_sheet = add_sheet_to_sheet(sheet, df, report_name)

    sheet_url = get_sheet_url(sheet)
    
    return sheet_url


