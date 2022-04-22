from __future__ import print_function

import os
import os.path
import json
import gspread
import slack
import pandas as pd
import plotly.graph_objects as go

from datetime import date
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask
from flask import request, Response
from slackeventsapi import SlackEventAdapter

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']
#'https://www.googleapis.com/auth/drive.metadata.readonly'

def get_gdrive_service():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('drive', 'v3', credentials=creds)

service_gdrive = get_gdrive_service()
service_sheets = gspread.service_account(filename="creds.json")

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(os.environ['SIGNING_SECRET'], '/slack/events', app)

client = slack.WebClient(token=os.environ['SLACK_TOKEN'])
BOT_ID = client.api_call("auth.test")['user_id']

def delete_file(service, file_id):
    """Permanently delete a file, skipping the trash.

    Args:
    service: Drive API service instance.
    file_id: ID of the file to delete.
    """
    try:
        service.files().delete(fileId=file_id).execute()
    except:
        print('An error occurred')

def insert_file(service, title, description, parent_id, mime_type, filename):
    """Insert new file.
        Args:
        service: Drive API service instance.
        title: Title of the file to insert, including the extension.
        description: Description of the file to insert.
        parent_id: Parent folder's ID.
        mime_type: MIME type of the file to insert.
        filename: Filename of the file to insert.
        Returns:
        Inserted file metadata if successful, None otherwise.
    """
    media_body = MediaFileUpload(filename, mimetype=mime_type, resumable=True)
    body = {
        'title': title,
        'description': description,
        'mimeType': mime_type
    }
    # Set the parent folder.
    if parent_id:
        body['parents'] = [{'id': parent_id}]

    try:
        file = service.files().insert(
        body=body,
        media_body=media_body).execute()

        # Uncomment the following line to print the File ID
        # print 'File ID: %s' % file['id']
        return file

    except:
        print('An error occurred')
        return None

def create_spreadsheet(service, service2, permission, parent, keys, data_row):
    """Create new spreadsheet.
        Args:
        service: Drive API service instance.
        service2: Sheets API service instance.
        permission: File permission to read and write.
        parent: Main folder for the spreadsheet to be created.
        data_row: Row values.
    """

    spreadsheet_metadata = {
        'name': 'campaign' + data_row[0],
        'mimeType': 'application/vnd.google-apps.spreadsheet',
        'parents': parent.get('id')
    }
    spreadsheet_data = service.files().create(body=spreadsheet_metadata,
                                        fields='id').execute()
    service.permissions().create(fileId=spreadsheet_data.get('id'), body=permission, fields="id").execute()

    # Retrieve the existing parents to remove
    spreadsheet = service.files().get(fileId=spreadsheet_data.get('id'),
                                     fields='parents').execute()
    previous_parents = ",".join(spreadsheet.get('parents'))

    # Move the file to the new folder
    spreadsheet = service.files().update(fileId=spreadsheet_data.get('id'),
                                        addParents=parent.get('id'),
                                        removeParents=previous_parents,
                                        fields='id, parents').execute()

    sheet = service2.open(spreadsheet_metadata['name'])
    worksheet = sheet.worksheet("Sayfa1")
    worksheet.insert_row(keys + ['Total Budget'], index=1)
    worksheet.insert_row(data_row, index=2)
    worksheet.update('G2', "=C2 * E2", raw=False)

    print("New spreadsheet created successfully: " + spreadsheet_metadata['name'])
    client.chat_postMessage(channel='#automation', text="New spreadsheet created successfully: " + spreadsheet_metadata['name'])

def automation():
    today = date.today().strftime("%d_%m_%Y")

    json_file = open('creds.json')
    json_data = json.load(json_file)

    folder_metadata = {
        'name': "campaignData-" + today,
        'mimeType': 'application/vnd.google-apps.folder'
    }

    folder = service_gdrive.files().create(body=folder_metadata, fields='id').execute()

    domain_permission = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': json_data['client_email']
    }
    campaign_data_metadata = {
        'name': 'campaignData',
        'mimeType': 'application/vnd.google-apps.spreadsheet',
        'parents': folder.get('id')
    }
    media = MediaFileUpload('campaignData.xlsx',
                            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                            resumable=True)
    campaign_data = service_gdrive.files().create(body=campaign_data_metadata,
                                        media_body=media,
                                        fields='id').execute()
    service_gdrive.permissions().create(fileId=campaign_data.get('id'), body=domain_permission, fields="id").execute()


    # Retrieve the existing parents to remove
    file = service_gdrive.files().get(fileId=campaign_data.get('id'),
                                     fields='parents').execute()
    previous_parents = ",".join(file.get('parents'))

    # Move the file to the new folder
    file = service_gdrive.files().update(fileId=campaign_data.get('id'),
                                        addParents=folder.get('id'),
                                        removeParents=previous_parents,
                                        fields='id, parents').execute()

    print('The converted file is on your Google Drive and has the Id: %s' % campaign_data.get('id'))

    sheet = service_sheets.open("campaignData")
    worksheet = sheet.worksheet("Sayfa1")

    for i in range(2, worksheet.row_count):
        if worksheet.row_values(i):
            print(worksheet.row_values(i))
            create_spreadsheet(service_gdrive, service_sheets, domain_permission, folder, worksheet.row_values(1), worksheet.row_values(i))
        else:
            print("Finished !")
            client.chat_postMessage(channel='#automation', text="Automation finished")
            break

def get_total_budget(service2, campaignName):
    """Get total budget.
        Args:
        service2: Google Sheets API service instance.
        campaignName: Title of the sheet file for the campaign
        Returns:
        Total Budget value of the given campaign.
    """
    sheet = service2.open(campaignName)
    worksheet = sheet.worksheet("Sayfa1")
    cell_value = worksheet.acell('G2').value

    return cell_value

def upload_chart(channel, param_x, param_y):
    """Upload chart.
        Args:
        channel: Channel id for the file to be uploaded.
        param_x: X line values for the chart.
        param_y: Y line values for the chart.
        Returns: ""
    """
    figure = go.Figure([go.Bar(x=param_x, y=param_y)])
    figure.write_image("./chart.png")
    client.files_upload(channels=channel,
        initial_comment="Here's your chart",
        file="./chart.png")

def compare(service2, channel_id, campaignNames):
    """Upload chart.
        Args:
        service2: Google Sheets API service instance.
        channel_id: Channel id for where the comparison will be made.
        campaignNames: Names of the campaigns to be compared.
        Returns: "Success" if everything works fine
    """
    frames = []

    for i in range(len(campaignNames)):
        sheet = service2.open("campaign" + campaignNames[i])
        worksheet = sheet.worksheet("Sayfa1")
        dataFrame_values = pd.DataFrame(worksheet.get_all_records())
        frames.append(dataFrame_values)

    dataFrame = pd.concat(frames)
    print(dataFrame['Campaign Name'].tolist())

    upload_chart(channel_id, dataFrame['Campaign Name'].tolist(), dataFrame['Total Impression'].tolist())
    upload_chart(channel_id, dataFrame['Campaign Name'].tolist(), dataFrame['Total Clicks'].tolist())
    upload_chart(channel_id, dataFrame['Campaign Name'].tolist(), dataFrame['Total App Install'].tolist())

    return print("Success!")

@app.route('/run_automation', methods=['POST'])
def run_automation():
    data = request.form
    channel_id = data.get('channel_id')
    client.chat_postMessage(channel=channel_id, text="I got the command - automation is running now ...")
    automation()
    return Response(), 200

@app.route('/calculate_budget', methods=['POST'])
def calculate_budget():
    data = request.form
    channel_id = data.get('channel_id')
    parameter = data.get('text')
    total_budget = get_total_budget(service_sheets, parameter)

    client.chat_postMessage(channel=channel_id, text="Total Budget: " + total_budget)
    return Response(), 200

@app.route('/compare_campaigns', methods=['POST'])
def compare_campaigns():
    data = request.form
    channel_id = data.get('channel_id')
    parameters = data.get('text')
    params = parameters.split("-")
    compare(service_sheets, channel_id, params)

    return Response(), 200

if __name__ == "__main__":
    app.run(debug=True)