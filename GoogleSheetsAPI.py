from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import requests
import csv
from io import StringIO


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1Z5flDzeF5CmraMvT0DV7pZW-K6lzkKTjoABjwUA0Bnk'
RANGE_NAME = 'Sheet1!A1:B'
value_input_option ='RAW'

values = []
body = {'values': values}

def get_chi_permits():
    url='https://data.cityofchicago.org/resource/ydr8-5enu.csv?issue_date=2019-08-08T00:00:00.000'
    response = requests.request('GET', url)
    response_string = response.text
    parsed_csv = StringIO(response_string)
    reader = csv.reader(parsed_csv, delimiter=',')
    # json_string = response.text
    # parsed_json = json.loads(json_string)

    # return parsed_json
    return reader

for row in get_chi_permits():
    body['values'].append(row)

body = {
    'values': values
}

def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    clearfile = service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()

    result = service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
        valueInputOption=value_input_option, body=body).execute()
    result.get
    #print('{0} cells updated.'.format(result.get('appendedCells')))


if __name__ == '__main__':
    main()