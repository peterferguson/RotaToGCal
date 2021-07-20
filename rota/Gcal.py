from json import load
import pickle
import os
import json
import shutil

from datetime import datetime
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pydantic import BaseModel
from typing import List, Optional
from contextlib import contextmanager
from dotenv import load_dotenv


load_dotenv()


class GoogleEvent(BaseModel):
    summary: str
    description: str
    color_id: Optional[int] = None
    start_time: datetime
    end_time: datetime
    time_zone: str = "Europe/London"


@contextmanager
def credentialsJson(name: str):
    if name:
        temp_filename = name.replace(".json", "_temp.json")
        shutil.copyfile(name, temp_filename)
        with open(name, "r") as creds_format:
            f = json.load(creds_format)
        with open(temp_filename, "w") as json_file:
            json.dump(
                {
                    "web": {
                        "client_id": os.getenv("client_id"),
                        "project_id": f["web"]["project_id"],
                        "auth_uri": f["web"]["auth_uri"],
                        "token_uri": f["web"]["token_uri"],
                        "auth_provider_x509_cert_url": f["web"][
                            "auth_provider_x509_cert_url"
                        ],
                        "client_secret": os.getenv("client_secret"),
                    }
                },
                json_file,
            )
        try:
            yield f
        finally:
            os.remove(temp_filename)


def calendarServiceClient(
    scopes: List[str] = ["https://www.googleapis.com/auth/calendar"],
    creds_path: str = "credentials/credentials.json",
):

    creds = None
    if os.path.exists("credentials/token.pickle"):
        with open("credentials/token.pickle", "rb") as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            with credentialsJson(creds_path) as cred:
                flow = InstalledAppFlow.from_client_secrets_file(
                    creds_path.replace(".json", "_temp.json"), scopes
                )
                creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open("credentials/token.pickle", "wb") as token:
            pickle.dump(creds, token)

    return build("calendar", "v3", credentials=creds)


def createCalendar(service, summary):
    new_calendar = {"summary": summary, "timeZone": "Europe/London"}
    new_calendar_id = None
    page_token = None
    while True:
        calendar_dict = service.calendarList().list(pageToken=page_token).execute()
        if summary not in (calendar["summary"] for calendar in calendar_dict["items"]):
            new_calendar = service.calendars().insert(body=new_calendar).execute()
            new_calendar_id = new_calendar["id"]
            print("New calendar {} has been created!".format(new_calendar["summary"]))
        else:
            new_calendar_id = calendar_dict["items"][
                [calendar["summary"] for calendar in calendar_dict["items"]].index(
                    summary
                )
            ]["id"]

        page_token = calendar_dict.get("nextPageToken")
        if not page_token:
            break
        print(service.calendarList().list(pageToken=page_token).execute())
    return new_calendar_id


def createEvent(service_client, event: GoogleEvent, calendar_id: str):
    event = {
        "summary": event.summary,
        "description": event.description,
        # 'colorId': 9, #Blueberry
        "start": {
            "dateTime": event.start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": event.time_zone,
        },
        "end": {
            "dateTime": event.end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": event.time_zone,
        },
    }
    service_client.events().insert(calendarId=calendar_id, body=event).execute()


if __name__ == "__main__":
    service_client = calendarServiceClient()
    calendar_id = createCalendar(service_client, "Ulster Cardiology Rota")
