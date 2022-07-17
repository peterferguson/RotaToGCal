from datetime import datetime, timedelta

import dateparser
import numpy as np
import pandas as pd
from numpy import column_stack
from pandas.core.frame import DataFrame

from Gcal import GoogleEvent, calendarServiceClient, createCalendar, createEvent
from utils import (
    formatCoworkerDescription,
    splitDurations,
    timedeltaString,
    splitDataFrameOnEmptyRows,
)

"""# Doctor Specific Rota"""


def gCalRota(
    rota: pd.DataFrame, shift_map: dict, doctor_name: str = "Laura Marmion"
) -> pd.DataFrame:
    """
    params
        rota (pd.DataFrame):    The datetime index dataframe with doctors names
                                as columns and shift entries
        shift_map (dict):       A key-vale pair set with key being shift name
                                and value shift length
    returns (pd.DataFrame):     Final rota must have columns
                                    - start_time
                                    - end_time
                                    - Date
                                    - summary
                                    - description (who else is working)
    """
    name_on_calendar = next(name for name in rota.columns if doctor_name in name)

    doctor_rota = (
        rota[name_on_calendar]
        .to_frame()
        .dropna()
        .rename(columns={name_on_calendar: "summary"})
    )

    doctor_rota["start_time"] = doctor_rota["summary"].apply(
        lambda x: splitDurations(shift_map[x]) if x else x
    )
    doctor_rota["end_time"] = doctor_rota["summary"].apply(
        lambda x: splitDurations(shift_map[x], False) if x else x
    )
    doctor_rota["description"] = doctor_rota.index
    doctor_rota["description"] = doctor_rota["description"].apply(
        lambda x: formatCoworkerDescription(rota.loc[x])
    )

    doctor_rota.reset_index(inplace=True)
    doctor_rota["start_time"] = doctor_rota["Date"] + doctor_rota["start_time"].apply(
        timedeltaString
    )
    doctor_rota["end_time"] = doctor_rota["Date"] + doctor_rota["end_time"].apply(
        timedeltaString
    )
    doctor_rota.loc[
        doctor_rota["end_time"].lt(doctor_rota["start_time"]), "end_time"
    ] += timedelta(days=1)

    return doctor_rota.drop(doctor_rota.query("start_time == end_time").index)


# TODO: This is the function which must be edited for each new rota
def orientateRota(rota_path: str, rota_start_date: datetime):
    rota = pd.read_excel(rota_path)

    # Data Cleaning
    rota.dropna(axis=0, how="all", inplace=True)
    rota.dropna(axis=1, how="all", inplace=True)
    rota = rota.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    rota = rota.fillna("").replace("OFF", "")

    # Add columns as doctor names
    doctor_dict = {
        str(i + 1): name
        for i, name in enumerate(
            [
                "C Rooney GP",
                "Bridin McKinney F2",
                "Michael McKee GP",
                "Clare Cunningham/ Emma McAuley ",
                "Emma Dougan/ Oliver Perks ",
                "Laura Marmion",
                "Kathryn Bryans/Cara Davidson",
                "Matthew Getty F2",
                "Molly Kerr GP",
                "Mostafa Salem/ Amy Smyth ",
                "Ismail AbdulAzeez",
                "Michael Cochrane/ Ciara Killoran",
                "Kathrine O'Boyle/ Susana Hall",
                "Anna McTear ",
                "Suzanne Rankin/ Maeve Corry",
                "Niamh Clayton",
                "William Rea/ Kathryn Graham",
            ]
        )
    }

    rota.rename(columns=rota.iloc[0].apply(str), inplace=True)
    rota.drop(rota.index[0], inplace=True)
    rota.rename(columns=doctor_dict, inplace=True)

    # datetime indexing
    start_year = rota_start_date.year
    rota.rename(columns={"": "Date"}, inplace=True)
    rota["Year"] = start_year
    rota["DateTime"] = rota["Date"].apply(dateparser.parse)

    year_change = rota[rota.DateTime.dt.month > rota.DateTime.dt.month.shift(-1)]

    if not year_change.empty:

        year_change_index = year_change.index[0]

        rota.loc[year_change_index:, "Year"] = rota.loc[year_change_index:, "Year"] + 1

        rota.Date = rota.Date + rota.Year.apply(lambda x: f" {x}")

    rota.Date = rota.Date.apply(dateparser.parse)

    rota.drop(["Year", "DateTime"], axis=1, inplace=True)

    rota.set_index(["Date"], inplace=True)

    shift_names = {
        "D": "Day shift",
        "L": "Long day shift",
        "N": "Night shift",
        "D*": "*Day shift*",
    }

    return rota.applymap(lambda x: shift_names[x] if x in shift_names else x)


def adhocTemplateMatching(
    rota_path: str, start_date: datetime, number_of_weeks: int = 0
) -> pd.DataFrame:
    with open(f"{rota_path}", "rb") as f:
        rota: pd.DataFrame = pd.read_excel(f)

    isSeparatedIntoWeeksWithDoctorColumns = True

    weeks: list[pd.DataFrame] = list(
        map(
            lambda week: week.T.dropna(how="all")
            if isSeparatedIntoWeeksWithDoctorColumns
            else week.dropna(how="all"),
            splitDataFrameOnEmptyRows(rota),
        )
    )

    weeks = list(
        map(
            lambda week: week.rename(columns=week.iloc[0]).iloc[1:].set_index("Doctor"),
            weeks,
        )
    )

    doctors = {doctor for week in weeks for doctor in week.columns}

    rotas: list = []
    for doctor in doctors:
        try:
            rotas.append(
                (pd.concat([week[doctor] for week in weeks if doctor in week]))
            )
        except:
            continue

    rota = pd.DataFrame(rotas).T
    rota = rota.fillna("").replace("OFF", "")

    rota.index.name = "Date"
    rota.columns = [str(col) for col in rota.columns]

    return rota


if __name__ == "__main__":

    from time import sleep

    from rich import print

    # rota_path = "rotas/Ulster Rota Dec 20.xlsx"
    # rota_path = "rotas/SHO Rolling rota April 2021.xlsx"
    # rota_path = "rotas/SHO Rolling rota Aug 2021"
    # rota_path = "rota April 2021.xlsx"
    rota_path = "rotas/Psych-Full-Shift-Aug-2022.xlsx"
    start_date = datetime(2022, 8, 2)

    rota = gCalRota(
        adhocTemplateMatching(rota_path, start_date),
        {
            "09.00-17.00": "09.00-17.00",
            "21.00-09.30": "21.00-09.30",
            "Long Day 1 09.00-21.30": "09.00-21.30",
            "Long Day 2 09.00-21.30": "09.00-21.30",
            "Half  09.00-13.00": "09.00-13.00",
        },
        # doctor_name="Laura Marmion",
    )

    service_client = calendarServiceClient()
    calendar_id = createCalendar(service_client, "Laura's Rota")
    for _, row in rota.iterrows():
        if row["summary"]:
            event = GoogleEvent(**row.to_dict())
            createEvent(service_client, event, calendar_id)
            sleep(0.1)
