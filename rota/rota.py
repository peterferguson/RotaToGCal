# -*- coding: utf-8 -*-
import pandas as pd
import dateparser

from datetime import datetime, timedelta
from utils import splitDurations, formatCoworkerDescription, timedeltaString
from Gcal import calendarServiceClient, createCalendar, createEvent, GoogleEvent

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
        rota[name_on_calendar].to_frame().rename(columns={name_on_calendar: "summary"})
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

    return doctor_rota


# TODO: This is the function which must be edited for each new rota
def orientateRota(rota_path: str, rota_start_date: datetime):
    rota = pd.read_excel(rota_path)

    # Select table area
    rota = rota[5:71].reset_index(drop=True)

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
                "Faryal Peerally(IMT1)",
                "Dean Carolan (IMT1)",
                "Tom Carson (F2)",
                "Handi Chishti (IMT1)",
                "Kelly McGuigan(GP)",
                "Dawn Pollock Sheals(F2)/Samantha Banford(IMT1)",
                "Kathryn Robinson(IMT2)",
                "Aisling Murray(Loc)",
                "Amy Wood(GP)",
                "Hannah Stewart (IMT1)",
                "Caoimhe Cooke(F2)",
                "Alasdair Trimble (GP)",
                "Ryan Doherty (F2)",
                "Laura Marmion (F2)",
                "Leslie Bailie(GP)",
                "Sajida Mushtaq (GP)",
                "Gary Roulston (IMT1)",
                "Opeyemi Makanjoula(IMT1)",
                "Alison Thompson (IMT1)",
                "Ashley Mulholland (GP)",
                "Chandrika Kanjee(F2)",
                "Razan Elnaim (F2)",
                "Alice Cullen (Loc)",
                "Craig meek (Loc)",
                "Peter Neeson(F2)",
                "Catriona Gormley (F2)",
                "Vishu Kuma Valladpareddy(IMT2)",
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


if __name__ == "__main__":

    # rota_path = "rotas/Ulster Rota Dec 20.xlsx"
    rota_path = "rotas/Ulster Rota Feb 21.xlsx"

    rota = gCalRota(
        orientateRota(rota_path, datetime(2021, 2, 3)),
        {
            "Day shift": "09.00-17.00",
            "Long day shift": "09.00-22.00",
            "Night shift": "21.00-10.00",
            "*Day shift*": "09.00-17.00",
        },
        doctor_name="Laura Marmion",
    )

    service_client = calendarServiceClient()
    calendar_id = createCalendar(service_client, "Ulster Cardiology Rota")
    for _, row in rota.iterrows():
        if row["summary"]:
            event = GoogleEvent(**row.to_dict())
            createEvent(service_client, event, calendar_id)
