from functools import update_wrapper
import pandas as pd
import dateparser

from datetime import datetime, timedelta

from pandas.core.frame import DataFrame
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

    return doctor_rota.drop(doctor_rota.query('start_time == end_time').index)


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


def firstRowAsColumn(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.iloc[0]
    df = df[1:]
    return df


def adhocTemplateMatching(rota_path: str, start_date: datetime) -> pd.DataFrame:
    weeks = [start_date + timedelta(days=7 * i) for i in range(0, 18)]
    first_week = [start_date + timedelta(days=i) for i in range(0, 7)]
    xls = pd.ExcelFile(rota_path)
    rota = pd.read_excel(xls, "Sheet1").dropna(how="all")[:19]
    rota["Week Commencing"] = weeks
    rota = rota.set_index("Week Commencing").astype(int)

    shifts = pd.read_excel(xls, "Sheet2")
    shifts.columns = map(lambda x: x.strip(), shifts.columns)
    shifts.rename(columns={"": "shift"}, inplace=True)
    shifts = shifts.set_index(["shift"])
    shifts.columns = first_week

    shifts = [
        pd.DataFrame(
            {
                shift: {
                    day: [
                        doc
                        for doctor in doctors.split("\n")
                        if (doc := doctor.replace("DOCTOR", "").strip())
                    ]
                    for day, doctors in value.items()
                }
                for shift, value in shifts.T.to_dict().items()
            }
        ).explode(shift_name)[shift_name]
        for shift_name in shifts.index
    ]

    data = {str(i): {day: None for day in first_week} for i in range(1, 18)}
    for shift_series in shifts:
        for day, row in shift_series.to_frame().iterrows():
            doctor = row.iloc[0]
            if doctor in data:
                data[doctor][day.to_pydatetime()] = shift_series.name

    rolling_data = []
    columns_map = {str(i): str(i) for i in range(1, 18)}
    for week_num in range(20):
        updated_data = pd.DataFrame(data).rename(columns=columns_map)
        updated_data.index = map(
            lambda x: x + timedelta(days=7 * week_num), updated_data.index
        )
        rolling_data.append(updated_data)
        columns_map = {
            doc: str(num) if (num := (int(i) - 1) % 17) != 0 else "17"
            for doc, i in columns_map.items()
        }
    
    final = (
        pd.concat(rolling_data)
        .fillna("")
        # * replace numbers with doctors names
        .rename(columns={str(v): k for k, v in rota.iloc[0].to_dict().items()})
    )
    final.index.name = "Date"
    return final 


if __name__ == "__main__":

    from rich import print
    from time import sleep

    # rota_path = "rotas/Ulster Rota Dec 20.xlsx"
    rota_path = "rotas/SHO Rolling rota April 2021.xlsx"
    # rota_path = "rota April 2021.xlsx"
    start_date = datetime(2021, 4, 5)

    rota = gCalRota(
        adhocTemplateMatching(rota_path, start_date),
        {
            "08:00-18:00": "08:00-18:00",
            "12:00-22:00": "12:00-22:00",
            "14:00-00:00": "14:00-00:00",
            "22:00-08:00": "22:00-08:00",
        },
        doctor_name="Laura-Anne Marmion",
    )
    service_client = calendarServiceClient()
    calendar_id = createCalendar(service_client, "Laura's Rota")
    for _, row in rota.iterrows():
        if row["summary"]:
            event = GoogleEvent(**row.to_dict())
            createEvent(service_client, event, calendar_id)
            sleep(0.1)
