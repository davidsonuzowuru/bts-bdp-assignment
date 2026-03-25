import sqlite3

from fastapi import APIRouter, status
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

DB_PATH = settings.db_url.replace("sqlite:///", "")

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)


class AircraftReturn(BaseModel):
    icao: str
    registration: str | None
    type: str | None
    owner: str | None
    manufacturer: str | None
    model: str | None


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: float | None


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    offset = page * num_results
    rows = con.execute(
        "SELECT icao, registration, type, owner, manufacturer, model "
        "FROM s8_aircraft ORDER BY icao ASC LIMIT ? OFFSET ?",
        (num_results, offset),
    ).fetchall()
    con.close()
    return [AircraftReturn(**dict(row)) for row in rows]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    # Count observations for this aircraft on this day
    row = con.execute(
        "SELECT COUNT(*) as cnt, MAX(type) as type FROM s8_observations WHERE icao = ? AND day = ?",
        (icao, day),
    ).fetchone()

    count = row["cnt"] if row else 0
    aircraft_type = row["type"] if row else None

    hours_flown = (count * 5) / 3600

    co2 = None
    if aircraft_type:
        fuel_row = con.execute(
            "SELECT galph FROM s8_fuel_rates WHERE type = ?",
            (aircraft_type,),
        ).fetchone()
        if fuel_row and fuel_row["galph"] is not None:
            fuel_used_kg = hours_flown * float(fuel_row["galph"]) * 3.04
            co2 = (fuel_used_kg * 3.15) / 907.185

    con.close()
    return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=co2)