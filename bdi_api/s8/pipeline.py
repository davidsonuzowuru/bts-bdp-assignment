import json
import sqlite3
from io import StringIO

import pandas as pd
import requests

from bdi_api.settings import Settings

settings = Settings()

TRACKING_BASE = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
AIRCRAFT_CSV_URL = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
FUEL_JSON_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
DB_PATH = settings.db_url.replace("sqlite:///", "")

TRACKING_FILES = [f"{str(h).zfill(2)}0000Z.json.gz" for h in range(0, 24, 4)]


def get_db():
    return sqlite3.connect(DB_PATH)


def download_tracking_files():
    records = []
    files_ok = 0
    for fname in TRACKING_FILES:
        url = TRACKING_BASE + fname
        try:
            r = requests.get(url, timeout=60)
            if r.status_code != 200:
                print(f"  Skip {fname}: status {r.status_code}")
                continue
            data = r.json()
            aircraft_list = data.get("aircraft", [])
            for ac in aircraft_list:
                records.append({
                    "icao": (ac.get("hex") or "").lower().strip(),
                    "type": (ac.get("t") or "").strip(),
                    "reg":  (ac.get("r") or "").strip(),
                    "day":  "2023-11-01",
                })
            files_ok += 1
            print(f"  OK: {fname} ({len(aircraft_list)} aircraft)")
        except Exception as e:
            print(f"  Skip {fname}: {e}")
    print(f"  Total observations: {len(records)} from {files_ok} files")
    return records


def download_aircraft_db():
    print("  Downloading aircraft database CSV ...")
    r = requests.get(AIRCRAFT_CSV_URL, timeout=120)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text), dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]

    # Rename only the columns we need, ignore the rest
    rename = {}
    if "icao24" in df.columns:
        rename["icao24"] = "icao"
    if "registration" in df.columns:
        rename["registration"] = "registration"
    if "manufacturername" in df.columns:
        rename["manufacturername"] = "manufacturer"
    if "model" in df.columns:
        rename["model"] = "model"
    if "owner" in df.columns:
        rename["owner"] = "owner"

    df = df.rename(columns=rename)

    # Keep only the columns we need (avoids duplicates from other cols)
    keep = ["icao", "registration", "manufacturer", "model", "owner"]
    for col in keep:
        if col not in df.columns:
            df[col] = None

    df = df[keep].copy()
    df["icao"] = df["icao"].str.lower().str.strip()
    return df.drop_duplicates("icao")


def download_fuel_rates():
    print("  Downloading fuel rates ...")
    r = requests.get(FUEL_JSON_URL, timeout=30)
    r.raise_for_status()
    return r.json()


def run():
    print("=== S8 Pipeline starting ===")

    print("\n[1/4] Downloading tracking data ...")
    observations = download_tracking_files()
    if not observations:
        print("ERROR: No tracking data. Aborting.")
        return

    print("\n[2/4] Downloading reference data ...")
    try:
        aircraft_df = download_aircraft_db()
        print(f"  Aircraft DB rows: {len(aircraft_df)}")
    except Exception as e:
        print(f"  Warning: aircraft DB failed ({e})")
        aircraft_df = pd.DataFrame(columns=["icao", "registration", "owner", "manufacturer", "model"])

    fuel_rates = {}
    try:
        fuel_rates = download_fuel_rates()
        print(f"  Fuel rate entries: {len(fuel_rates)}")
    except Exception as e:
        print(f"  Warning: fuel rates failed ({e})")

    print("\n[3/4] Enriching data ...")
    obs_df = pd.DataFrame(observations)
    enriched = obs_df.merge(aircraft_df, on="icao", how="left")
    if "reg" in enriched.columns:
        mask = enriched["reg"].notna() & (enriched["reg"] != "")
        enriched["registration"] = enriched["reg"].where(mask, enriched.get("registration"))
        enriched.drop(columns=["reg"], inplace=True)
    print(f"  Enriched rows: {len(enriched)}")

    print("\n[4/4] Writing to database ...")
    con = get_db()

    aircraft_silver = (
        enriched.groupby("icao", as_index=False)
        .agg({
            "registration": "first",
            "type": "first",
            "owner": "first",
            "manufacturer": "first",
            "model": "first",
        })
        .sort_values("icao")
    )
    aircraft_silver.to_sql("s8_aircraft", con, if_exists="replace", index=False)
    print(f"  s8_aircraft rows: {len(aircraft_silver)}")

    enriched[["icao", "type", "day"]].to_sql("s8_observations", con, if_exists="replace", index=False)
    print(f"  s8_observations rows: {len(enriched)}")

    fuel_df = pd.DataFrame([
        {"type": k, "galph": v.get("galph") if isinstance(v, dict) else v}
        for k, v in fuel_rates.items()
    ])
    fuel_df.to_sql("s8_fuel_rates", con, if_exists="replace", index=False)
    print(f"  s8_fuel_rates rows: {len(fuel_df)}")

    con.close()
    print("\n=== Pipeline complete! ===")


if __name__ == "__main__":
    run()