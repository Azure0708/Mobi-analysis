import pandas as pd
import os

# ─────────────────────────────────────────────
# SETTINGS — update filenames if needed
# ─────────────────────────────────────────────
MOBI_FILE     = "Mobi_System_Data_2025-09.csv"
WEATHER_FILE  = "weather.csv"
BIKEWAYS_FILE = "bikeways.csv"
# ─────────────────────────────────────────────


# ══════════════════════════════════════════════
# 1. MOBI TRIP DATA
# ══════════════════════════════════════════════
print("=" * 50)
print("LOADING MOBI TRIP DATA...")
print("=" * 50)

df = pd.read_csv(MOBI_FILE)
print(f"Columns found: {list(df.columns)}")
print(f"Total rows: {len(df)}\n")

# Standardise column names
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# Parse datetime columns
df["departure"] = pd.to_datetime(df["departure"], errors="coerce")
df["return"]    = pd.to_datetime(df["return"],    errors="coerce")

# Derive useful columns
df["trip_date"]   = df["departure"].dt.date
df["hour_of_day"] = df["departure"].dt.hour
df["day_of_week"] = df["departure"].dt.day_name()
df["month"]       = df["departure"].dt.to_period("M").astype(str)

# Convert units
if "duration_(sec.)" in df.columns:
    df["duration_min"] = (df["duration_(sec.)"] / 60).round(2)
elif "duration_(seconds)" in df.columns:
    df["duration_min"] = (df["duration_(seconds)"] / 60).round(2)
elif "duration" in df.columns:
    df["duration_min"] = (df["duration"] / 60).round(2)

if "covered_distance_(m)" in df.columns:
    df["distance_km"] = (df["covered_distance_(m)"] / 1000).round(3)
elif "covered_distance" in df.columns:
    df["distance_km"] = (df["covered_distance"] / 1000).round(3)

# Bike type flag
if "electric_bike" in df.columns:
    df["bike_type"] = df["electric_bike"].apply(
        lambda x: "E-bike" if pd.notna(x) and str(x).strip() not in ["", "nan", "False", "false"] else "Classic"
    )
elif "bike" in df.columns:
    df["bike_type"] = "Classic"

# Drop rows with missing station names
before = len(df)
df = df.dropna(subset=["departure_station", "return_station"])
print(f"Removed {before - len(df)} rows with missing station names.")

df.to_csv("trips_clean.csv", index=False)
print(f"Saved: trips_clean.csv  ({len(df)} rows)\n")


# ══════════════════════════════════════════════
# 2. WEATHER DATA (separate clean file)
# ══════════════════════════════════════════════
print("=" * 50)
print("LOADING WEATHER DATA...")
print("=" * 50)

if os.path.exists(WEATHER_FILE):
    weather = pd.read_csv(WEATHER_FILE)
    weather.columns = weather.columns.str.strip().str.lower().str.replace(" ", "_")
    print(f"Columns found: {list(weather.columns)}")
    print(f"Total rows: {len(weather)}")

    keep_cols = []
    for col in ["name", "datetime", "tempmax", "tempmin", "temp", "feelslike",
                "humidity", "precip", "precipprob", "preciptype",
                "windspeed", "cloudcover", "conditions", "description"]:
        if col in weather.columns:
            keep_cols.append(col)

    weather = weather[keep_cols]
    weather.to_csv("weather_clean.csv", index=False)
    print(f"Saved: weather_clean.csv  ({len(weather)} rows)\n")
else:
    print("Weather file not found — skipping.\n")


# ══════════════════════════════════════════════
# 3. BIKEWAYS DATA (separate clean file)
# ══════════════════════════════════════════════
print("=" * 50)
print("LOADING BIKEWAYS DATA...")
print("=" * 50)

if os.path.exists(BIKEWAYS_FILE):
    # Try semicolon first (Vancouver Open Data default)
    bk = pd.read_csv(BIKEWAYS_FILE, sep=";")

    # If only 1 column detected, try comma
    if len(bk.columns) == 1:
        print("Trying comma separator instead...")
        bk = pd.read_csv(BIKEWAYS_FILE, sep=",")

    print(f"Columns found: {list(bk.columns)}")
    print(f"Total rows: {len(bk)}")

    bk.columns = bk.columns.str.strip().str.lower().str.replace(" ", "_")

    # Split geo_point_2d into lat/lon
    if "geo_point_2d" in bk.columns:
        print("Splitting geo_point_2d into latitude and longitude...")
        bk[["latitude", "longitude"]] = (
            bk["geo_point_2d"]
            .str.split(",", expand=True)
            .apply(pd.to_numeric, errors="coerce")
        )
        bk = bk.drop(columns=["geo_point_2d"])

    # Keep useful columns
    keep = [c for c in [
        "objectid", "bike_route_name", "street_name", "bikeway_type",
        "subtype", "status", "surface_type", "snow_removal",
        "segment_length", "latitude", "longitude"
    ] if c in bk.columns]

    if keep:
        bk = bk[keep]

    bk.to_csv("bikeways_clean.csv", index=False)
    print(f"Saved: bikeways_clean.csv  ({len(bk)} rows)\n")
else:
    print("Bikeways file not found — skipping.\n")


print("=" * 50)
print("ALL DONE! 3 separate files ready for Tableau:")
print("  - trips_clean.csv   (join key: trip_date)")
print("  - weather_clean.csv (join key: datetime)")
print("  - bikeways_clean.csv")
print("=" * 50)
