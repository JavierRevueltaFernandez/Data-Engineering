import os
import re
from typing import Optional, List, Tuple

import pandas as pd
import matplotlib.pyplot as plt

DOWNLOAD_DIR = "downloads"
PROCESSED_DIR = "processed"

# Common duration column names across Divvy historical files (vary by year/quarter)
DURATION_CANDIDATES = [
    "tripduration",
    "trip_duration",
    "Trip Duration",
    "01 - Rental Details Rental Duration In Seconds",
    "Rental Duration In Seconds",
    "01 - Rental Details Duration In Seconds Uncapped",
    "Rental Details Duration In Seconds Uncapped",
    "ride_length",  # some newer Divvy formats use HH:MM:SS
]

# ---------- Helpers ----------
def verify_directory(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)

def list_csv_files(folder: str) -> List[str]:
    return sorted(
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith(".csv")
    )

def extract_quarter_from_filename(filename: str) -> Optional[str]:
    m = re.search(r"(\d{4})_Q([1-4])", filename)
    if not m:
        return None
    return f"{m.group(1)}_Q{m.group(2)}"

def find_duration_column(columns: List[str]) -> Optional[str]:
    # Exact match first
    for c in DURATION_CANDIDATES:
        if c in columns:
            return c

    # Fallback: try to find something that looks like duration in seconds
    lowered = {c.lower(): c for c in columns}
    for key in lowered:
        if "duration" in key and "second" in key:
            return lowered[key]

    for key in lowered:
        if "trip" in key and "duration" in key:
            return lowered[key]

    return None

def coerce_duration_to_seconds(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().mean() > 0.90:
        return numeric

    as_time = pd.to_timedelta(series, errors="coerce")
    if as_time.notna().mean() > 0.50:
        return as_time.dt.total_seconds()

    return numeric

# ---------- Question e) Null report ----------
def null_report(csv_path: str) -> None:
    df = pd.read_csv(csv_path)

    print("\n" + "=" * 80)
    print(f"File: {os.path.basename(csv_path)} | Rows: {len(df):,} | Cols: {df.shape[1]}")
    print("-" * 80)

    null_counts = df.isna().sum().sort_values(ascending=False)
    print("Null values per column:")
    any_nulls = False
    for col, cnt in null_counts.items():
        if cnt > 0:
            any_nulls = True
            pct = (cnt / len(df)) * 100
            print(f"  - {col}: {cnt:,} ({pct:.2f}%)")

    if not any_nulls:
        print("  (No null values detected)")

    # Focus on demographic fields (common in Divvy datasets)
    demo_cols = ["gender", "birthyear", "Member Gender", "05 - Member Details Member Birthday Year"]
    for col in demo_cols:
        if col in df.columns:
            cnt = int(df[col].isna().sum())
            pct = (cnt / len(df)) * 100
            print(f"\nDemographics -> {col}: {cnt:,} missing ({pct:.2f}%)")

# ---------- Question 4) Mean trip time ----------
def mean_trip_time_by_file(csv_path: str) -> dict:
    filename = os.path.basename(csv_path)
    quarter = extract_quarter_from_filename(filename)

    df = pd.read_csv(csv_path)
    duration_col = find_duration_column(list(df.columns))

    # If no duration column, try compute from timestamps if possible
    if duration_col is None:
        # try common timestamp columns
        start_col = next((c for c in df.columns if c.lower() in ["start_time", "starttime", "started_at", "01 - rental details local start time".lower()]), None)
        end_col = next((c for c in df.columns if c.lower() in ["end_time", "stoptime", "ended_at", "01 - rental details local end time".lower()]), None)

        if start_col and end_col:
            start_dt = pd.to_datetime(df[start_col], errors="coerce")
            end_dt = pd.to_datetime(df[end_col], errors="coerce")
            duration_seconds = (end_dt - start_dt).dt.total_seconds()
            duration_col = "computed_from_timestamps"
        else:
            return {
                "quarter": quarter,
                "file": filename,
                "trips_count": len(df),
                "mean_seconds": None,
                "duration_col": None,
                "note": "No duration column (and cannot compute from timestamps)",
            }
    else:
        duration_seconds = coerce_duration_to_seconds(df[duration_col])

    valid = duration_seconds[(duration_seconds.notna()) & (duration_seconds > 0)]

    if len(valid) == 0:
        return {
            "quarter": quarter,
            "file": filename,
            "trips_count": len(df),
            "mean_seconds": None,
            "duration_col": duration_col,
            "note": "No valid duration values",
        }

    return {
        "quarter": quarter,
        "file": filename,
        "trips_count": len(df),
        "mean_seconds": float(valid.mean()),
        "duration_col": duration_col,
        "note": "",
    }

def compute_mean_trip_time_by_quarter(csv_files: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows = [mean_trip_time_by_file(path) for path in csv_files]
    by_file = pd.DataFrame(rows)
    by_file = by_file[by_file["quarter"].notna()].copy()

    # Weighted mean by trip count (robust if multiple files per quarter)
    quarter_rows = []
    for quarter, g in by_file.groupby("quarter"):
        g2 = g.dropna(subset=["mean_seconds"])
        if g2.empty:
            mean_sec = None
        else:
            w = g2["trips_count"].astype(float)
            mean_sec = float((g2["mean_seconds"] * w).sum() / w.sum())

        quarter_rows.append({
            "quarter": quarter,
            "mean_seconds": mean_sec,
            "mean_minutes": (mean_sec / 60.0) if mean_sec is not None else None,
            "files": ", ".join(sorted(g["file"].tolist())),
            "duration_columns": ", ".join(sorted(set([c for c in g["duration_col"].dropna().tolist()]))),
        })

    by_quarter = pd.DataFrame(quarter_rows)

    def sort_key(q: str) -> tuple[int, int]:
        y, qq = q.split("_Q")
        return (int(y), int(qq))

    by_quarter = by_quarter.sort_values(by="quarter", key=lambda s: s.map(sort_key)).reset_index(drop=True)
    return by_quarter, by_file

# ---------- Question 5) Extra analysis ----------
def _find_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None

def _extract_quarter_from_path(path: str) -> str:
    name = os.path.basename(path)
    m = re.search(r"(20\d{2}_Q[1-4])", name)
    return m.group(1) if m else "unknown"

def standardize_divvy_columns(df: pd.DataFrame) -> pd.DataFrame:
    start_candidates = ["start_time", "starttime", "01 - Rental Details Local Start Time", "started_at"]
    end_candidates = ["end_time", "stoptime", "01 - Rental Details Local End Time", "ended_at"]
    duration_candidates = [
        "tripduration",
        "01 - Rental Details Duration In Seconds Uncapped",
        "01 - Rental Details Rental Duration In Seconds",
        "ride_length",
    ]
    usertype_candidates = ["usertype", "User Type", "member_casual", "Member Type"]
    start_station_candidates = ["from_station_name", "start_station_name", "FROM STATION NAME"]
    end_station_candidates = ["to_station_name", "end_station_name", "TO STATION NAME"]

    start_col = _find_first_existing_column(df, start_candidates)
    end_col = _find_first_existing_column(df, end_candidates)
    duration_col = _find_first_existing_column(df, duration_candidates)
    usertype_col = _find_first_existing_column(df, usertype_candidates)
    start_station_col = _find_first_existing_column(df, start_station_candidates)
    end_station_col = _find_first_existing_column(df, end_station_candidates)

    out = df.copy()
    out["start_dt"] = pd.to_datetime(out[start_col], errors="coerce") if start_col else pd.NaT
    out["end_dt"] = pd.to_datetime(out[end_col], errors="coerce") if end_col else pd.NaT

    if duration_col:
        if out[duration_col].dtype == "object":
            as_td = pd.to_timedelta(out[duration_col], errors="coerce")
            out["duration_sec"] = as_td.dt.total_seconds()
            if out["duration_sec"].isna().mean() > 0.95:
                out["duration_sec"] = pd.to_numeric(out[duration_col], errors="coerce")
        else:
            out["duration_sec"] = pd.to_numeric(out[duration_col], errors="coerce")
    else:
        out["duration_sec"] = (out["end_dt"] - out["start_dt"]).dt.total_seconds()

    out["usertype_std"] = out[usertype_col].astype(str) if usertype_col else "unknown"
    out["start_station_std"] = out[start_station_col].astype(str) if start_station_col else "unknown"
    out["end_station_std"] = out[end_station_col].astype(str) if end_station_col else "unknown"
    return out

def run_extra_analysis(csv_files: List[str]) -> None:
    verify_directory(PROCESSED_DIR)

    all_rows = []
    for path in csv_files:
        df = pd.read_csv(path)
        df_std = standardize_divvy_columns(df)
        df_std["quarter"] = _extract_quarter_from_path(path)
        df_std["file"] = os.path.basename(path)
        all_rows.append(df_std[[
            "quarter", "file", "start_dt", "end_dt",
            "duration_sec", "usertype_std",
            "start_station_std", "end_station_std"
        ]])

    data = pd.concat(all_rows, ignore_index=True)

    # Basic validity filter
    data = data.dropna(subset=["start_dt"])
    data = data[data["duration_sec"].notna()]
    data = data[data["duration_sec"] > 0]

    # A) Trips by hour and user type
    data["hour"] = data["start_dt"].dt.hour
    trips_by_hour = (
        data.groupby(["hour", "usertype_std"])
            .size()
            .reset_index(name="trip_count")
            .sort_values(["hour", "usertype_std"])
    )
    out_csv = os.path.join(PROCESSED_DIR, "trips_by_hour_usertype.csv")
    trips_by_hour.to_csv(out_csv, index=False)
    print(f"Saved: {out_csv}")

    pivot_hour = trips_by_hour.pivot(index="hour", columns="usertype_std", values="trip_count").fillna(0)
    plt.figure()
    pivot_hour.plot()
    plt.title("Trips by Hour of Day (by user type)")
    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Trips")
    out_png = os.path.join(PROCESSED_DIR, "trips_by_hour_usertype.png")
    plt.savefig(out_png, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out_png}")

    # B) Top 10 start stations
    top_start = (
        data[data["start_station_std"] != "unknown"]
        .groupby("start_station_std")
        .size()
        .reset_index(name="trip_count")
        .sort_values("trip_count", ascending=False)
        .head(10)
    )
    out_csv = os.path.join(PROCESSED_DIR, "top_start_stations.csv")
    top_start.to_csv(out_csv, index=False)
    print(f"Saved: {out_csv}")

    plt.figure()
    plt.barh(top_start["start_station_std"][::-1], top_start["trip_count"][::-1])
    plt.title("Top 10 Start Stations (by number of trips)")
    plt.xlabel("Number of Trips")
    plt.ylabel("Start Station")
    out_png = os.path.join(PROCESSED_DIR, "top_start_stations.png")
    plt.savefig(out_png, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out_png}")

    # C) Average duration by user type + by quarter
    duration_by_usertype = (
        data.groupby("usertype_std")["duration_sec"]
            .mean()
            .reset_index(name="mean_duration_sec")
            .sort_values("mean_duration_sec", ascending=False)
    )
    duration_by_usertype["mean_duration_min"] = duration_by_usertype["mean_duration_sec"] / 60.0
    out_csv = os.path.join(PROCESSED_DIR, "duration_by_usertype.csv")
    duration_by_usertype.to_csv(out_csv, index=False)
    print(f"Saved: {out_csv}")

    duration_by_quarter_usertype = (
        data.groupby(["quarter", "usertype_std"])["duration_sec"]
            .mean()
            .reset_index(name="mean_duration_sec")
            .sort_values(["quarter", "usertype_std"])
    )
    duration_by_quarter_usertype["mean_duration_min"] = duration_by_quarter_usertype["mean_duration_sec"] / 60.0
    out_csv = os.path.join(PROCESSED_DIR, "duration_by_quarter_usertype.csv")
    duration_by_quarter_usertype.to_csv(out_csv, index=False)
    print(f"Saved: {out_csv}")

    print("\nEXTRA ANALYSIS DONE (Question 5). Outputs saved in 'processed/'.")

# ---------- Main ----------
def main() -> None:
    if not os.path.exists(DOWNLOAD_DIR):
        raise FileNotFoundError(f"'{DOWNLOAD_DIR}' folder not found. Run downloader.py first.")

    csv_files = list_csv_files(DOWNLOAD_DIR)
    if not csv_files:
        print("No CSV files found in downloads.")
        return

    # e) Null analysis
    print("\n" + "#" * 80)
    print("NULL VALUE REPORT (for Exercise 1 - Question e)")
    print("#" * 80)
    for csv_path in csv_files:
        null_report(csv_path)

    # 4) Mean trip time per quarter
    print("\n" + "#" * 80)
    print("MEAN TRIP TIME BY QUARTER (for Exercise 1 - Question 4)")
    print("#" * 80)

    verify_directory(PROCESSED_DIR)
    by_quarter, by_file = compute_mean_trip_time_by_quarter(csv_files)

    out_quarter = os.path.join(PROCESSED_DIR, "mean_trip_time_by_quarter.csv")
    out_file = os.path.join(PROCESSED_DIR, "mean_trip_time_by_file.csv")
    by_quarter.to_csv(out_quarter, index=False)
    by_file.to_csv(out_file, index=False)

    print(f"Saved: {out_quarter}")
    print(f"Saved: {out_file}\n")
    print(by_quarter.to_string(index=False))

    # 5) Extra analysis
    print("\n" + "#" * 80)
    print("EXTRA ANALYSIS (for Exercise 1 - Question 5)")
    print("#" * 80)
    run_extra_analysis(csv_files)

if __name__ == "__main__":
    main()
