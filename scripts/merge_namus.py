from pathlib import Path
import pandas as pd
import glob
import re

# --- Paths ---
BASE_DIR = Path(__file__).resolve().parent.parent  # repo root
RAW_DIR = BASE_DIR / "raw"       # your raw CSVs live here
CLEAN_DIR = BASE_DIR / "data" / "clean"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

MP_INPUTS = glob.glob(str(RAW_DIR / "missing_*_download_*.csv"))
UP_INPUTS = glob.glob(str(RAW_DIR / "unidentified_*_download_*.csv"))

print("MP_INPUTS:", MP_INPUTS)
print("UP_INPUTS:", UP_INPUTS)

# --- Canonical columns and aliases ---
COL_ALIASES = {
    "namus_number": [
        "NamUs Number","NamUs #","NamUs Case Number","NamUs Case #",
        "Case Number","Case #","Case Number(s)","Case ID","CaseID",
        "NamusID","NamUsID","UP Number","UP #","MP Number","MP #",
        "Case"  # <-- Unidentified exports
    ],
    "first_name": ["First Name","Forename","Given Name"],
    "middle_name": ["Middle Name","Middle"],
    "last_name": ["Last Name","Surname","Family Name"],
    "sex": ["Sex","Gender","Biological Sex","Sex at Birth","Sex Assigned at Birth"],
    "race": ["Race / Ethnicity","Race","Ethnicity"],
    "age_min": ["Min Age","Minimum Age","Age From","Age Minimum","Age From (Years)"],
    "age_max": ["Max Age","Maximum Age","Age To","Age Maximum","Age To (Years)"],
    "date_missing": ["Date Missing","Missing Date","Date Last Seen","Last Seen Date"],
    "date_found": ["Date Found","Found Date","Date Recovered","Recovery Date","DBF"],
    "city": ["City","City/Community"],
    "county": ["County","County/Parish","Parish"],
    "state": ["State","Province/State","Province"],
    "country": ["Country"],
    "latitude": ["Latitude","Lat"],
    "longitude": ["Longitude","Long","Lng"],
    "height_in": ["Height (in)","HeightInches","Height (inches)","Height Inches"],
    "weight_lb": ["Weight (lbs)","Weight","Weight (lb)","Weight Pounds"],
    "circumstances": ["Circumstances","Circumstance Details","Case Information","Comments","Narrative"],
    "link": ["URL","Case URL","NamUs URL","Link"],
}

DATE_COLS = ["date_missing","date_found"]

def _clean_header(s: str) -> str:
    return (s.replace("\ufeff", "")  # strip BOM
             .strip()
             .replace("  ", " "))

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [_clean_header(c) for c in df.columns]
    incoming_lower = {c.lower(): c for c in df.columns}
    colmap = {}
    for canon, aliases in COL_ALIASES.items():
        for a in aliases:
            if a.lower() in incoming_lower:
                colmap[incoming_lower[a.lower()]] = canon
                break
    df = df.rename(columns=colmap)
    # Ensure all canon cols exist
    for canon in COL_ALIASES.keys():
        if canon not in df.columns:
            df[canon] = pd.NA
    ordered = list(COL_ALIASES.keys()) + [c for c in df.columns if c not in COL_ALIASES]
    return df[ordered]

def normalize_values(df: pd.DataFrame, group_hint: str) -> pd.DataFrame:
    # ID normalization
    def norm_id(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return pd.NA
        s = str(x).strip().replace("\ufeff","")
        if not s:
            return pd.NA
        up = s.upper()
        if up.startswith("MP") or up.startswith("UP"):
            return re.sub(r"[^A-Z0-9]", "", up)
        digits = re.sub(r"\D","", up)
        if digits:
            prefix = "MP" if group_hint == "MP" else "UP"
            return prefix + digits
        return up
    df["namus_number"] = df["namus_number"].apply(norm_id)

    # Backfill ID from URL if present
    if "link" in df.columns:
        need_id = df["namus_number"].isna() | (df["namus_number"].astype(str).str.strip() == "")
        extracted = (df["link"].astype(str)
                       .str.extract(r'/(MP|UP)\s*([0-9]+)', expand=True)
                       .agg(lambda r: (r[0] + r[1]) if pd.notna(r[0]) and pd.notna(r[1]) else pd.NA, axis=1))
        df.loc[need_id & extracted.notna(), "namus_number"] = extracted

    # Sex
    if "sex" in df.columns:
        nonnull = df["sex"].notna()
        df.loc[nonnull, "sex"] = (df.loc[nonnull, "sex"].astype(str).str.strip().str.upper()
                                  .replace({"FEMALE":"F","MALE":"M","UNKNOWN":"U","UNSURE":"U"}))

    # State
    if "state" in df.columns:
        nonnull = df["state"].notna()
        df.loc[nonnull, "state"] = df.loc[nonnull, "state"].astype(str).str.strip().str.upper()

    # Dates
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Numerics
    for col in ["age_min","age_max","height_in","weight_lb","latitude","longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def read_csv(path: str) -> pd.DataFrame:
    for enc in ("utf-8-sig","utf-8","latin-1"):
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False,
                               na_values=["","NA","N/A"], encoding=enc, low_memory=False)
        except Exception:
            continue
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=["","NA","N/A"], low_memory=False)

def union_and_clean(paths, group_hint):
    frames = []
    for p in paths:
        df = read_csv(p)
        df = standardize_columns(df)
        df = normalize_values(df, group_hint)
        df["source_file"] = Path(p).name
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=list(COL_ALIASES.keys())+["source_file"])
    return pd.concat(frames, ignore_index=True)

def dedupe_keep_most_complete(df: pd.DataFrame) -> pd.DataFrame:
    key_fields = ["first_name","last_name","sex","race","age_min","age_max",
                  "date_missing","date_found","state","city","latitude","longitude"]
    df["_score"] = df[key_fields].notna().sum(axis=1)
    with_id = df[df["namus_number"].notna()].copy()
    without_id = df[df["namus_number"].isna()].copy()
    with_id = (with_id.sort_values(["namus_number","_score"], ascending=[True, False])
                      .drop_duplicates(subset=["namus_number"], keep="first"))
    out = pd.concat([with_id, without_id], ignore_index=True).drop(columns=["_score"])
    return out

def merge_group(paths, label, outstem, group_hint):
    print(f"--- {label} ---")
    df = union_and_clean(paths, group_hint)
    print(f"raw rows={len(df)}, null IDs={df['namus_number'].isna().sum()}")
    before = len(df)
    df = dedupe_keep_most_complete(df)
    after = len(df)
    print(f"after de-dupe={after}, removed={before-after}")
    for c in ["sex","race","state","date_found"]:
        if c in df.columns:
            print(f"non-null {c}: {df[c].notna().sum()}")
    out_csv = CLEAN_DIR / f"{outstem}.csv"
    out_parquet = CLEAN_DIR / f"{outstem}.parquet"
    df.to_csv(out_csv, index=False)
    try:
        df.to_parquet(out_parquet, index=False)
    except Exception:
        print("Parquet save skipped (install pyarrow for .parquet).")
    return df

def main():
    mp = merge_group(MP_INPUTS, "Missing Persons", "MP_master", "MP")
    up = merge_group(UP_INPUTS, "Unidentified Persons", "UP_master", "UP")
    print("\n=== Quick sanity ===")
    print("MP rows:", len(mp), "UP rows:", len(up))
    print("MP sex counts:\n", mp["sex"].value_counts(dropna=False).head())
    print("UP sex counts:\n", up["sex"].value_counts(dropna=False).head())
    print("MP top states:\n", mp["state"].value_counts().head(10))
    print("UP top states:\n", up["state"].value_counts().head(10))

if __name__ == "__main__":
    main()
