# data-build/namus_mapper.py
import os, re
import pandas as pd
from dateutil import parser as dparser

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW = os.path.join(ROOT, "raw")
DATA = os.path.join(ROOT, "data")
os.makedirs(DATA, exist_ok=True)

# helpers
NUM = re.compile(r"\d+")
H_PAT = [
    re.compile(r"(?P<ft>\d)\s*'\s*(?P<in>\d{1,2})\s*\"?"),
    re.compile(r"(?P<ft>\d)\s*-\s*(?P<in>\d{1,2})"),
    re.compile(r"(?P<in>\d{2,3})\s*(?:in|inch|inches)\b", re.I),
]
def parse_height(v):
    if pd.isna(v): return None
    s=str(v).strip().lower()
    for p in H_PAT:
        m=p.search(s)
        if m:
            if "ft" in m.groupdict():
                return int(m.group("ft"))*12 + int(m.group("in") or 0)
            return int(m.group("in"))
    m=NUM.search(s)
    if m:
        n=int(m.group())
        if 48<=n<=84: return n
    return None

def parse_weight(v):
    if pd.isna(v): return None
    s=str(v).lower()
    # range e.g. 110-130
    m=re.search(r"(\d{2,3})\s*[-–to]{1,3}\s*(\d{2,3})", s)
    if m: return (int(m.group(1))+int(m.group(2)))//2
    nums=[int(n) for n in NUM.findall(s)]
    for n in nums:
        if 60<=n<=400: return n
    return None

def parse_date(v):
    if pd.isna(v) or str(v).strip()=="" or str(v).strip().lower()=="unknown":
        return None
    try:
        return dparser.parse(str(v), fuzzy=True).date().isoformat()
    except Exception:
        return None

def split_tokens(v):
    if pd.isna(v): return ""
    s=str(v).replace("\n"," ").replace(",",";")
    toks=[t.strip().lower() for t in s.split(";") if t.strip()]
    return ";".join(toks)

# load raw (headers must match the fake CSVs we made)
mp_raw  = pd.read_csv(os.path.join(RAW, "namus_missing.csv"))
uid_raw = pd.read_csv(os.path.join(RAW, "namus_unidentified.csv"))

# map to normalized schema expected downstream
mp = pd.DataFrame({
    "id": mp_raw["Case Number"].astype(str),
    "sex": mp_raw["Sex"],
    "age_min": mp_raw["Min Age"],
    "age_max": mp_raw["Max Age"],
    "height_in": mp_raw["Height"].apply(parse_height),
    "weight_lb": mp_raw["Weight"].apply(parse_weight),
    "last_seen_date": mp_raw["Date Last Seen"].apply(parse_date),
    "lat": pd.to_numeric(mp_raw.get("Latitude"), errors="coerce"),
    "lon": pd.to_numeric(mp_raw.get("Longitude"), errors="coerce"),
    "eye_color": mp_raw.get("Eye Color"),
    "race": mp_raw.get("Race / Ethnicity"),
    "tattoos": mp_raw.get("Tattoos", "").apply(split_tokens),
    "items": mp_raw.get("Clothing / Accessories", "").apply(split_tokens),
})

uid = pd.DataFrame({
    "id": uid_raw["Case Number"].astype(str),
    "sex": uid_raw["Sex"],
    "age_min": uid_raw["Estimated Age (Min)"],
    "age_max": uid_raw["Estimated Age (Max)"],
    "height_in": uid_raw["Estimated Height"].apply(parse_height),
    "weight_lb": uid_raw["Estimated Weight"].apply(parse_weight),
    "found_date": uid_raw["Date Found"].apply(parse_date),
    "lat": pd.to_numeric(uid_raw.get("Latitude"), errors="coerce"),
    "lon": pd.to_numeric(uid_raw.get("Longitude"), errors="coerce"),
    "eye_color": uid_raw.get("Eye Color"),
    "race": uid_raw.get("Race / Ethnicity"),
    "tattoos": uid_raw.get("Tattoos", "").apply(split_tokens),
    "items": uid_raw.get("Clothing / Accessories", "").apply(split_tokens),
})

mp_out = os.path.join(DATA, "sample_mp.csv")
uid_out = os.path.join(DATA, "sample_uid.csv")
mp.to_csv(mp_out, index=False)
uid.to_csv(uid_out, index=False)
print("Wrote:", mp_out)
print("Wrote:", uid_out)
