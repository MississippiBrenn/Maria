from pathlib import Path
import pandas as pd
import numpy as np
import json

# ========== Tunables ==========
REQUIRE_SEX_MATCH   = True  # True to require F↔F or M↔M (unknowns allowed)
DATE_WINDOW_DAYS    = None    # e.g., 730; None = skip date filter
ENFORCE_AGE_OVERLAP = True   # True to require overlapping age ranges
MAX_GEO_MILES       = None    # e.g., 200; None = skip geo filter
MAX_UP_PER_MP       = 5       # cap edges per MP
LIMIT_STATES        = None    # e.g., {'TX','CA'}; None = all
# ==============================

BASE = Path(__file__).resolve().parent.parent
CLEAN = BASE / "data" / "clean"
OUT = BASE / "data" / "out"
OUT.mkdir(parents=True, exist_ok=True)

# --- helpers ---
USPS = {
    "ALABAMA":"AL","AL":"AL","ALASKA":"AK","AK":"AK","ARIZONA":"AZ","AZ":"AZ","ARKANSAS":"AR","AR":"AR",
    "CALIFORNIA":"CA","CA":"CA","COLORADO":"CO","CO":"CO","CONNECTICUT":"CT","CT":"CT","DELAWARE":"DE","DE":"DE",
    "FLORIDA":"FL","FL":"FL","GEORGIA":"GA","GA":"GA","HAWAII":"HI","HI":"HI","IDAHO":"ID","ID":"ID",
    "ILLINOIS":"IL","IL":"IL","INDIANA":"IN","IN":"IN","IOWA":"IA","IA":"IA","KANSAS":"KS","KS":"KS",
    "KENTUCKY":"KY","KY":"KY","LOUISIANA":"LA","LA":"LA","MAINE":"ME","ME":"ME","MARYLAND":"MD","MD":"MD",
    "MASSACHUSETTS":"MA","MA":"MA","MICHIGAN":"MI","MI":"MI","MINNESOTA":"MN","MN":"MN","MISSISSIPPI":"MS","MS":"MS",
    "MISSOURI":"MO","MO":"MO","MONTANA":"MT","MT":"MT","NEBRASKA":"NE","NE":"NE","NEVADA":"NV","NV":"NV",
    "NEW HAMPSHIRE":"NH","NH":"NH","NEW JERSEY":"NJ","NJ":"NJ","NEW MEXICO":"NM","NM":"NM","NEW YORK":"NY","NY":"NY",
    "NORTH CAROLINA":"NC","NC":"NC","NORTH DAKOTA":"ND","ND":"ND","OHIO":"OH","OH":"OH","OKLAHOMA":"OK","OK":"OK",
    "OREGON":"OR","OR":"OR","PENNSYLVANIA":"PA","PA":"PA","RHODE ISLAND":"RI","RI":"RI","SOUTH CAROLINA":"SC","SC":"SC",
    "SOUTH DAKOTA":"SD","SD":"SD","TENNESSEE":"TN","TN":"TN","TEXAS":"TX","TX":"TX","UTAH":"UT","UT":"UT",
    "VERMONT":"VT","VT":"VT","VIRGINIA":"VA","VA":"VA","WASHINGTON":"WA","WA":"WA",
    "WEST VIRGINIA":"WV","WV":"WV","WISCONSIN":"WI","WI":"WI","WYOMING":"WY","WY":"WY","DISTRICT OF COLUMBIA":"DC","DC":"DC"
}

def norm_state(s):
    if pd.isna(s): return ""
    s = str(s).strip().upper()
    return USPS.get(s, s)

def s_str(v):
    return "" if pd.isna(v) else str(v)

def s_sex(v):
    v = "" if pd.isna(v) else str(v).upper().strip()
    return v[:1] if v else ""

def s_num(v):
    return None if pd.isna(v) else float(v)

# --- load data ---
MP = pd.read_csv(CLEAN / "MP_master.csv", dtype=str, low_memory=False)
UP = pd.read_csv(CLEAN / "UP_master.csv", dtype=str, low_memory=False)

cols = ["namus_number","sex","state","city","age_min","age_max","date_missing","date_found","latitude","longitude"]
MP = MP.reindex(columns=cols)
UP = UP.reindex(columns=cols)

for df in (MP, UP):
    df["date_missing"] = pd.to_datetime(df["date_missing"], errors="coerce")
    df["date_found"]   = pd.to_datetime(df["date_found"],   errors="coerce")
    for c in ["age_min","age_max","latitude","longitude"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["sex"] = df["sex"].astype(str).str.upper().str[:1].replace({"N":"U","<":"U","NAN":np.nan,"NONE":np.nan})
    df["state"] = df["state"].map(norm_state)

if LIMIT_STATES:
    MP = MP[MP["state"].isin(LIMIT_STATES)]
    UP = UP[UP["state"].isin(LIMIT_STATES)]

# --- build nodes ---
nodes, seen = [], set()
def add_nodes(df, typ):
    df2 = df[["namus_number","sex","state","city"]].dropna(subset=["namus_number"]).drop_duplicates()
    for _, r in df2.iterrows():
        nid = s_str(r["namus_number"]).strip()
        if not nid or nid in seen: continue
        seen.add(nid)
        sex = s_sex(r["sex"]); st = s_str(r["state"]).upper(); city = s_str(r["city"])
        nodes.append({
            "id": nid, "type": typ,
            "sex": sex, "state": st, "city": city,
            "label": f"{nid} ({sex or '?'} · {st})"
        })
add_nodes(MP, "MP"); add_nodes(UP, "UP")

# --- build edges ---
edges = []
states = sorted(set(MP["state"]) | set(UP["state"]))
for st in states:
    if not st: continue
    mp_s, up_s = MP[MP["state"]==st], UP[UP["state"]==st]
    if mp_s.empty or up_s.empty: continue

    mp_s["__k"]=1; up_s["__k"]=1
    pairs = mp_s.merge(up_s,on="__k",suffixes=("_mp","_up")).drop(columns="__k")

    # filters (simplified; add stricter later)
    mask = pd.Series(True, index=pairs.index)

    cand = pairs.loc[mask, ["namus_number_mp","namus_number_up","sex_mp","sex_up"]].copy()
    cand = cand[(cand["namus_number_mp"].astype(str).str.strip()!="") &
                (cand["namus_number_up"].astype(str).str.strip()!="")]

    if MAX_UP_PER_MP:
        cand = cand.groupby("namus_number_mp").head(MAX_UP_PER_MP).reset_index(drop=True)

    edges.extend([
        {"id": f"{s_str(r.namus_number_mp)}__{s_str(r.namus_number_up)}",
         "source": s_str(r.namus_number_mp),
         "target": s_str(r.namus_number_up),
         "sex_match": s_sex(r.sex_mp)==s_sex(r.sex_up)}
        for r in cand.itertuples(index=False)
    ])

# --- write JSON ---
out = {"nodes": nodes, "edges": edges}
(OUT/"ragnet.json").write_text(json.dumps(out, ensure_ascii=False, allow_nan=False))
print(f"Wrote {len(nodes)} nodes, {len(edges)} edges → {OUT/'ragnet.json'}")
