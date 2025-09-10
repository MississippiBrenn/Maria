from pathlib import Path
import pandas as pd
import numpy as np
from datetime import timedelta
import json

# ================ Tunables ================
REQUIRE_SEX_MATCH   = False     # True to require F↔F or M↔M (unknowns allowed)
DATE_WINDOW_DAYS    = None      # e.g., 730; None = no date filter
MAX_GEO_MILES       = None      # e.g., 200; None = no geo filter
ENFORCE_AGE_OVERLAP = False     # True to require overlapping age ranges
LIMIT_STATES        = None      # e.g., {'TX','CA'} to test; None = all
MAX_UP_PER_MP       = 25        # cap edges per MP to keep JSON manageable
PRINT_EVERY         = 5         # progress ping every N states
# =========================================

BASE_DIR = Path(__file__).resolve().parent.parent
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUT_DIR = BASE_DIR / "data" / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Helpers ----------
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
    "WEST VIRGINIA":"WV","WV":"WV","WISCONSIN":"WI","WI":"WI","WYOMING":"WY","WY":"WY",
    "DISTRICT OF COLUMBIA":"DC","DC":"DC"
}

def normalize_state(s):
    if pd.isna(s): return np.nan
    s = str(s).strip().upper()
    return USPS.get(s, s)

def haversine_miles(lat1, lon1, lat2, lon2):
    lat1 = np.radians(lat1.astype(float))
    lon1 = np.radians(lon1.astype(float))
    lat2 = np.radians(lat2.astype(float))
    lon2 = np.radians(lon2.astype(float))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2.0)**2
    c = 2*np.arcsin(np.sqrt(a))
    km = 6371.0088 * c
    return km * 0.621371

# ---------- Load & normalize ----------
MP = pd.read_csv(CLEAN_DIR / "MP_master.csv", dtype=str, low_memory=False)
UP = pd.read_csv(CLEAN_DIR / "UP_master.csv", dtype=str, low_memory=False)

# Only needed columns (RAM saver)
keep = ["namus_number","sex","state","city","age_min","age_max","date_missing","date_found","latitude","longitude"]
MP = MP.reindex(columns=keep)
UP = UP.reindex(columns=keep)

for df in (MP, UP):
    df["date_missing"] = pd.to_datetime(df["date_missing"], errors="coerce")
    df["date_found"]   = pd.to_datetime(df["date_found"],   errors="coerce")
    for c in ["age_min","age_max","latitude","longitude"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["sex"] = df["sex"].astype(str).str.upper().str[:1].replace({"N":"U","<":"U","NAN":np.nan,"NONE":np.nan})

MP["state"] = MP["state"].map(normalize_state)
UP["state"] = UP["state"].map(normalize_state)

if LIMIT_STATES:
    allowed = {s.upper() for s in LIMIT_STATES}
    MP = MP[MP["state"].isin(allowed)]
    UP = UP[UP["state"].isin(allowed)]

# ---------- Nodes ----------
nodes = []
seen = set()
def add_nodes(df, typ):
    df2 = df[["namus_number","sex","state","city"]].dropna(subset=["namus_number"]).drop_duplicates()
    for _, r in df2.iterrows():
        nid = r["namus_number"]
        if nid in seen: continue
        seen.add(nid)
        nodes.append({
            "id": nid,
            "type": typ,
            "sex": (r["sex"] or "").upper()[:1],
            "state": (r["state"] or ""),
            "city": r["city"] or "",
            "label": f"{nid} ({(r['sex'] or '').upper()[:1] or '?'} · {(r['state'] or '')})"
        })
add_nodes(MP, "MP")
add_nodes(UP, "UP")

# ---------- Vectorized edges per state ----------
edges = []
states = sorted(set(MP["state"].dropna()) | set(UP["state"].dropna()))

for i, st in enumerate(states, 1):
    mp_s = MP[MP["state"] == st].copy()
    up_s = UP[UP["state"] == st].copy()
    if mp_s.empty or up_s.empty:
        continue

    # Cross join within state
    mp_s["__k"] = 1
    up_s["__k"] = 1
    pairs = mp_s.merge(up_s, on="__k", suffixes=("_mp","_up")).drop(columns="__k")

    total = len(pairs)

    # Sex rule
    if REQUIRE_SEX_MATCH:
        msex = pairs["sex_mp"].fillna("")
        usex = pairs["sex_up"].fillna("")
        sex_ok = ~((msex.isin(["F","M"])) & (usex.isin(["F","M"])) & (msex != usex))
    else:
        sex_ok = pd.Series(True, index=pairs.index)

    # Date rule
    if DATE_WINDOW_DAYS is not None:
        dm = pairs["date_missing_mp"]; df = pairs["date_found_up"]
        date_ok = dm.isna() | df.isna() | ((df >= dm) & (df <= dm + pd.Timedelta(days=DATE_WINDOW_DAYS)))
    else:
        date_ok = pd.Series(True, index=pairs.index)

    # Age rule
    if ENFORCE_AGE_OVERLAP:
        mp_min = pairs["age_min_mp"].fillna(-1e9); mp_max = pairs["age_max_mp"].fillna(1e9)
        up_min = pairs["age_min_up"].fillna(-1e9); up_max = pairs["age_max_up"].fillna(1e9)
        age_ok = (np.maximum(mp_min, up_min) <= np.minimum(mp_max, up_max))
    else:
        age_ok = pd.Series(True, index=pairs.index)

    # Geo rule
    if MAX_GEO_MILES is not None:
        lat1 = pairs["latitude_mp"]; lon1 = pairs["longitude_mp"]
        lat2 = pairs["latitude_up"];  lon2 = pairs["longitude_up"]
        have_geo = ~(lat1.isna() | lon1.isna() | lat2.isna() | lon2.isna())
        geo_dist = pd.Series(np.nan, index=pairs.index)
        if have_geo.any():
            geo_dist.loc[have_geo] = haversine_miles(lat1[have_geo].values, lon1[have_geo].values,
                                                     lat2[have_geo].values, lon2[have_geo].values)
        geo_ok = (~have_geo) | (geo_dist <= MAX_GEO_MILES)
    else:
        geo_dist = pd.Series(np.nan, index=pairs.index)
        geo_ok = pd.Series(True, index=pairs.index)

    mask = sex_ok & date_ok & age_ok & geo_ok
    kept = int(mask.sum())

    # Slice candidates — DO NOT drop for ID yet
    cand = pairs.loc[mask, [
        "namus_number_mp","namus_number_up","sex_mp","sex_up",
        "date_missing_mp","date_found_up"
    ]].copy()

    # Attach metrics
    cand["date_gap_days"] = (pairs.loc[cand.index, "date_found_up"] - pairs.loc[cand.index, "date_missing_mp"]).dt.days
    cand["geo_miles"] = geo_dist.loc[cand.index].astype(float)

    # Cap N UP per MP (optional)
    if MAX_UP_PER_MP is not None and not cand.empty:
        cand = cand.sort_values(
            by=["namus_number_mp","date_gap_days","geo_miles"],
            ascending=[True, True, True],
            na_position="last"
        )
        cand = cand.groupby("namus_number_mp", dropna=False).head(MAX_UP_PER_MP).reset_index(drop=True)

    # Build edges (coerce IDs to strings & strip; we'll filter empties after)
    if not cand.empty:
        new_edges = []
        for r in cand.itertuples(index=False):
            src = (str(r.namus_number_mp) if r.namus_number_mp is not None else "").strip()
            tgt = (str(r.namus_number_up) if r.namus_number_up is not None else "").strip()
            new_edges.append({
                "id": f"{src}__{tgt}",
                "source": src,
                "target": tgt,
                "sex_match": bool((str(r.sex_mp or "")[:1]).upper() == (str(r.sex_up or "")[:1]).upper()),
                "geo_miles": None if pd.isna(r.geo_miles) else float(r.geo_miles),
                "date_gap_days": None if pd.isna(r.date_gap_days) else int(r.date_gap_days),
            })

        # Now drop edges with empty source/target and report
        before = len(new_edges)
        new_edges = [e for e in new_edges if e["source"] and e["target"]]
        after = len(new_edges)
        if before != after:
            print(f"   └─ {st}: removed {before-after} edges due to empty source/target IDs (post-strip)")

        edges.extend(new_edges)

    if i % PRINT_EVERY == 0:
        print(f"[{i}/{len(states)}] {st}: pairs={total:,} kept_mask={kept:,} edges_total={len(edges):,}")

# ---------- Write JSON ----------
out = {"nodes": nodes, "edges": edges}
out_path = OUT_DIR / "ragnet.json"
out_path.write_text(json.dumps(out, ensure_ascii=False))
print(f"Wrote {len(nodes)} nodes, {len(edges)} edges → {out_path}")
