# data-build/build_candidates.py

import os, json
import pandas as pd
from scoring import score_pair

ROOT = os.path.dirname(os.path.dirname(__file__))
OUT_DIR = os.environ.get("OUT_DIR", os.path.join(ROOT, "out"))
load_dir = os.path.join(OUT_DIR, "neptune_load")

# Load edges (we only care about NEAR relationships for now)
edges = pd.read_csv(os.path.join(load_dir, "edges.csv"))
near = edges[edges["~label"] == "NEAR"].copy()
near = near.rename(
    columns={"~from": "mp_id", "~to": "uid_id", "km:double": "km"}
)[["mp_id", "uid_id", "km"]]

# Bring back attributes from the cases json files
mp = pd.read_json(os.path.join(OUT_DIR, "cases_mp.json"))
uid = pd.read_json(os.path.join(OUT_DIR, "cases_uid.json"))

# Prefix/normalize columns to avoid overlap
mp = mp.rename(
    columns={
        "id": "mp_id",
        "sex": "mp_sex",
        "age_min": "mp_age_min",
        "age_max": "mp_age_max",
        "height_in": "mp_height",
        "weight_lb": "mp_weight",
        "last_seen_date": "mp_last_seen",
        "eye_color": "mp_eye",
        "race": "mp_race",
        "lat": "mp_lat",
        "lon": "mp_lon",
    }
)

uid = uid.rename(
    columns={
        "id": "uid_id",
        "sex": "uid_sex",
        "age_min": "uid_age_min",
        "age_max": "uid_age_max",
        "age_est_min": "uid_age_min",
        "age_est_max": "uid_age_max",
        "height_in": "uid_height",
        "weight_lb": "uid_weight",
        "found_date": "uid_found",
        "eye_color": "uid_eye",
        "race": "uid_race",
        "lat": "uid_lat",
        "lon": "uid_lon",
    }
)

# Keep only the fields we need
mp_keep = [
    "mp_id",
    "mp_sex",
    "mp_age_min",
    "mp_age_max",
    "mp_height",
    "mp_weight",
    "mp_last_seen",
    "mp_eye",
    "mp_race",
    "mp_lat",
    "mp_lon",
]
uid_keep = [
    "uid_id",
    "uid_sex",
    "uid_age_min",
    "uid_age_max",
    "uid_height",
    "uid_weight",
    "uid_found",
    "uid_eye",
    "uid_race",
    "uid_lat",
    "uid_lon",
]
mp = mp[mp_keep]
uid = uid[uid_keep]

# Merge without overlapping column names
cand = near.merge(mp, on="mp_id", how="left").merge(uid, on="uid_id", how="left")

# Date ordinals
cand["mp_date_days"] = pd.to_datetime(cand["mp_last_seen"]).dt.date.map(
    lambda d: d.toordinal() if pd.notna(d) else None
)
cand["uid_date_days"] = pd.to_datetime(cand["uid_found"]).dt.date.map(
    lambda d: d.toordinal() if pd.notna(d) else None
)

# Placeholder features
cand["shared_tattoos"] = 0
cand["shared_items"] = 0
cand["distinctive_match"] = False
cand["shared_modalities"] = 0

# Score each candidate
rows = []
for _, r in cand.iterrows():
    row = {
        "mp_sex": r["mp_sex"],
        "uid_sex": r["uid_sex"],
        "mp_age_min": r["mp_age_min"],
        "mp_age_max": r["mp_age_max"],
        "uid_age_min": r.get("uid_age_min"),
        "uid_age_max": r.get("uid_age_max"),
        "mp_height": r["mp_height"],
        "uid_height": r["uid_height"],
        "mp_weight": r["mp_weight"],
        "uid_weight": r["uid_weight"],
        "mp_eye": r["mp_eye"],
        "uid_eye": r["uid_eye"],
        "mp_race": r["mp_race"],
        "uid_race": r["uid_race"],
        "mp_date_days": r["mp_date_days"],
        "uid_date_days": r["uid_date_days"],
        "km": r["km"],
        "shared_tattoos": r["shared_tattoos"],
        "shared_items": r["shared_items"],
        "distinctive_match": r["distinctive_match"],
        "shared_modalities": r["shared_modalities"],
    }
    score, why = score_pair(row, use_race=False)
    if score is None:
        continue
    rows.append(
        {
            "mp_id": r["mp_id"],
            "uid_id": r["uid_id"],
            "score": round(score, 4),
            "km": round(float(r["km"]), 1) if pd.notna(r["km"]) else None,
            "why": why,
        }
    )

# Group by MP and keep top 20 matches
out = {}
for rec in sorted(rows, key=lambda x: (-x["score"], x["km"] or 1e9)):
    out.setdefault(rec["mp_id"], []).append(rec)
for k in out.keys():
    out[k] = out[k][:20]

# Write candidates.jsonl
os.makedirs(OUT_DIR, exist_ok=True)
out_path = os.path.join(OUT_DIR, "candidates.jsonl")
with open(out_path, "w", encoding="utf-8") as f:
    for mp_id, cands in out.items():
        f.write(json.dumps({"mp_id": mp_id, "candidates": cands}) + "\n")

print("Wrote:", out_path)
