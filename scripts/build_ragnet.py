_import pandas as pd
import networkx as nx
from pathlib import Path
from math import radians, sin, cos, asin, sqrt
from datetime import timedelta

# === Paths ===
BASE_DIR = Path(__file__).resolve().parent.parent
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUT_DIR = BASE_DIR / "data" / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MP_PATH = CLEAN_DIR / "MP_master.csv"
UP_PATH = CLEAN_DIR / "UP_master.csv"
GRAPH_PATH = OUT_DIR / "ragnet.graphml"   # portable graph format

# === Parameters ===
DATE_WINDOW_DAYS = 730    # 2 years
MAX_GEO_MILES = 200       # edge if within 200 miles

def haversine_mi(lat1, lon1, lat2, lon2):
    if any(pd.isna([lat1, lon1, lat2, lon2])): return None
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6371.0088 * c
    return km * 0.621371

def main():
    # Load
    mp = pd.read_csv(MP_PATH, dtype=str, low_memory=False)
    up = pd.read_csv(UP_PATH, dtype=str, low_memory=False)

    # Normalize types
    for df, who in [(mp,"MP"), (up,"UP")]:
        if "date_missing" in df.columns:
            df["date_missing"] = pd.to_datetime(df["date_missing"], errors="coerce")
        if "date_found" in df.columns:
            df["date_found"] = pd.to_datetime(df["date_found"], errors="coerce")
        for c in ["age_min","age_max","latitude","longitude"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

    # Build graph
    G = nx.Graph()

    # Add nodes
    for _, r in mp.iterrows():
        G.add_node(r["namus_number"], type="MP", sex=r.get("sex"), state=r.get("state"))
    for _, r in up.iterrows():
        G.add_node(r["namus_number"], type="UP", sex=r.get("sex"), state=r.get("state"))

    # Candidate edges (basic rules)
    for _, mp_r in mp.iterrows():
        for _, up_r in up.iterrows():
            # State must match
            if str(mp_r.get("state")).upper() != str(up_r.get("state")).upper():
                continue
            # Sex must match if both known
            if pd.notna(mp_r.get("sex")) and pd.notna(up_r.get("sex")):
                if str(mp_r["sex"]).upper()[0] != str(up_r["sex"]).upper()[0]:
                    continue
            # Date window
            if pd.notna(mp_r.get("date_missing")) and pd.notna(up_r.get("date_found")):
                if not (mp_r["date_missing"] <= up_r["date_found"] <= mp_r["date_missing"] + timedelta(days=DATE_WINDOW_DAYS)):
                    continue
            # Age overlap
            age_ok = True
            if pd.notna(mp_r.get("age_min")) or pd.notna(mp_r.get("age_max")):
                if pd.notna(up_r.get("age_min")) or pd.notna(up_r.get("age_max")):
                    mp_min = mp_r.get("age_min") if pd.notna(mp_r.get("age_min")) else -1e9
                    mp_max = mp_r.get("age_max") if pd.notna(mp_r.get("age_max")) else 1e9
                    up_min = up_r.get("age_min") if pd.notna(up_r.get("age_min")) else -1e9
                    up_max = up_r.get("age_max") if pd.notna(up_r.get("age_max")) else 1e9
                    age_ok = max(mp_min, up_min) <= min(mp_max, up_max)
            if not age_ok:
                continue
            # Geo distance
            geo_mi = haversine_mi(mp_r.get("latitude"), mp_r.get("longitude"),
                                  up_r.get("latitude"), up_r.get("longitude"))
            if geo_mi is not None and geo_mi > MAX_GEO_MILES:
                continue

            # Add edge with attributes
            G.add_edge(mp_r["namus_number"], up_r["namus_number"],
                       sex_match=(mp_r.get("sex") == up_r.get("sex")),
                       geo_miles=geo_mi,
                       date_gap_days=(up_r.get("date_found") - mp_r.get("date_missing")).days
                                     if pd.notna(up_r.get("date_found")) and pd.notna(mp_r.get("date_missing"))
                                     else None)

    print(nx.info(G))
    nx.write_graphml(G, GRAPH_PATH)
    print(f"Graph saved to {GRAPH_PATH}")

if __name__ == "__main__":
    main()
