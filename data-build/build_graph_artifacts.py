import os, csv
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data')
OUT_DIR = os.environ.get('OUT_DIR', os.path.join(ROOT, 'out'))
os.makedirs(OUT_DIR, exist_ok=True)

# Simple haversine (km)
R = 6371.0088
def haversine_km(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, asin, sqrt
    if None in (lat1, lon1, lat2, lon2):
        return None
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))

# Load the normalized CSVs (from namus_mapper.py)
mp = pd.read_csv(os.path.join(DATA_DIR, 'sample_mp.csv'))
uid = pd.read_csv(os.path.join(DATA_DIR, 'sample_uid.csv'))

# Normalize dates → ordinal days
mp['mp_date_days'] = pd.to_datetime(mp['last_seen_date']).dt.date.map(lambda d: d.toordinal())
uid['uid_date_days'] = pd.to_datetime(uid['found_date']).dt.date.map(lambda d: d.toordinal())

# Tokenize tattoos/items
for df, prefix in [(mp, 'mp'), (uid, 'uid')]:
    for col in ['tattoos', 'items']:
        toks = []
        for x in df[col].fillna(''):
            parts = [t.strip().lower() for t in str(x).replace(',', ';').split(';') if t.strip()]
            toks.append(parts)
        df[f'{prefix}_{col}_tokens'] = toks

# Pair up candidates with basic blocking (sex + overlapping ages)
pairs = []
for _, m in mp.iterrows():
    for _, u in uid.iterrows():
        if str(m['sex']).strip().upper() != str(u['sex']).strip().upper():
            continue
        if not (m['age_min'] <= u['age_max'] and u['age_min'] <= m['age_max']):
            continue
        km = haversine_km(m['lat'], m['lon'], u['lat'], u['lon'])
        days_gap = int(u['uid_date_days'] - m['mp_date_days'])
        shared_tat = len(set(m['mp_tattoos_tokens']).intersection(u['uid_tattoos_tokens']))
        shared_items = len(set(m['mp_items_tokens']).intersection(u['uid_items_tokens']))
        pairs.append(dict(
            mp_id=m['id'], uid_id=u['id'], km=km, days_gap=days_gap,
            shared_tattoos=shared_tat, shared_items=shared_items,
        ))

pairs_df = pd.DataFrame(pairs)

# --- Emit Neptune bulk loader CSVs ---
load_dir = os.path.join(OUT_DIR, 'neptune_load')
os.makedirs(load_dir, exist_ok=True)
nodes_path = os.path.join(load_dir, 'nodes.csv')
edges_path = os.path.join(load_dir, 'edges.csv')

# Collect unique Tattoo/Item nodes
tattoos = sorted({t for ts in mp['mp_tattoos_tokens'] for t in ts}.union(
                 {t for ts in uid['uid_tattoos_tokens'] for t in ts}))
items = sorted({i for its in mp['mp_items_tokens'] for i in its}.union(
               {i for its in uid['uid_items_tokens'] for i in its}))

with open(nodes_path, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['~id','~label','sex','age_min:int','age_max:int','height_in:int','weight_lb:int','date:string','lat:double','lon:double','state','eye_color','race','notes'])
    for _, m in mp.iterrows():
        w.writerow([m['id'],'MP',m['sex'],m['age_min'],m['age_max'],m['height_in'],m['weight_lb'],m['last_seen_date'],m['lat'],m['lon'],'',m['eye_color'],m['race'], ''])
    for _, u in uid.iterrows():
        w.writerow([u['id'],'UID',u['sex'],u['age_min'],u['age_max'],u['height_in'],u['weight_lb'],u['found_date'],u['lat'],u['lon'],'',u['eye_color'],u['race'], ''])
    for t in tattoos:
        w.writerow([f'tattoo:{t}','Tattoo','','','','','','','','','','','', t])
    for i in items:
        w.writerow([f'item:{i}','ClothingItem','','','','','','','','','','','', i])

with open(edges_path, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['~from','~to','~label','km:double','days_gap:int'])
    for _, r in pairs_df.iterrows():
        w.writerow([r['mp_id'], r['uid_id'], 'NEAR', r['km'] or '', ''])
        w.writerow([r['mp_id'], r['uid_id'], 'TIME_OVERLAP', '', r['days_gap']])
    for _, m in mp.iterrows():
        for t in m['mp_tattoos_tokens']:
            w.writerow([m['id'], f'tattoo:{t}', 'HAS_TATTOO', '', ''])
        for it in m['mp_items_tokens']:
            w.writerow([m['id'], f'item:{it}', 'HAS_ITEM', '', ''])
    for _, u in uid.iterrows():
        for t in u['uid_tattoos_tokens']:
            w.writerow([u['id'], f'tattoo:{t}', 'HAS_TATTOO', '', ''])
        for it in u['uid_items_tokens']:
            w.writerow([u['id'], f'item:{it}', 'HAS_ITEM', '', ''])

print(f"Wrote: {nodes_path}, {edges_path}")
