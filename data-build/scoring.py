import math

def exp_decay(gap: float, scale: float) -> float:
    return math.exp(-max(gap, 0.0)/scale)

def sim_age(mp_min, mp_max, uid_min, uid_max):
    if None in (mp_min, mp_max, uid_min, uid_max):
        return 0.5
    gap = max(0, max(mp_min - uid_max, uid_min - mp_max))
    return exp_decay(gap, 5.0)

def sim_height(mp_h, uid_h):
    if mp_h is None or uid_h is None:
        return 0.5
    return exp_decay(abs(mp_h - uid_h), 3.0)

def sim_weight(mp_w, uid_w):
    if mp_w is None or uid_w is None:
        return 0.5
    return exp_decay(abs(mp_w - uid_w), 20.0)

def sim_distance_km(km):
    if km is None:
        return 0.5
    return 1.0 / (1.0 + math.log1p(max(km, 0.0)))

def date_consistency(last_seen_days, found_days):
    if last_seen_days is None or found_days is None:
        return 0.5, False
    delta = found_days - last_seen_days
    if delta < -7:
        return 0.0, True
    if delta < 0:
        return 0.2, False
    return 1.0 / (1.0 + (delta / 365.0)), False

def cat_sim(a, b, equal_score=1.0, diff_score=0.2):
    if not a or not b:
        return 0.5
    return equal_score if (str(a).strip().lower() == str(b).strip().lower()) else diff_score

DEFAULT_WEIGHTS = dict(
    sex=1.0, age=1.2, height=0.8, weight=0.4, distance=1.0, date=1.0,
    eyes=0.5, race=0.3, tattoo=1.4, items=0.8, features=1.6, modality=0.6
)

FEATURE_TO_WEIGHT_KEY = {
    "SexMatch":"sex","AgeSim":"age","HeightSim":"height","WeightSim":"weight",
    "DistanceSim":"distance","DateConsistency":"date","EyeColorSim":"eyes",
    "RaceSim":"race","TattooSignal":"tattoo","ClothingSignal":"items",
    "DistinctiveMarkSignal":"features","ForensicsSignal":"modality"
}

def score_pair(row, use_race=False, weights=None):
    W = dict(DEFAULT_WEIGHTS)
    if weights:
        W.update(weights)

    # Hard filters
    if row.get('mp_sex') and row.get('uid_sex') and row['mp_sex'] != row['uid_sex']:
        return None, ["Sex mismatch (reject)"]

    dc, reject = date_consistency(row.get('mp_date_days'), row.get('uid_date_days'))
    if reject:
        return None, ["Found before last seen (reject)"]

    comps = []
    comps.append(("SexMatch", 1.0))
    comps.append(("AgeSim", sim_age(row.get('mp_age_min'), row.get('mp_age_max'),
                                   row.get('uid_age_min'), row.get('uid_age_max'))))
    comps.append(("HeightSim", sim_height(row.get('mp_height'), row.get('uid_height'))))
    comps.append(("WeightSim", sim_weight(row.get('mp_weight'), row.get('uid_weight'))))
    comps.append(("DistanceSim", sim_distance_km(row.get('km'))))
    comps.append(("DateConsistency", dc))
    comps.append(("EyeColorSim", cat_sim(row.get('mp_eye'), row.get('uid_eye'), 1.0, 0.2)))

    if use_race:
        comps.append(("RaceSim", cat_sim(row.get('mp_race'), row.get('uid_race'), 0.8, 0.4)))

    T = min(int(row.get('shared_tattoos', 0) or 0), 2) / 2.0
    I = min(int(row.get('shared_items', 0) or 0), 2) / 2.0
    F = 1.0 if row.get('distinctive_match') else 0.0
    M = min(max(int(row.get('shared_modalities', 0) or 0), 0), 3) / 3.0

    comps += [("TattooSignal", T), ("ClothingSignal", I),
              ("DistinctiveMarkSignal", F), ("ForensicsSignal", M)]

    num = 0.0; den = 0.0; why = []
    for name, val in comps:
        key = FEATURE_TO_WEIGHT_KEY[name]
        if (key == 'race') and (not use_race):
            continue
        w = W[key]
        num += w * val; den += w
        why.append((name, w * val))

    score = max(0.0, min(1.0, num / den if den > 0 else 0.0))
    why_msgs = [f"{n} (+{c:.2f})" for n, c in
                sorted(why, key=lambda x: x[1], reverse=True)[:5]]
    return score, why_msgs
