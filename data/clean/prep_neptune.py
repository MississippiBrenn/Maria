import pandas as pd, re

def prep_for_neptune(input_csv, output_csv, label, id_prefix):
    df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)

    # sanitize headers (no spaces/commas/weird chars)
    def clean(h):
        h = h.strip()
        h = re.sub(r'[ ,\r\n]+', '_', h)
        h = re.sub(r'[^A-Za-z0-9_]', '', h)
        if h.startswith('~'):
            h = 'prop_' + h[1:]
        return h

    # normalize column names first so we can find namus_number
    renamer = {c: clean(c) for c in df.columns}
    df.rename(columns=renamer, inplace=True)

    # use namus_number as the ID
    key = next((c for c in df.columns if c.lower() == 'namus_number'), None)
    if not key:
        raise ValueError(f"'namus_number' column not found. Available: {list(df.columns)}")

    df[key] = df[key].astype(str).str.strip()
    df = df[df[key] != ""].copy()     # drop empty-ID rows if any

    df["~id"] = id_prefix + df[key]
    df["~label"] = label

    # reorder & type-suffix properties as String
    user_cols = [c for c in df.columns if c not in ["~id","~label","~from","~to"]]
    df = df[["~id","~label"] + user_cols]
    df.columns = [c if c in ["~id","~label","~from","~to"] else f"{c}:String" for c in df.columns]

    df.to_csv(output_csv, index=False)
    print(f"✅ Wrote {output_csv} ({len(df)} rows)")

# Run for both
prep_for_neptune("MP_master.csv", "MP_neptune.csv", "MissingPerson", "MP_")
prep_for_neptune("UP_master.csv", "UP_neptune.csv", "UnidentifiedPerson", "UP_")
