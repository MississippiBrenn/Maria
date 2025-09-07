# data-build/make_report.py
import os, json, html, datetime
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(__file__))
OUT = os.path.join(ROOT, "out")
CANDS = os.path.join(OUT, "candidates.jsonl")
MP = os.path.join(OUT, "cases_mp.json")
UID = os.path.join(OUT, "cases_uid.json")
os.makedirs(OUT, exist_ok=True)

# Load data
mp = pd.read_json(MP) if os.path.exists(MP) else pd.DataFrame()
uid = pd.read_json(UID) if os.path.exists(UID) else pd.DataFrame()

rows = []
if os.path.exists(CANDS):
    with open(CANDS, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            mp_id = rec["mp_id"]
            for c in rec["candidates"]:
                rows.append({
                    "mp_id": mp_id,
                    "uid_id": c["uid_id"],
                    "score": c["score"],
                    "why": "; ".join(c.get("why", []))
                })
df = pd.DataFrame(rows)

# Join a few reference fields (optional)
if not mp.empty:
    mp_ref = mp[["id","sex","age_min","age_max","eye_color","race"]].rename(columns={
        "id":"mp_id","sex":"mp_sex","eye_color":"mp_eye","race":"mp_race"
    })
    df = df.merge(mp_ref, on="mp_id", how="left")

if not uid.empty:
    uid_ref = uid[["id","sex","eye_color","race"]].rename(columns={
        "id":"uid_id","sex":"uid_sex","eye_color":"uid_eye","race":"uid_race"
    })
    df = df.merge(uid_ref, on="uid_id", how="left")

df = df.sort_values(["mp_id","score"], ascending=[True, False])

# Minimal HTML (no external deps)
timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
def esc(x): return html.escape("" if x is None else str(x))

table_rows = []
for _, r in df.iterrows():
    table_rows.append(
        "<tr>"
        f"<td>{esc(r['mp_id'])}</td>"
        f"<td>{esc(r['uid_id'])}</td>"
        f"<td>{r['score']:.3f}</td>"
        f"<td>{esc(r.get('mp_sex'))}</td>"
        f"<td>{esc(r.get('mp_eye'))}</td>"
        f"<td>{esc(r.get('mp_race'))}</td>"
        f"<td>{esc(r.get('uid_sex'))}</td>"
        f"<td>{esc(r.get('uid_eye'))}</td>"
        f"<td>{esc(r.get('uid_race'))}</td>"
        f"<td>{esc(r['why'])}</td>"
        "</tr>"
    )

html_doc = f"""<!doctype html>
<html lang="en">
<meta charset="utf-8">
<title>Maria — Candidate Matches</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root {{ --fg:#111; --bg:#fff; --muted:#666; --line:#e6e6e6; --pill:#f4f6f8; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; margin: 24px; color: var(--fg); background: var(--bg); }}
  h1 {{ margin: 0 0 6px; font-size: 22px; }}
  .sub {{ color: var(--muted); margin-bottom: 18px; }}
  .tools {{ display:flex; gap:8px; margin: 12px 0 16px; align-items:center; flex-wrap:wrap; }}
  input[type=search] {{ padding:8px 10px; border:1px solid var(--line); border-radius:8px; min-width:260px; }}
  .pill {{ background: var(--pill); padding:6px 10px; border-radius:999px; font-size:12px; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ border-bottom: 1px solid var(--line); text-align: left; padding: 8px 10px; vertical-align: top; }}
  th {{ position: sticky; top:0; background: white; }}
  tr:hover td {{ background: #fafafa; }}
  .num {{ text-align:right; font-variant-numeric: tabular-nums; }}
  .foot {{ margin-top: 18px; color: var(--muted); font-size: 12px; }}
</style>
<h1>Maria — Candidate Matches</h1>
<div class="sub">Precomputed matches (local run). Generated {esc(timestamp)}.</div>
<div class="tools">
  <input id="q" type="search" placeholder="Filter by MP, UID, eye, race, why…" />
  <span class="pill">rows: {len(df)}</span>
</div>
<table id="t">
  <thead>
    <tr>
      <th>MP ID</th>
      <th>UID ID</th>
      <th>Score</th>
      <th>MP Sex</th>
      <th>MP Eyes</th>
      <th>MP Race</th>
      <th>UID Sex</th>
      <th>UID Eyes</th>
      <th>UID Race</th>
      <th>Why</th>
    </tr>
  </thead>
  <tbody>
    {''.join(table_rows)}
  </tbody>
</table>
<div class="foot">Tip: type in the search box to filter rows client-side.</div>
<script>
  const q = document.getElementById('q');
  const t = document.getElementById('t').getElementsByTagName('tbody')[0];
  q.addEventListener('input', () => {{
    const term = q.value.toLowerCase();
    for (const tr of t.rows) {{
      const txt = tr.innerText.toLowerCase();
      tr.style.display = term && !txt.includes(term) ? 'none' : '';
    }}
  }});
</script>
</html>
"""

out_path = os.path.join(OUT, "report.html")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(html_doc)
print("Wrote", out_path)
