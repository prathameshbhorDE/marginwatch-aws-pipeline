"""
MarginWatch - counterparty identity matching (the MDM idea, applied to collateral).
Different sources name the same counterparty differently. We map every name to ONE master id (cpty_id).
  1. normalise the text (case, punctuation, common abbreviations)
  2. exact match on the normalised name            -> confident
  3. otherwise fuzzy match (similarity score)        -> accept only if very high AND clearly better than #2
  4. otherwise send to a manual-review queue         -> a human decides, never guess
"""
import csv, difflib, re

ABBREV = {"INTL": "INTERNATIONAL", "SECS": "SECURITIES", "BK": "BANK", "LDN": "LONDON", "CO": "COMPANY",
          "&": "AND", "NA": "NATIONAL ASSOCIATION"}

def normalise(name):
    name = name.upper().replace("&", " & ")
    name = re.sub(r"[^A-Z0-9& ]", " ", name)          # drop punctuation: "N.A." -> "N A"
    name = re.sub(r"\bN A\b", "NA", name)
    words = [ABBREV.get(w, w) for w in name.split()]
    return " ".join(words)

def load_master(path="config/counterparties.csv"):
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return {normalise(r["legal_name"]): r for r in rows}

def match(raw_name, master, accept=0.92, margin=0.04):
    n = normalise(raw_name)
    if n in master:
        return master[n]["cpty_id"], "EXACT", 1.0, master[n]["legal_name"]
    scored = sorted(((difflib.SequenceMatcher(None, n, k).ratio(), k) for k in master), reverse=True)
    (best, key), (second, _) = scored[0], scored[1]
    if best >= accept and best - second >= margin:
        return master[key]["cpty_id"], "FUZZY", round(best, 3), master[key]["legal_name"]
    return None, f"REVIEW (best {best:.3f} vs next {second:.3f})", round(best, 3), master[key]["legal_name"]

if __name__ == "__main__":
    master = load_master()
    incoming = [  # how names really show up in emails, portal reports and old spreadsheets
        ("email subject",   "GOLDMAN SACHS INTERNATIONAL"),
        ("email signature", "Goldman Sachs Intl"),
        ("agent bank CSV",  "JPMorgan Chase Bank NA"),
        ("old spreadsheet", "Deutsche Bank AG London Branch"),
        ("email signature", "Ueda Yagi Tanshi Co Ltd"),
        ("PDF notice",      "Morgan Stanley and Co. Intl PLC"),
        ("email signature", "Morgan Stanley & Co"),
        ("portal report",   "Tokyo Tanshi"),
    ]
    print(f"{'source':16} | {'name as received':32} | {'cpty_id':7} | {'match':30} | {'score':5} | best master candidate")
    for src, raw in incoming:
        cid, how, score, cand = match(raw, master)
        print(f"{src:16} | {raw:32} | {cid or '-':7} | {how:30} | {score:<5} | {cand}")
