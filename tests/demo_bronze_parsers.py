"""Parse every landed file of one business day into Bronze rows (pure Python, no AWS) and show samples."""
import collections, os, sys
sys.path.insert(0, "src/bronze")
from parsers import pick_parser, file_identity, SchemaError

ROOT = sys.argv[1] if len(sys.argv) > 1 else "sample_data/landing"
tables, notes, errors = collections.defaultdict(list), collections.Counter(), []
for source in sorted(os.listdir(ROOT)):
    for name in sorted(os.listdir(os.path.join(ROOT, source))):
        table, parser = pick_parser(source, name)
        if parser is None:
            continue
        try:
            rows, n = parser(open(os.path.join(ROOT, source, name), "rb").read())
        except SchemaError as e:
            errors.append((name, str(e)))
            continue
        for note in n:
            notes[note.split(":")[0].split(" row")[0]] += 1
        ident = file_identity(name) if source == "email" else {}
        for r in rows:
            tables[table].append({"source_file": name, **ident, **r})

print("Bronze tables built from one business day of files:")
for t, rows in sorted(tables.items()):
    print(f"  {t:26} {len(rows):5} rows")
print("\nParser notes:", dict(notes))
print("Quarantined files:", errors or "none")

def show(table, n=3, where=None):
    rows = [r for r in tables[table] if where is None or where(r)][:n]
    print(f"\n{table} (first {len(rows)} rows):")
    for r in rows:
        print("  ", {k: v for k, v in r.items()})

show("bronze_cpty_mtm", where=lambda r: r["cpty_id"] == "CP001")
show("bronze_cpty_mtm", n=2, where=lambda r: r["source_file"].endswith(".pdf"))
dup = [r for r in tables["bronze_cpty_mtm"] if r["cpty_id"] == "CP094"]
print(f"\nTOKYO TANSHI rows in Bronze: {len(dup)} (one is the planted duplicate; Bronze keeps it, Silver removes it)")
show("bronze_cpty_margin_call", n=1)
show("bronze_pb_margin", n=1)
