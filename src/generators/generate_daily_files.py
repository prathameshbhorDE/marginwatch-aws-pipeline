"""
MarginWatch - Piece 1: synthetic daily source files for one business date.

Counterparty names come from config/counterparties.csv (200 real institution names used ONLY as
labels - every trade, MTM, balance and call amount is randomly generated).

Channels produced:
  landing/email/            per counterparty agreement: trade-level MTM (Excel or PDF), plus a
                            margin call notice (PDF) only when the counterparty calls;
                            prime broker margin report (Excel); _email_metadata.jsonl
  landing/agent_bank/       trades & MTM report (CSV), securities holdings statement (CSV)
  landing/collateral_hub/   collateral balances extract (CSV), agreement terms (CSV)
  landing/reference/        FX rates (CSV)
  expected/                 answer key: the margin summary the pipeline SHOULD produce

Usage:
  python src/generators/generate_daily_files.py                     # business date 2026-10-26
  python src/generators/generate_daily_files.py --date 2026-10-27 --out ./data
"""
import argparse, csv, io, json, os, random, re, zipfile
from datetime import date, datetime, timedelta, timezone

from openpyxl import Workbook
from openpyxl.styles import Font
from reportlab import rl_config
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

FUND_ID, FUND_NAME = "ORION-MF", "Orion Capital Master Fund"
APPROVAL_THRESHOLD = {"ISDA": 2_500_000, "GMRA": 5_000_000}   # client confirms calls at/above this
TERMS = {"ISDA": {"threshold": 1_000_000, "mta": 250_000},
         "GMRA": {"threshold": 0,         "mta": 500_000}}
FX_TO_USD = {"USD": 1.0, "EUR": 1.08, "GBP": 1.29, "JPY": 0.0066}
PRODUCTS = {"ISDA": ["IRS", "CCS", "FX_FWD", "TRS", "CDS"], "GMRA": ["REPO", "REVERSE_REPO"]}
CUTOFF_DUBLIN = (12, 30)
rl_config.invariant = 1          # PDFs: no creation timestamp/random id, so same content = same bytes
HERE = os.path.dirname(os.path.abspath(__file__))
CPTY_MASTER = os.path.join(HERE, "..", "..", "config", "counterparties.csv")

# Deliberate scenarios on named agreements so every pipeline rule is exercised every day.
# target_net = collateral held (post-haircut) - requirement. Negative = fund pays, positive = fund recalls.
PLANTED = {
    ("GS INTL", "ISDA"): {"target_net": -3_200_000, "mtm_break": True, "trades": 400},   # big call + MTM break + skew
    ("JPM PLC", "ISDA"): {"target_net":   -800_000, "position_break": True},           # small call + position breaks
    ("MS INTL", "ISDA"): {"target_net":   -120_000},                                   # below MTA
    ("DB LDN", "ISDA"):  {"target_net":  1_400_000, "mtm_fmt": "pdf"},                 # recall, MTM sent as PDF
    ("TOKYO TANSHI", "GMRA"): {"target_net": -6_100_000, "duplicate": True},           # big GMRA call + duplicate row
    ("UEDA TANSHI", "GMRA"):  {"target_net":  2_300_000, "late": True},                # recall, file after cut-off
}
PRIME_BROKER = {"cpty_id": "PB01", "short_name": "MS PB", "legal_name": "Morgan Stanley & Co. LLC (Prime Brokerage)"}


# ---------- small helpers (plain Python practice) ----------
def isin_check_digit(body11):
    """ISIN check digit: letters -> numbers (A=10..Z=35), then Luhn over the digit string."""
    digits = "".join(str(int(ch, 36)) for ch in body11)
    total = 0
    for i, d in enumerate(reversed(digits)):
        n = int(d)
        if i % 2 == 0:
            n *= 2
            n = n - 9 if n > 9 else n
        total += n
    return str((10 - total % 10) % 10)

def make_isin(country="XS"):
    body = country + "".join(random.choice("0123456789") for _ in range(9))
    return body + isin_check_digit(body)

def money(x):
    return f"{x:,.2f}"

def slug(text):
    return "".join(ch for ch in text if ch.isalnum())

def load_counterparties(path):
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---------- build the "truth": agreements, trades, collateral ----------
def build_agreements(cptys):
    agreements = []
    for c in cptys:
        for agr in c["agreements"].split("|"):
            plan = PLANTED.get((c["short_name"], agr), {})
            a = {**c, "agr": agr, "agreement_id": f"{FUND_ID}-{c['cpty_id']}-{agr}", **TERMS[agr], "plan": plan}
            a["im_fund"] = random.choice([0, 0, 250_000, 500_000, 1_000_000]) if agr == "ISDA" else 0
            a["im_cpty"] = random.choice([0, 0, 0, 100_000, 250_000]) if agr == "ISDA" else 0
            a["target_net"] = plan.get("target_net", random.choice([-1, 1]) * random.randint(20_000, 7_000_000))
            a["n_trades"] = plan.get("trades", random.randint(2, 12))
            a["mtm_fmt"] = plan.get("mtm_fmt", "pdf" if random.random() < 0.25 else "xlsx")
            agreements.append(a)
    return agreements

def build_trades(a):
    """Fund-perspective trades. Biased negative so the fund owes collateral (typical hedge fund vs dealer)."""
    scale = 1 / max(a["n_trades"] / 8, 1)          # big books: smaller MTM per trade, same order of total
    trades = []
    for i in range(1, a["n_trades"] + 1):
        ccy = random.choice(["USD", "USD", "EUR", "GBP", "JPY"] if a["country"] == "JP" else ["USD", "USD", "EUR", "GBP"])
        mtm_usd = random.uniform(-2_400_000, 700_000) * scale
        trades.append({"fund_id": FUND_ID, "agreement_id": a["agreement_id"], "cpty_id": a["cpty_id"],
                       "trade_id": f"{a['cpty_id']}-{a['agr'][0]}{i:04d}", "product": random.choice(PRODUCTS[a["agr"]]),
                       "notional": random.choice([5, 10, 25, 50]) * 1_000_000, "ccy": ccy,
                       "mtm": round(mtm_usd / FX_TO_USD[ccy], 2)})
    return trades

def to_usd(rows):
    return round(sum(r["mtm"] * FX_TO_USD[r["ccy"]] for r in rows), 2)

def requirement_usd(a, fund_exposure_usd):
    """Simplified credit support amount owed BY the fund:
       max(-exposure - threshold, 0) + IM posted by fund - IM posted by counterparty, floored at 0."""
    return round(max(max(-fund_exposure_usd - a["threshold"], 0) + a["im_fund"] - a["im_cpty"], 0), 2)

def split_collateral(a, held_target_usd):
    """Held collateral = cash + two bonds whose post-haircut value adds up to the target."""
    cash = round(held_target_usd * random.uniform(0.3, 0.6), 2)
    remaining = held_target_usd - cash
    ch = [{"agreement_id": a["agreement_id"], "collateral_type": "CASH", "isin": "", "quantity": cash, "price": 1.0,
           "haircut_pct": 0.0, "currency": "USD", "market_value_usd": cash, "post_haircut_value_usd": cash}]
    custody = []
    for _ in range(2):
        hc = random.choice([2.0, 3.0, 5.0])
        price = round(random.uniform(95, 102), 4)
        nominal = round(remaining / 2 / (1 - hc / 100) / (price / 100), -3)   # bonds held in round nominal
        mv = round(nominal * price / 100, 2)
        isin = make_isin()
        ch.append({"agreement_id": a["agreement_id"], "collateral_type": "BOND", "isin": isin, "quantity": nominal,
                   "price": price, "haircut_pct": hc, "currency": "USD", "market_value_usd": mv,
                   "post_haircut_value_usd": round(mv * (1 - hc / 100), 2)})
        custody.append({"account": f"{FUND_ID}-PLEDGED-{a['cpty_id']}-{a['agr']}", "isin": isin,
                        "security_name": f"Govt Bond {random.randint(2028, 2040)} {random.choice([2, 3, 4])}%",
                        "nominal": nominal, "price": price, "market_value": mv, "currency": "USD",
                        "pledged_to": a["cpty_id"]})
    return ch, custody


# ---------- writers, one per real-world file type ----------
def save_xlsx(wb, path, bdate):
    """An .xlsx is a zip. openpyxl stamps 'modified = now' inside it and the zip stamps each member
    with the current time, so identical content gives different bytes (and a different SHA-256).
    Rewrite the zip with a fixed time so the same content always gives the same bytes."""
    fixed = datetime(bdate.year, bdate.month, bdate.day, 23, 0)
    wb.properties.created = fixed
    buf = io.BytesIO()
    wb.save(buf)
    stamp = fixed.strftime("%Y-%m-%dT%H:%M:%SZ")
    with zipfile.ZipFile(io.BytesIO(buf.getvalue())) as src, \
         zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as dst:
        for item in src.infolist():
            data = src.read(item.filename)
            if item.filename == "docProps/core.xml":
                data = re.sub(rb"(<dcterms:modified[^>]*>)[^<]*", rb"\g<1>" + stamp.encode(), data)
            info = zipfile.ZipInfo(item.filename, date_time=fixed.timetuple()[:6])
            info.compress_type = zipfile.ZIP_DEFLATED
            dst.writestr(info, data)

def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)

def write_mtm_xlsx(path, a, bdate, rows):
    """Counterparty-style Excel: banner rows above the header, merged title, footer total row."""
    wb = Workbook(); ws = wb.active; ws.title = "Valuations"
    ws["A1"] = f"{a['legal_name']} - Collateral Valuation Statement"; ws["A1"].font = Font(bold=True, size=12)
    ws.merge_cells("A1:F1")
    ws["A2"] = f"Client: {FUND_NAME}"; ws["A3"] = f"Agreement: {a['agr']}   Valuation date: {bdate:%d-%b-%Y}"
    ws.append([])
    ws.append(["Trade Ref", "Product", "Notional", "Currency", "MTM (Cpty view)", "Agreement"])
    for r in rows:
        ws.append([r["trade_id"], r["product"], r["notional"], r["ccy"], r["mtm"], a["agreement_id"]])
    ws.append([]); ws.append(["Total (trade ccy, not converted)", "", "", "", round(sum(r["mtm"] for r in rows), 2)])
    save_xlsx(wb, path, bdate)

def pdf_doc(path, title, lines, table_rows):
    styles = getSampleStyleSheet()
    story = [Paragraph(title, styles["Title"])] + [Paragraph(l, styles["Normal"]) for l in lines] + [Spacer(1, 10)]
    t = Table(table_rows, repeatRows=1)
    t.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
                           ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey), ("FONTSIZE", (0, 0), (-1, -1), 8)]))
    SimpleDocTemplate(path, pagesize=A4).build(story + [t])

def write_mtm_pdf(path, a, bdate, rows):
    table = [["Trade Ref", "Product", "Notional", "CCY", "MTM"]] + \
            [[r["trade_id"], r["product"], money(r["notional"]), r["ccy"], money(r["mtm"])] for r in rows]
    pdf_doc(path, f"{a['legal_name']} - Exposure Statement",
            [f"Client: {FUND_NAME}", f"Agreement: {a['agreement_id']} ({a['agr']})", f"As of: {bdate:%d %B %Y}"], table)

def write_call_notice_pdf(path, a, bdate, exposure, held, call):
    """The four things a real notice carried: exposure, IM (client and cpty), collateral held, call/recall."""
    direction = "CALL - please deliver" if call > 0 else "RECALL - we will return"
    table = [["Field", "Value"],
             ["Agreement", f"{a['agreement_id']} ({a['agr']})"],
             ["Valuation date", f"{bdate:%d %B %Y}"],
             ["Exposure (USD)", money(exposure)],
             ["IM - client (USD)", money(a["im_fund"])],
             ["IM - counterparty (USD)", money(a["im_cpty"])],
             ["Collateral held (USD, post-haircut)", money(held)],
             ["Call / Recall (USD)", f"{money(abs(call))}  {direction}"],
             ["Settlement", f"T+1 ({bdate + timedelta(days=1):%d %B %Y})"]]
    pdf_doc(path, "Margin Call Notice", [f"From: {a['legal_name']}", f"To: {FUND_NAME}"], table)

def write_pb_report_xlsx(path, bdate, accounts):
    wb = Workbook(); ws = wb.active; ws.title = "Margin"
    ws["A1"] = f"{PRIME_BROKER['legal_name']} - Daily Margin Report ({bdate:%Y-%m-%d})"; ws["A1"].font = Font(bold=True)
    ws.append(["Account", "Account Type", "Total Equity (USD)", "Total Requirement (USD)", "Excess/Deficit (USD)"])
    for acc in accounts:
        ws.append([acc["account"], acc["type"], acc["equity"], acc["requirement"], round(acc["equity"] - acc["requirement"], 2)])
    save_xlsx(wb, path, bdate)

def received_time(bdate, late):
    """Next-day arrival in Dublin local time. Europe/Dublin = UTC+0 after the last Sunday of October,
    so for this demo date Dublin == UTC. (The pipeline itself must use zoneinfo, not this shortcut.)"""
    h, m = CUTOFF_DUBLIN
    minutes = random.randint(h * 60 + m + 1, 15 * 60) if late else random.randint(3 * 60, h * 60 + m)
    d = bdate + timedelta(days=1)
    return datetime(d.year, d.month, d.day, minutes // 60, minutes % 60, tzinfo=timezone.utc)


# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-10-26", help="business (COB) date, YYYY-MM-DD")
    ap.add_argument("--out", default="data")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--late-rate", type=float, default=0.06, help="share of counterparties sending after cut-off")
    args = ap.parse_args()
    random.seed(args.seed)
    bdate = date.fromisoformat(args.date)
    d = f"{bdate:%Y%m%d}"
    paths = {k: os.path.join(args.out, "landing", k) for k in ("email", "agent_bank", "collateral_hub", "reference")}
    for p in list(paths.values()) + [os.path.join(args.out, "expected")]:
        os.makedirs(p, exist_ok=True)

    agreements = build_agreements(load_counterparties(CPTY_MASTER))
    late_cptys = {c for c in sorted({a["cpty_id"] for a in agreements}) if random.random() < args.late_rate}   # sorted: set order changes per run
    all_trades, ch_balances, custody, expected, emails = [], [], [], [], []

    for a in agreements:
        plan = a["plan"]
        if plan:                          # planted scenario: re-draw (bounded) until the deficit fits
            for attempt in range(500):
                trades = build_trades(a)
                exposure = to_usd(trades)
                req = requirement_usd(a, exposure)
                if req + a["target_net"] >= 1_000_000:
                    break
            else:
                raise RuntimeError(f"cannot fit planted scenario for {a['agreement_id']}")
        else:                             # random agreement: pick a target that is feasible for THIS book
            trades = build_trades(a)
            exposure = to_usd(trades)
            req = requirement_usd(a, exposure)
            a["target_net"] = round(random.uniform(-0.6, 0.4) * req, 2) if req > 0 else round(random.uniform(0, 3e6), 2)
        ch_rows, custody_rows = split_collateral(a, round(req + a["target_net"], 2))
        held = round(sum(r["post_haircut_value_usd"] for r in ch_rows), 2)
        net = round(held - req, 2)
        all_trades += trades; ch_balances += ch_rows; custody += custody_rows

        if abs(net) < a["mta"]:
            summary, action = "Below MTA", "No action"
        else:
            summary = "Margin deficit - pay" if net < 0 else "Margin excess - recall"
            action = "Client approval required" if abs(net) >= APPROVAL_THRESHOLD[a["agr"]] else "Inform client"
        expected.append({"business_date": bdate.isoformat(), "fund_id": FUND_ID, "agreement_id": a["agreement_id"],
                         "cpty_id": a["cpty_id"], "cpty_short_name": a["short_name"], "agreement_type": a["agr"],
                         "exposure_usd": exposure, "im_fund_usd": a["im_fund"], "im_cpty_usd": a["im_cpty"],
                         "total_requirement_usd": req, "collateral_post_haircut_usd": held,
                         "net_excess_deficit_usd": net, "mta_usd": a["mta"], "summary": summary, "action": action})

        # Counterparty's statement: sign flipped + up to 1% model noise, plus planted problems
        cp_rows = [dict(t, mtm=round(-t["mtm"] * (1 + random.uniform(-0.01, 0.01)), 2)) for t in trades]
        if plan.get("mtm_break"):
            cp_rows[1]["mtm"] = round(cp_rows[1]["mtm"] * 1.25, 2)
        if plan.get("position_break"):
            cp_rows.pop()
            cp_rows.append(dict(cp_rows[0], trade_id=f"{a['cpty_id']}-X901", notional=1_000_000, mtm=48_750.00))
        if plan.get("duplicate"):
            cp_rows.append(dict(cp_rows[0]))

        base = f"{a['cpty_id']}_{slug(a['short_name'])}_{a['agr']}"
        files = [os.path.join(paths["email"], f"{base}_MTM_{d}.{a['mtm_fmt']}")]
        (write_mtm_xlsx if a["mtm_fmt"] == "xlsx" else write_mtm_pdf)(files[0], a, bdate, cp_rows)
        if net <= -a["mta"]:              # counterparty only sends a notice when IT is owed collateral
            cp_exposure = to_usd(cp_rows)  # cpty view: + = fund owes cpty
            cp_req = max(max(cp_exposure - a["threshold"], 0) + a["im_fund"] - a["im_cpty"], 0)
            files.append(os.path.join(paths["email"], f"{base}_MarginCall_{d}.pdf"))
            write_call_notice_pdf(files[1], a, bdate, cp_exposure, held, round(cp_req - held, 2))

        rec = received_time(bdate, plan.get("late", False) or a["cpty_id"] in late_cptys)
        for f in files:
            emails.append({"message_id": f"<{a['cpty_id']}.{a['agr']}.{d}.{len(emails)}@mail.example>",
                           "from": f"collateral.ops@{slug(a['short_name']).lower()}.example",
                           "subject": f"{a['short_name']} | {FUND_NAME} | {a['agr']} | {bdate:%d/%m/%Y}",
                           "received_at_utc": rec.isoformat(), "attachment": os.path.basename(f),
                           "size_bytes": os.path.getsize(f), "business_date": bdate.isoformat()})

    # Prime broker section: PB calculates equity and requirement itself
    pb = [{"account": f"{FUND_ID}-PB-{t}", "type": t, "equity": round(random.uniform(5e6, 40e6), 2),
           "requirement": round(random.uniform(3e6, 38e6), 2)} for t in ("CASH", "SWAP", "MARGIN")]
    pb_path = os.path.join(paths["email"], f"PB01_MSPB_MarginReport_{d}.xlsx")
    write_pb_report_xlsx(pb_path, bdate, pb)
    emails.append({"message_id": f"<PB01.{d}@mail.example>", "from": "pb.margin@mspb.example",
                   "subject": f"Daily Margin Report {bdate:%Y-%m-%d}", "received_at_utc": received_time(bdate, False).isoformat(),
                   "attachment": os.path.basename(pb_path), "size_bytes": os.path.getsize(pb_path),
                   "business_date": bdate.isoformat()})
    for acc in pb:
        net = round(acc["equity"] - acc["requirement"], 2)
        expected.append({"business_date": bdate.isoformat(), "fund_id": FUND_ID, "agreement_id": acc["account"],
                         "cpty_id": "PB01", "cpty_short_name": PRIME_BROKER["short_name"], "agreement_type": "PB",
                         "exposure_usd": "", "im_fund_usd": "", "im_cpty_usd": "",
                         "total_requirement_usd": acc["requirement"], "collateral_post_haircut_usd": acc["equity"],
                         "net_excess_deficit_usd": net, "mta_usd": 0,
                         "summary": "Account in margin excess" if net >= 0 else "Account in margin deficit",
                         "action": "Inform client"})

    # Planted custody issues: balance break vs Collateral Hub, invalid ISIN check digit
    custody[0]["nominal"] -= 50_000
    custody[0]["market_value"] = round(custody[0]["nominal"] * custody[0]["price"] / 100, 2)
    custody[2]["isin"] = custody[2]["isin"][:-1] + str((int(custody[2]["isin"][-1]) + 1) % 10)

    write_csv(os.path.join(paths["agent_bank"], f"trades_mtm_{d}.csv"),
              [dict(t, valuation_date=bdate.isoformat()) for t in all_trades])
    write_csv(os.path.join(paths["agent_bank"], f"securities_holdings_{d}.csv"),
              [dict(h, statement_date=bdate.isoformat()) for h in custody])
    write_csv(os.path.join(paths["collateral_hub"], f"ch_balances_{d}.csv"),
              [dict(r, as_of_date=bdate.isoformat()) for r in ch_balances])
    write_csv(os.path.join(paths["collateral_hub"], "agreements.csv"),
              [{"agreement_id": a["agreement_id"], "fund_id": FUND_ID, "cpty_id": a["cpty_id"],
                "cpty_short_name": a["short_name"], "cpty_legal_name": a["legal_name"], "agreement_type": a["agr"],
                "base_ccy": "USD", "threshold": a["threshold"], "mta": a["mta"], "im_fund": a["im_fund"],
                "im_cpty": a["im_cpty"], "effective_from": "2025-01-01", "effective_to": "9999-12-31"} for a in agreements])
    write_csv(os.path.join(paths["reference"], f"fx_rates_{d}.csv"),
              [{"rate_date": bdate.isoformat(), "ccy": k, "to_usd": v} for k, v in FX_TO_USD.items()])
    write_csv(os.path.join(args.out, "expected", f"expected_margin_summary_{d}.csv"), expected)
    with open(os.path.join(paths["email"], "_email_metadata.jsonl"), "w", encoding="utf-8") as f:
        f.writelines(json.dumps(e, ensure_ascii=False) + "\n" for e in emails)

    calls = sum(1 for e in expected if e["summary"].startswith("Margin deficit"))
    print(f"business_date={bdate} counterparties={len({a['cpty_id'] for a in agreements})} "
          f"agreements={len(agreements)} trades={len(all_trades)} email_files={len(emails)} "
          f"calls={calls} late_cptys={len(late_cptys) + 1} out={args.out}")


if __name__ == "__main__":
    main()
