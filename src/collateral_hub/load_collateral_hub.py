"""
MarginWatch - load the synthetic Collateral Hub database, and simulate a day of changes.

  python src/collateral_hub/load_collateral_hub.py --dsn postgresql://localhost/collateral_hub --init
  python src/collateral_hub/load_collateral_hub.py --dsn ... --simulate-day 2026-10-27

--init           create tables and load agreements + 2026-10-26 balances from sample_data
--simulate-day   what operations staff would do in Collateral Hub the next day:
                 * GS INTL ISDA renegotiates its MTA from 250,000 to 500,000   (an UPDATE)
                 * new collateral balances for the new date for 3 agreements  (INSERTs)
                 * one balance is corrected after a price fix                 (an UPDATE)
"""
import argparse, csv, os
import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..", "..")
AGREEMENT_COLS = ["agreement_id", "fund_id", "cpty_id", "cpty_short_name", "cpty_legal_name",
                  "agreement_type", "base_ccy", "threshold", "mta", "im_fund", "im_cpty"]
BALANCE_COLS = ["agreement_id", "as_of_date", "collateral_type", "isin", "quantity", "price",
                "haircut_pct", "currency", "market_value_usd", "post_haircut_value_usd"]


def init(conn, sample_root):
    with open(os.path.join(HERE, "schema.sql")) as f:
        conn.execute(f.read())
    with open(os.path.join(sample_root, "agreements.csv"), encoding="utf-8") as f:
        rows = [[r[c] for c in AGREEMENT_COLS] for r in csv.DictReader(f)]
    with conn.cursor() as cur:
        cur.executemany(f"INSERT INTO agreements ({','.join(AGREEMENT_COLS)}) VALUES "
                        f"({','.join(['%s'] * len(AGREEMENT_COLS))}) ON CONFLICT (agreement_id) DO NOTHING", rows)
    bal_file = next(n for n in sorted(os.listdir(sample_root)) if n.startswith("ch_balances_"))
    with open(os.path.join(sample_root, bal_file), encoding="utf-8") as f:
        rows = [[r[c] for c in BALANCE_COLS] for r in csv.DictReader(f)]
    with conn.cursor() as cur:
        cur.executemany(f"INSERT INTO collateral_balances ({','.join(BALANCE_COLS)}) VALUES "
                        f"({','.join(['%s'] * len(BALANCE_COLS))}) "
                        "ON CONFLICT (agreement_id, as_of_date, collateral_type, isin) DO NOTHING", rows)
    conn.commit()
    n_a = conn.execute("SELECT count(*) FROM agreements").fetchone()[0]
    n_b = conn.execute("SELECT count(*) FROM collateral_balances").fetchone()[0]
    print(f"loaded: agreements={n_a} collateral_balances={n_b}")


def simulate_day(conn, new_date):
    with conn.cursor() as cur:
        cur.execute("UPDATE agreements SET mta = 500000 WHERE agreement_id = 'ORION-MF-CP001-ISDA'")
        cur.execute("""
            INSERT INTO collateral_balances (agreement_id, as_of_date, collateral_type, isin, quantity, price,
                                             haircut_pct, currency, market_value_usd, post_haircut_value_usd)
            SELECT agreement_id, %s::date, collateral_type, isin, quantity, price, haircut_pct, currency,
                   market_value_usd, post_haircut_value_usd
            FROM collateral_balances
            WHERE agreement_id IN ('ORION-MF-CP001-ISDA', 'ORION-MF-CP005-ISDA', 'ORION-MF-CP094-GMRA')
              AND as_of_date = (SELECT max(as_of_date) FROM collateral_balances WHERE as_of_date < %s::date)
            ON CONFLICT (agreement_id, as_of_date, collateral_type, isin) DO NOTHING""", (new_date, new_date))
        inserted = cur.rowcount
        cur.execute("""
            UPDATE collateral_balances
               SET price = price - 0.5,
                   market_value_usd = round(quantity * (price - 0.5) / 100, 2),
                   post_haircut_value_usd = round(quantity * (price - 0.5) / 100 * (1 - haircut_pct / 100), 2)
             WHERE balance_id = (SELECT min(balance_id) FROM collateral_balances
                                  WHERE collateral_type = 'BOND' AND agreement_id = 'ORION-MF-CP045-ISDA')""")
    conn.commit()
    print(f"simulated {new_date}: 1 agreement updated (MTA 250k -> 500k), {inserted} balances inserted, 1 balance corrected")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.environ.get("CH_DSN", "postgresql://localhost/collateral_hub"))
    ap.add_argument("--init", action="store_true")
    ap.add_argument("--simulate-day")
    ap.add_argument("--sample-root", default=os.path.join(ROOT, "sample_data", "landing", "collateral_hub"))
    a = ap.parse_args()
    with psycopg.connect(a.dsn) as conn:
        if a.init:
            init(conn, a.sample_root)
        if a.simulate_day:
            simulate_day(conn, a.simulate_day)
