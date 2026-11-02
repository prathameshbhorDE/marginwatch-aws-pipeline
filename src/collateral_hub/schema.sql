-- MarginWatch - a small stand-in for the Collateral Hub operational database (OLTP).
-- Every table has last_modified, kept current by a trigger: that column is what the
-- watermark (incremental / CDC) pull reads.

CREATE TABLE IF NOT EXISTS agreements (
    agreement_id        TEXT PRIMARY KEY,
    fund_id             TEXT        NOT NULL,
    cpty_id             TEXT        NOT NULL,
    cpty_short_name     TEXT        NOT NULL,
    cpty_legal_name     TEXT        NOT NULL,
    agreement_type      TEXT        NOT NULL CHECK (agreement_type IN ('ISDA', 'GMRA')),
    base_ccy            TEXT        NOT NULL,
    threshold           NUMERIC(18,2) NOT NULL,
    mta                 NUMERIC(18,2) NOT NULL,
    im_fund             NUMERIC(18,2) NOT NULL,
    im_cpty             NUMERIC(18,2) NOT NULL,
    last_modified       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS collateral_balances (
    balance_id              BIGSERIAL PRIMARY KEY,
    agreement_id            TEXT        NOT NULL REFERENCES agreements(agreement_id),
    as_of_date              DATE        NOT NULL,
    collateral_type         TEXT        NOT NULL CHECK (collateral_type IN ('CASH', 'BOND')),
    isin                    TEXT        NOT NULL DEFAULT '',
    quantity                NUMERIC(20,2) NOT NULL,
    price                   NUMERIC(12,4) NOT NULL,
    haircut_pct             NUMERIC(6,2)  NOT NULL,
    currency                TEXT        NOT NULL,
    market_value_usd        NUMERIC(18,2) NOT NULL,
    post_haircut_value_usd  NUMERIC(18,2) NOT NULL,
    last_modified           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (agreement_id, as_of_date, collateral_type, isin)
);

-- Index so "WHERE last_modified > :watermark" does not scan the whole table every run.
CREATE INDEX IF NOT EXISTS ix_agreements_last_modified ON agreements (last_modified);
CREATE INDEX IF NOT EXISTS ix_balances_last_modified   ON collateral_balances (last_modified);

-- Any UPDATE stamps the row with the current time, so changed rows are picked up next run.
CREATE OR REPLACE FUNCTION touch_last_modified() RETURNS trigger AS $$
BEGIN
    NEW.last_modified := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_agreements_touch ON agreements;
CREATE TRIGGER trg_agreements_touch BEFORE UPDATE ON agreements
    FOR EACH ROW EXECUTE FUNCTION touch_last_modified();

DROP TRIGGER IF EXISTS trg_balances_touch ON collateral_balances;
CREATE TRIGGER trg_balances_touch BEFORE UPDATE ON collateral_balances
    FOR EACH ROW EXECUTE FUNCTION touch_last_modified();
