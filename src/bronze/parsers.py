"""
MarginWatch - Bronze parsers: turn each landed file into rows, WITHOUT cleaning the values.

Bronze rule: keep what the sender sent. Every value stays text exactly as in the file
("5,000,000.00", "26-Oct-2026", duplicates included). Only the *structure* is fixed:
find the header row, skip banner/footer lines, read every page. Cleaning happens in Silver.

Each function takes the file's bytes and returns (rows, notes):
  rows  = list of dicts, one per data row
  notes = what the parser skipped or noticed (kept for the audit trail)
"""
import csv, io

import openpyxl
import pdfplumber

MTM_HEADER = ["Trade Ref", "Product", "Notional", "Currency", "MTM (Cpty view)", "Agreement"]
MTM_PDF_HEADER = ["Trade Ref", "Product", "Notional", "CCY", "MTM"]
PB_HEADER = ["Account", "Account Type", "Total Equity (USD)", "Total Requirement (USD)", "Excess/Deficit (USD)"]


class SchemaError(Exception):
    """The file does not have the columns we expect -> the file goes to quarantine, not Bronze."""


def _text(v):
    return "" if v is None else str(v).strip()


def file_identity(file_name):
    """CP001_GSINTL_ISDA_MTM_20261026.xlsx -> cpty_id CP001, agreement_type ISDA."""
    bits = file_name.split("_")
    return {"cpty_id": bits[0], "agreement_type": bits[2] if len(bits) > 2 and bits[2] in ("ISDA", "GMRA") else "PB"}


def parse_mtm_xlsx(data):
    ws = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True).active
    rows, notes, header_at, header = [], [], None, None
    for i, raw in enumerate(ws.iter_rows(values_only=True), start=1):
        cells = [_text(v) for v in raw]
        if header is None:
            if cells and cells[0] == "Trade Ref":
                header, header_at = cells, i
            continue
        if not any(cells):
            continue                                            # blank spacer line
        if cells[0].lower().startswith("total"):
            notes.append(f"skipped footer row {i}: {cells[0]}")
            continue
        rows.append(dict(zip(MTM_HEADER, cells[:len(MTM_HEADER)]), source_row=i))
    if header is None:
        raise SchemaError("no 'Trade Ref' header row found")
    if header[:len(MTM_HEADER)] != MTM_HEADER:
        raise SchemaError(f"unexpected columns: {header}")
    notes.insert(0, f"header found on row {header_at}")
    return rows, notes


def parse_mtm_pdf(data):
    rows, notes = [], []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for p, page in enumerate(pdf.pages, start=1):
            for t in page.extract_tables():
                for r in t:
                    cells = [_text(v) for v in r]
                    if cells == MTM_PDF_HEADER:
                        continue                                # header repeats on every page
                    if len(cells) != len(MTM_PDF_HEADER):
                        raise SchemaError(f"page {p}: unexpected row shape {cells}")
                    rows.append({"Trade Ref": cells[0], "Product": cells[1], "Notional": cells[2],
                                 "Currency": cells[3], "MTM (Cpty view)": cells[4], "Agreement": "",
                                 "source_row": f"p{p}"})
        notes.append(f"read {len(pdf.pages)} page(s)")
    if not rows:
        raise SchemaError("no trade rows found in PDF")
    return rows, notes


def parse_margin_call_pdf(data):
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        page = pdf.pages[0]
        text_lines = (page.extract_text() or "").splitlines()
        table = page.extract_tables()[0]
    fields = {_text(k): _text(v) for k, v in table[1:]}         # skip the "Field | Value" header
    fields["From"] = next((l[5:].strip() for l in text_lines if l.startswith("From:")), "")
    needed = ["Agreement", "Exposure (USD)", "IM - client (USD)", "IM - counterparty (USD)",
              "Collateral held (USD, post-haircut)", "Call / Recall (USD)"]
    missing = [k for k in needed if k not in fields]
    if missing:
        raise SchemaError(f"margin call notice missing fields: {missing}")
    return [fields], [f"{len(fields)} fields read"]


def parse_pb_xlsx(data):
    ws = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True).active
    rows, header = [], None
    for i, raw in enumerate(ws.iter_rows(values_only=True), start=1):
        cells = [_text(v) for v in raw]
        if header is None:
            if cells and cells[0] == "Account":
                header = cells
            continue
        if any(cells):
            rows.append(dict(zip(PB_HEADER, cells[:len(PB_HEADER)]), source_row=i))
    if header is None or header[:len(PB_HEADER)] != PB_HEADER:
        raise SchemaError("prime broker report: header not found or changed")
    return rows, []


def parse_csv(data):
    rows = list(csv.DictReader(io.StringIO(data.decode("utf-8"))))
    for i, r in enumerate(rows, start=2):
        r["source_row"] = i
    return rows, []


def pick_parser(source, file_name):
    """Which parser for which file, and which Bronze table it feeds."""
    if source == "email" and file_name.startswith("PB"):
        return "bronze_pb_margin", parse_pb_xlsx
    if source == "email" and "_MarginCall_" in file_name:
        return "bronze_cpty_margin_call", parse_margin_call_pdf
    if source == "email" and "_MTM_" in file_name:
        return "bronze_cpty_mtm", parse_mtm_xlsx if file_name.endswith(".xlsx") else parse_mtm_pdf
    if source == "agent_bank":
        return ("bronze_bank_trades_mtm" if file_name.startswith("trades_mtm") else "bronze_bank_holdings"), parse_csv
    if source == "collateral_hub":
        return ("bronze_ch_agreements" if file_name.startswith("agreements") else "bronze_ch_balances"), parse_csv
    if source == "reference":
        return "bronze_fx_rates", parse_csv
    return None, None
