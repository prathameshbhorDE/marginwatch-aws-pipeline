"""
MarginWatch - Lambda: the end-of-run email, called by Step Functions after Gold succeeds.

Reads the run summaries the three Glue jobs wrote to the artifacts bucket
(runs/bronze|silver|gold/business_date=<d>/<run_id>.json, newest one each) and turns them into one
short email for ops: what loaded, data quality, which agreements need client approval, disputes, breaks.

Input  (from Step Functions): {"business_date": "2026-10-26", "run_type": "CUTOFF_1230" | "UPDATE_1500" | "MANUAL_RERUN",
                               "seconds": {"bronze": 188, ...}}
If an earlier Gold run exists for the same day (e.g. 12:30 before the 15:00 update), the email also lists
what changed: statements that arrived late, approvals and disputes that appeared or went away, breaks.
Output: {"subject": "...", "message": "<plain text>", "html": "<html tables>"}
        -> Step Functions sends it with Amazon SES (formatted report). SNS is kept for alerts only.
"""
import html, json, os

import boto3

BUCKET = os.environ.get("ARTIFACTS_BUCKET", "")
_s3 = boto3.client("s3")


RUN_LABEL = {"CUTOFF_1230": "12:30 cut-off run", "UPDATE_1500": "15:00 UPDATE run (late files included)",
             "MANUAL_RERUN": "manual re-run"}


def summaries(layer, business_date):
    """All run summaries for the day, oldest first."""
    prefix = f"runs/{layer}/business_date={business_date}/"
    objs = []
    for page in _s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        objs += page.get("Contents", [])
    return [json.loads(_s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read())
            for o in sorted(objs, key=lambda o: o["LastModified"])]


def latest_summary(layer, business_date):
    runs = summaries(layer, business_date)
    return runs[-1] if runs else None


def changes_since(prev, cur):
    """What a later run of the same day changed, in words ops can act on."""
    p, c = prev["fact_margin_call"], cur["fact_margin_call"]
    lines = []
    no_stmt = lambda g: g["fact_trade_mtm"]["by_status"].get("NO_STATEMENT", 0)
    if no_stmt(prev) != no_stmt(cur):
        lines.append(f"  trades without a counterparty statement: {no_stmt(prev)} -> {no_stmt(cur)} (late statements now included)")
    for label, key, field in (("needs client approval", "client_approval_required", "agreement_id"),
                              ("call in dispute", "calls_not_agreed", "agreement_id")):
        before, after = {x[field] for x in p[key]}, {x[field] for x in c[key]}
        lines += [f"  NEW    {a}: {label}" for a in sorted(after - before)]
        lines += [f"  CLEARED {a}: no longer {label}" for a in sorted(before - after)]
    bb = {(x["agreement_id"], x["trade_id"]) for x in prev["breaks"]}
    ba = {(x["agreement_id"], x["trade_id"]) for x in cur["breaks"]}
    lines += [f"  NEW    trade break {a} {t}" for a, t in sorted(ba - bb)]
    lines += [f"  CLEARED trade break {a} {t}" for a, t in sorted(bb - ba)]
    return lines or ["  no change"]


def money(x):
    return f"{float(x):,.2f}" if x not in (None, "") else "-"


def build_report(business_date, seconds=None, run_type="CUTOFF_1230"):
    seconds = seconds or {}
    b, s = latest_summary("bronze", business_date), latest_summary("silver", business_date)
    gold_runs = [x for x in summaries("gold", business_date) if "fact_margin_call" in x]   # skip pre-margin-step runs
    g = gold_runs[-1] if gold_runs else None
    if not (b and s and g):
        missing = [n for n, v in (("bronze", b), ("silver", s), ("gold", g)) if not v]
        raise RuntimeError(f"run summary missing for {missing} on {business_date}")

    m = g["fact_margin_call"]
    approvals, disputes, breaks = m["client_approval_required"], m["calls_not_agreed"], g["breaks"]
    t = lambda k: f" ({seconds[k]} s)" if seconds.get(k) is not None else ""
    lines = [
        f"MarginWatch - business date {business_date} - {RUN_LABEL.get(run_type, run_type)}: SUCCEEDED", "",
        f"Bronze{t('bronze')}: {b['files_loaded']} of {b['files_in_landing']} files loaded, "
        f"{b['files_quarantined']} quarantined",
        f"Silver{t('silver')}: {s['dq']['checks']} quality checks, {len(s['dq']['fail'])} failed, {len(s['dq']['warn'])} warnings",
        f"Gold{t('gold')}: {m['rows']} agreements / PB accounts, {g['fact_trade_mtm']['rows']} trades reconciled", "",
        "Margin summary:"]
    lines += [f"  {k}: {v}" for k, v in m["by_summary_action"].items()]
    lines += ["", f"Client approval required ({len(approvals)}):"]
    lines += [f"  {a['cpty_short_name']} ({a['agreement_id']}): {money(a['net_excess_deficit_usd'])} USD, {a['summary']}"
              for a in approvals]
    lines += ["", f"Counterparty calls in dispute ({len(disputes)}):"]
    lines += [f"  {d['agreement_id']}: ours {money(d['net_excess_deficit_usd'])}, theirs {d['cpty_call_direction']} "
              f"{money(d['cpty_call_amount_usd'])}, difference {money(d['call_difference_usd'])}, trade breaks {d['trade_breaks']}"
              for d in disputes] or ["  none"]
    lines += ["", f"Trade breaks ({len(breaks)}):"]
    lines += [f"  {x['agreement_id']} {x['trade_id']}: {x['recon_status']}, ours {money(x['mtm_ours_usd'])}, "
              f"theirs {money(x['mtm_theirs_usd'])}" for x in breaks] or ["  none"]
    changes = changes_since(gold_runs[-2], g) if len(gold_runs) > 1 else None
    if changes:
        lines += ["", "Changes since the previous run of this day:"] + changes
    if s["dq"]["warn"]:
        lines += ["", "Data quality warnings:"] + [f"  {w}" for w in s["dq"]["warn"]]
    lines += ["", "Payments are not made by this system: every pay/recall is approved by a person."]
    tag = {"UPDATE_1500": " 15:00 UPDATE", "MANUAL_RERUN": " re-run"}.get(run_type, "")
    subject = f"MarginWatch {business_date}{tag}: {len(approvals)} approvals, {len(disputes)} disputes, {len(breaks)} breaks"
    return {"subject": subject[:99], "message": "\n".join(lines),
            "html": to_html(business_date, run_type, seconds, b, s, g, changes)}


# ---------- HTML version (Amazon SES) ---------------------------------------------------------------
CSS = ("font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#1f2933")
TH = "text-align:left;padding:6px 10px;background:#eef2f7;border-bottom:1px solid #cbd2d9"
TD = "padding:5px 10px;border-bottom:1px solid #e4e7eb"
NUM = TD + ";text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap"


def table(headers, rows, numeric=()):
    if not rows:
        return "<p style='color:#616e7c'>none</p>"
    e = html.escape
    head = "".join(f"<th style='{TH}{';text-align:right' if i in numeric else ''}'>{e(h)}</th>" for i, h in enumerate(headers))
    body = "".join("<tr>" + "".join(f"<td style='{NUM if i in numeric else TD}'>{e(str(v))}</td>" for i, v in enumerate(r)) + "</tr>"
                   for r in rows)
    return f"<table style='border-collapse:collapse;margin:4px 0 14px'><tr>{head}</tr>{body}</table>"


def to_html(business_date, run_type, seconds, b, s, g, changes):
    m, e = g["fact_margin_call"], html.escape
    sec = lambda k: f"{seconds[k]} s" if seconds.get(k) is not None else "-"
    parts = [f"<div style='{CSS}'>",
             f"<h2 style='margin:0 0 4px'>MarginWatch - business date {e(business_date)}</h2>",
             f"<p style='margin:0 0 14px;color:#616e7c'>{e(RUN_LABEL.get(run_type, run_type))}: <b style='color:#2f8132'>SUCCEEDED</b></p>",
             "<h3>Run</h3>",
             table(["Layer", "Result", "Time"], [
                 ["Bronze", f"{b['files_loaded']} of {b['files_in_landing']} files loaded, {b['files_quarantined']} quarantined", sec("bronze")],
                 ["Silver", f"{s['dq']['checks']} quality checks, {len(s['dq']['fail'])} failed, {len(s['dq']['warn'])} warnings", sec("silver")],
                 ["Gold", f"{m['rows']} agreements / PB accounts, {g['fact_trade_mtm']['rows']} trades reconciled", sec("gold")]],
                 numeric=(2,)),
             "<h3>Margin summary</h3>",
             table(["Summary / action", "Agreements"], [[k, v] for k, v in m["by_summary_action"].items()], numeric=(1,)),
             f"<h3>Client approval required ({len(m['client_approval_required'])})</h3>",
             table(["Counterparty", "Agreement", "Net excess / deficit (USD)", "Summary"],
                   [[a["cpty_short_name"], a["agreement_id"], money(a["net_excess_deficit_usd"]), a["summary"]]
                    for a in m["client_approval_required"]], numeric=(2,)),
             f"<h3>Counterparty calls in dispute ({len(m['calls_not_agreed'])})</h3>",
             table(["Agreement", "Our net (USD)", "Their call", "Their amount (USD)", "Difference (USD)", "Trade breaks"],
                   [[d["agreement_id"], money(d["net_excess_deficit_usd"]), d["cpty_call_direction"],
                     money(d["cpty_call_amount_usd"]), money(d["call_difference_usd"]), d["trade_breaks"]]
                    for d in m["calls_not_agreed"]], numeric=(1, 3, 4, 5)),
             f"<h3>Trade breaks ({len(g['breaks'])})</h3>",
             table(["Agreement", "Trade", "Status", "Ours (USD)", "Theirs (USD)"],
                   [[x["agreement_id"], x["trade_id"], x["recon_status"], money(x["mtm_ours_usd"]), money(x["mtm_theirs_usd"])]
                    for x in g["breaks"]], numeric=(3, 4))]
    if changes:
        parts += ["<h3>Changes since the previous run of this day</h3><ul>"] + \
                 [f"<li>{e(c.strip())}</li>" for c in changes] + ["</ul>"]
    if s["dq"]["warn"]:
        parts += ["<h3>Data quality warnings</h3><ul>"] + [f"<li>{e(w)}</li>" for w in s["dq"]["warn"]] + ["</ul>"]
    parts += ["<p style='color:#616e7c;margin-top:18px'>Payments are not made by this system: "
              "every pay / recall is approved by a person.</p></div>"]
    return "".join(parts)


def handler(event, context):
    return build_report(event["business_date"], event.get("seconds"), event.get("run_type", "CUTOFF_1230"))
