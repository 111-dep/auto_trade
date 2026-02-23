#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from okx_trader.common import load_dotenv
from okx_trader.config import read_config
from okx_trader.okx_client import OKXClient


UTC = timezone.utc


def _to_decimal(v: Any) -> Decimal:
    try:
        return Decimal(str(v or "0"))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _to_int(v: Any) -> int:
    try:
        return int(str(v))
    except Exception:
        return 0


def _fmt_usdt(v: Decimal) -> str:
    return f"{v.quantize(Decimal('0.00000001'))}"


def parse_utc_to_ms(raw: str, is_end: bool) -> int:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("empty datetime")

    if re.fullmatch(r"\d{10,13}", text):
        iv = int(text)
        return iv if len(text) >= 13 else iv * 1000

    date_only = re.fullmatch(r"\d{4}-\d{2}-\d{2}", text)
    if date_only:
        dt = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=UTC)
        if is_end:
            dt += timedelta(days=1)
        return int(dt.timestamp() * 1000)

    suffixes = (" UTC", "Z", " +00:00")
    for suf in suffixes:
        if text.endswith(suf):
            text = text[: -len(suf)]
            break

    fmts = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    )
    for fmt in fmts:
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=UTC)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"unsupported datetime format: {raw}")


def fetch_bills(
    client: OKXClient,
    *,
    inst_type: str,
    start_ms: int,
    end_ms: int,
    inst_ids: Optional[Sequence[str]],
    endpoint: str,
    limit: int,
    max_pages: int,
) -> List[Dict[str, Any]]:
    after: Optional[str] = None
    out: List[Dict[str, Any]] = []
    seen_bill_ids: set[str] = set()
    want_inst = {x.upper() for x in (inst_ids or [])}
    page = 0

    while page < max_pages:
        page += 1
        params: Dict[str, str] = {"instType": inst_type, "limit": str(limit)}
        if after:
            params["after"] = after
        data = client._request("GET", endpoint, params=params, private=True)
        rows = data.get("data", []) or []
        if not rows:
            break

        new_count = 0
        stop = False
        for row in rows:
            bill_id = str(row.get("billId", "")).strip()
            if bill_id:
                if bill_id in seen_bill_ids:
                    continue
                seen_bill_ids.add(bill_id)
                new_count += 1
            ts = _to_int(row.get("ts"))
            if ts and ts < start_ms:
                stop = True
            if not (start_ms <= ts < end_ms):
                continue
            if want_inst:
                inst = str(row.get("instId", "")).upper()
                if inst and inst not in want_inst:
                    continue
            out.append(row)

        # bills API: use `after=<oldest_bill_id_in_current_page>` to paginate older records.
        last_id = str(rows[-1].get("billId", "")).strip()
        if not last_id:
            break
        after = last_id
        if new_count == 0:
            break
        if stop:
            break

    return out


def summarize_bills(
    rows: Sequence[Dict[str, Any]],
    *,
    trade_clord_prefix: str,
    trade_filter_mode: str,
    allowed_ord_ids: Optional[set[str]],
    allowed_clord_ids: Optional[set[str]],
    inst_ids_scope: Optional[Sequence[str]],
    funding_scope: str,
) -> Dict[str, Any]:
    # Raw account-level totals (for sanity check)
    raw_total_bal = Decimal("0")
    raw_total_pnl = Decimal("0")
    raw_total_fee = Decimal("0")
    raw_total_interest = Decimal("0")
    raw_total_earn = Decimal("0")

    by_type_sub: Counter[Tuple[str, str]] = Counter()

    prefix = str(trade_clord_prefix or "").strip()
    filter_mode = str(trade_filter_mode or "prefix").strip().lower()
    scope_insts = {x.upper() for x in (inst_ids_scope or [])}
    ord_id_set = set(str(x).strip() for x in (allowed_ord_ids or set()) if str(x).strip())
    cl_ord_id_set = set(str(x).strip() for x in (allowed_clord_ids or set()) if str(x).strip())

    matched_trade_insts: set[str] = set()
    selected_trade_rows: List[Dict[str, Any]] = []
    selected_funding_rows: List[Dict[str, Any]] = []

    for r in rows:
        inst = str(r.get("instId", "") or "-").upper()
        typ = str(r.get("type", "") or "")
        sub = str(r.get("subType", "") or "")
        ord_id = str(r.get("ordId", "") or "").strip()
        cl_ord_id = str(r.get("clOrdId", "") or "").strip()

        bal = _to_decimal(r.get("balChg"))
        pnl = _to_decimal(r.get("pnl"))
        fee = _to_decimal(r.get("fee"))
        interest = _to_decimal(r.get("interest"))
        earn_amt = _to_decimal(r.get("earnAmt"))

        raw_total_bal += bal
        raw_total_pnl += pnl
        raw_total_fee += fee
        raw_total_interest += interest
        raw_total_earn += earn_amt

        by_type_sub[(typ, sub)] += 1

        if typ == "2":
            match_prefix = (not prefix) or cl_ord_id.startswith(prefix)
            match_links = bool(ord_id and ord_id in ord_id_set) or bool(cl_ord_id and cl_ord_id in cl_ord_id_set)
            if filter_mode == "none":
                keep_trade = True
            elif filter_mode == "order-link":
                keep_trade = match_links
            elif filter_mode == "merge":
                keep_trade = match_prefix or match_links
            else:  # prefix
                keep_trade = match_prefix
            if not keep_trade:
                continue
            selected_trade_rows.append(r)
            if inst and inst != "-":
                matched_trade_insts.add(inst)

    for r in rows:
        typ = str(r.get("type", "") or "")
        if typ != "8":
            continue
        inst = str(r.get("instId", "") or "-").upper()
        keep = False
        if funding_scope == "all":
            keep = True
        elif funding_scope == "inst-ids":
            keep = (not scope_insts) or (inst in scope_insts)
        else:  # matched-trade-inst
            keep = inst in matched_trade_insts
        if keep:
            selected_funding_rows.append(r)

    trade_pnl = Decimal("0")
    trade_fee = Decimal("0")
    trade_bal = Decimal("0")
    funding_bal = Decimal("0")
    funding_fee = Decimal("0")

    by_inst: Dict[str, Dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
    trade_subtype_net: Dict[str, Dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))

    for r in selected_trade_rows:
        inst = str(r.get("instId", "") or "-").upper()
        sub = str(r.get("subType", "") or "")
        pnl = _to_decimal(r.get("pnl"))
        fee = _to_decimal(r.get("fee"))
        bal = _to_decimal(r.get("balChg"))
        trade_pnl += pnl
        trade_fee += fee
        trade_bal += bal
        b = by_inst[inst]
        b["trade_rows"] += Decimal("1")
        b["trade_pnl"] += pnl
        b["trade_fee"] += fee
        b["trade_net"] += (pnl + fee)
        b["trade_bal"] += bal

        ts = trade_subtype_net[sub]
        ts["rows"] += Decimal("1")
        ts["pnl"] += pnl
        ts["fee"] += fee
        ts["net"] += (pnl + fee)

    for r in selected_funding_rows:
        inst = str(r.get("instId", "") or "-").upper()
        bal = _to_decimal(r.get("balChg"))
        fee = _to_decimal(r.get("fee"))
        funding_bal += bal
        funding_fee += fee
        b = by_inst[inst]
        b["funding_rows"] += Decimal("1")
        b["funding_bal"] += bal

    trade_net = trade_pnl + trade_fee
    recommended_net = trade_net + funding_bal

    return {
        "raw_rows": len(rows),
        "raw_total_bal": raw_total_bal,
        "raw_total_pnl": raw_total_pnl,
        "raw_total_fee": raw_total_fee,
        "raw_total_interest": raw_total_interest,
        "raw_total_earn": raw_total_earn,
        "selected_trade_rows": len(selected_trade_rows),
        "selected_funding_rows": len(selected_funding_rows),
        "trade_pnl": trade_pnl,
        "trade_fee": trade_fee,
        "trade_net": trade_net,
        "trade_bal": trade_bal,
        "funding_bal": funding_bal,
        "funding_fee": funding_fee,
        "recommended_net": recommended_net,
        "by_type_sub": by_type_sub,
        "by_inst": by_inst,
        "matched_trade_insts": matched_trade_insts,
        "trade_subtype_net": trade_subtype_net,
        "selected_trade_rows_data": selected_trade_rows,
    }


def load_trade_order_link_index(
    path: str,
    *,
    start_ms: int,
    end_ms: int,
    inst_ids: Optional[Sequence[str]],
) -> Dict[str, Any]:
    want_inst = {x.upper() for x in (inst_ids or [])}
    ord_ids: set[str] = set()
    cl_ord_ids: set[str] = set()
    ord_to_trade: Dict[str, set[str]] = defaultdict(set)
    cl_ord_to_trade: Dict[str, set[str]] = defaultdict(set)
    trade_to_inst: Dict[str, set[str]] = defaultdict(set)
    trade_ids: set[str] = set()
    n = 0
    if not path or (not os.path.exists(path)):
        return {
            "rows": 0,
            "ord_ids": ord_ids,
            "cl_ord_ids": cl_ord_ids,
            "ord_to_trade": ord_to_trade,
            "cl_ord_to_trade": cl_ord_to_trade,
            "trade_to_inst": trade_to_inst,
            "trade_ids": trade_ids,
        }

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            inst = str(row.get("inst_id", "")).strip().upper()
            if want_inst and inst and inst not in want_inst:
                continue
            ts_raw = str(row.get("event_ts_ms", "")).strip()
            ts = _to_int(ts_raw)
            if not (start_ms <= ts < end_ms):
                continue
            n += 1
            trade_id = str(row.get("trade_id", "") or "").strip()
            if trade_id:
                trade_ids.add(trade_id)
                if inst:
                    trade_to_inst[trade_id].add(inst)
            for key in ("entry_ord_id", "event_ord_id"):
                oid = str(row.get(key, "") or "").strip()
                if oid:
                    ord_ids.add(oid)
                    if trade_id:
                        ord_to_trade[oid].add(trade_id)
            for key in ("entry_cl_ord_id", "event_cl_ord_id"):
                cid = str(row.get(key, "") or "").strip()
                if cid:
                    cl_ord_ids.add(cid)
                    if trade_id:
                        cl_ord_to_trade[cid].add(trade_id)
    return {
        "rows": n,
        "ord_ids": ord_ids,
        "cl_ord_ids": cl_ord_ids,
        "ord_to_trade": ord_to_trade,
        "cl_ord_to_trade": cl_ord_to_trade,
        "trade_to_inst": trade_to_inst,
        "trade_ids": trade_ids,
    }


def load_trade_journal_close_pnl(
    path: str,
    *,
    start_ms: int,
    end_ms: int,
    inst_ids: Optional[Sequence[str]],
) -> Tuple[int, Decimal]:
    want_inst = {x.upper() for x in (inst_ids or [])}
    close_types = {"CLOSE", "EXTERNAL_CLOSE", "PARTIAL_CLOSE"}
    n = 0
    pnl_sum = Decimal("0")

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if str(row.get("event_type", "")).strip().upper() not in close_types:
                continue
            inst = str(row.get("inst_id", "")).upper()
            if want_inst and inst and inst not in want_inst:
                continue
            ts_raw = str(row.get("event_ts_ms", "")).strip()
            ts = _to_int(ts_raw)
            if not (start_ms <= ts < end_ms):
                continue
            n += 1
            pnl_sum += _to_decimal(row.get("pnl_usdt"))
    return n, pnl_sum


def load_trade_journal_close_by_trade_id(
    path: str,
    *,
    start_ms: int,
    end_ms: int,
    inst_ids: Optional[Sequence[str]],
) -> Dict[str, Dict[str, Any]]:
    want_inst = {x.upper() for x in (inst_ids or [])}
    close_types = {"CLOSE", "EXTERNAL_CLOSE", "PARTIAL_CLOSE"}
    out: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"rows": 0, "pnl": Decimal("0"), "inst_ids": set()})
    if not path or (not os.path.exists(path)):
        return out

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if str(row.get("event_type", "")).strip().upper() not in close_types:
                continue
            inst = str(row.get("inst_id", "")).strip().upper()
            if want_inst and inst and inst not in want_inst:
                continue
            ts_raw = str(row.get("event_ts_ms", "")).strip()
            ts = _to_int(ts_raw)
            if not (start_ms <= ts < end_ms):
                continue
            trade_id = str(row.get("trade_id", "") or "").strip()
            if not trade_id:
                continue
            rec = out[trade_id]
            rec["rows"] = int(rec["rows"]) + 1
            rec["pnl"] = _to_decimal(rec.get("pnl")) + _to_decimal(row.get("pnl_usdt"))
            if inst:
                rec["inst_ids"].add(inst)
    return out


def _resolve_row_trade_id(
    row: Dict[str, Any],
    *,
    ord_to_trade: Dict[str, set[str]],
    cl_ord_to_trade: Dict[str, set[str]],
) -> Tuple[str, str]:
    ord_id = str(row.get("ordId", "") or "").strip()
    cl_ord_id = str(row.get("clOrdId", "") or "").strip()
    ord_hits = set(ord_to_trade.get(ord_id, set())) if ord_id else set()
    cl_hits = set(cl_ord_to_trade.get(cl_ord_id, set())) if cl_ord_id else set()

    if len(ord_hits) == 1:
        return next(iter(ord_hits)), "ord"
    if len(cl_hits) == 1:
        return next(iter(cl_hits)), "cl_ord"
    merged = ord_hits | cl_hits
    if len(merged) == 1:
        return next(iter(merged)), "union"
    if merged:
        return "", "ambiguous"
    return "", "unmapped"


def summarize_selected_trade_rows_by_trade_id(
    selected_trade_rows: Sequence[Dict[str, Any]],
    *,
    link_index: Dict[str, Any],
    journal_by_trade_id: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    ord_to_trade = link_index.get("ord_to_trade", {}) or {}
    cl_ord_to_trade = link_index.get("cl_ord_to_trade", {}) or {}
    trade_to_inst = link_index.get("trade_to_inst", {}) or {}

    per_trade: Dict[str, Dict[str, Any]] = {}
    mapped_rows = 0
    ambiguous_rows = 0
    unmapped_rows = 0
    mapped_net = Decimal("0")
    ambiguous_net = Decimal("0")
    unmapped_net = Decimal("0")
    match_counts: Counter[str] = Counter()

    for row in selected_trade_rows:
        trade_id, match_by = _resolve_row_trade_id(
            row,
            ord_to_trade=ord_to_trade,
            cl_ord_to_trade=cl_ord_to_trade,
        )
        pnl = _to_decimal(row.get("pnl"))
        fee = _to_decimal(row.get("fee"))
        net = pnl + fee

        if not trade_id:
            if match_by == "ambiguous":
                ambiguous_rows += 1
                ambiguous_net += net
            else:
                unmapped_rows += 1
                unmapped_net += net
            continue

        mapped_rows += 1
        mapped_net += net
        match_counts[match_by] += 1

        rec = per_trade.get(trade_id)
        if rec is None:
            rec = {
                "trade_id": trade_id,
                "bill_rows": 0,
                "bill_pnl": Decimal("0"),
                "bill_fee": Decimal("0"),
                "bill_net": Decimal("0"),
                "inst_ids": set(),
                "match_ord_rows": 0,
                "match_cl_ord_rows": 0,
                "match_union_rows": 0,
                "journal_rows": 0,
                "journal_pnl": Decimal("0"),
                "delta_bill_minus_journal": Decimal("0"),
            }
            per_trade[trade_id] = rec
        rec["bill_rows"] = int(rec["bill_rows"]) + 1
        rec["bill_pnl"] = _to_decimal(rec.get("bill_pnl")) + pnl
        rec["bill_fee"] = _to_decimal(rec.get("bill_fee")) + fee
        rec["bill_net"] = _to_decimal(rec.get("bill_net")) + net
        if match_by == "ord":
            rec["match_ord_rows"] = int(rec["match_ord_rows"]) + 1
        elif match_by == "cl_ord":
            rec["match_cl_ord_rows"] = int(rec["match_cl_ord_rows"]) + 1
        else:
            rec["match_union_rows"] = int(rec["match_union_rows"]) + 1
        inst = str(row.get("instId", "") or "").strip().upper()
        if inst:
            rec["inst_ids"].add(inst)

    for trade_id, rec in per_trade.items():
        j = journal_by_trade_id.get(trade_id, {})
        rec["journal_rows"] = int(j.get("rows", 0) or 0)
        rec["journal_pnl"] = _to_decimal(j.get("pnl"))
        rec["delta_bill_minus_journal"] = _to_decimal(rec.get("bill_net")) - _to_decimal(rec.get("journal_pnl"))
        if not rec["inst_ids"]:
            rec["inst_ids"] = set(trade_to_inst.get(trade_id, set()))

    journal_only_trade_ids = set(journal_by_trade_id.keys()) - set(per_trade.keys())
    journal_only_rows = 0
    journal_only_pnl = Decimal("0")
    for tid in journal_only_trade_ids:
        one = journal_by_trade_id.get(tid, {})
        journal_only_rows += int(one.get("rows", 0) or 0)
        journal_only_pnl += _to_decimal(one.get("pnl"))

    return {
        "per_trade": per_trade,
        "mapped_rows": mapped_rows,
        "ambiguous_rows": ambiguous_rows,
        "unmapped_rows": unmapped_rows,
        "mapped_net": mapped_net,
        "ambiguous_net": ambiguous_net,
        "unmapped_net": unmapped_net,
        "match_counts": match_counts,
        "journal_only_trade_ids": journal_only_trade_ids,
        "journal_only_rows": journal_only_rows,
        "journal_only_pnl": journal_only_pnl,
    }


def dump_trade_id_report_csv(path: str, report: Dict[str, Any]) -> None:
    if not path:
        return
    per_trade = report.get("per_trade", {}) or {}
    rows = []
    for trade_id, rec in per_trade.items():
        inst_ids = sorted(set(rec.get("inst_ids", set()) or set()))
        rows.append(
            {
                "trade_id": trade_id,
                "inst_ids": ",".join(inst_ids),
                "bill_rows": int(rec.get("bill_rows", 0) or 0),
                "bill_pnl": _fmt_usdt(_to_decimal(rec.get("bill_pnl"))),
                "bill_fee": _fmt_usdt(_to_decimal(rec.get("bill_fee"))),
                "bill_net": _fmt_usdt(_to_decimal(rec.get("bill_net"))),
                "journal_rows": int(rec.get("journal_rows", 0) or 0),
                "journal_pnl": _fmt_usdt(_to_decimal(rec.get("journal_pnl"))),
                "delta_bill_minus_journal": _fmt_usdt(_to_decimal(rec.get("delta_bill_minus_journal"))),
                "match_ord_rows": int(rec.get("match_ord_rows", 0) or 0),
                "match_cl_ord_rows": int(rec.get("match_cl_ord_rows", 0) or 0),
                "match_union_rows": int(rec.get("match_union_rows", 0) or 0),
            }
        )

    rows.sort(key=lambda x: Decimal(str(x["bill_net"])), reverse=True)
    fieldnames = [
        "trade_id",
        "inst_ids",
        "bill_rows",
        "bill_pnl",
        "bill_fee",
        "bill_net",
        "journal_rows",
        "journal_pnl",
        "delta_bill_minus_journal",
        "match_ord_rows",
        "match_cl_ord_rows",
        "match_union_rows",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _ms_to_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reconcile net PnL from OKX bills (includes fee/funding) and compare with trade_journal."
    )
    parser.add_argument("--env", required=True, help="Path to env file (okx_auto_trader.env)")
    parser.add_argument("--start", required=True, help="UTC start (e.g. 2026-02-22 or 2026-02-22 00:00:00)")
    parser.add_argument("--end", required=True, help="UTC end (exclusive)")
    parser.add_argument("--inst-type", default="SWAP", help="OKX instType filter (default: SWAP)")
    parser.add_argument("--inst-ids", default="", help="Optional comma-separated inst ids")
    parser.add_argument("--endpoint", default="/api/v5/account/bills", help="Bills endpoint")
    parser.add_argument("--limit", type=int, default=100, help="Page size (max 100)")
    parser.add_argument("--max-pages", type=int, default=200, help="Max pages to fetch")
    parser.add_argument("--journal-path", default="/home/dandan/Workspace/test/okx_trade_suite/trade_journal.csv")
    parser.add_argument(
        "--order-link-path",
        default="/home/dandan/Workspace/test/okx_trade_suite/trade_journal_order_links.csv",
        help="trade order link csv path (used by --trade-filter-mode=order-link/merge).",
    )
    parser.add_argument(
        "--trade-clord-prefix",
        default="",
        help="Only include type=2 trade rows with this clOrdId prefix (e.g. ATS).",
    )
    parser.add_argument(
        "--trade-filter-mode",
        default="prefix",
        choices=("prefix", "order-link", "merge", "none"),
        help="trade row filter: prefix / order-link / merge / none",
    )
    parser.add_argument(
        "--show-trade-ids",
        type=int,
        default=20,
        help="Show top/bottom N rows in trade-id net report (0 to disable).",
    )
    parser.add_argument(
        "--dump-trade-id-csv",
        default="",
        help="Optional csv output path for trade-id level bill-vs-journal report.",
    )
    parser.add_argument(
        "--funding-scope",
        default="matched-trade-inst",
        choices=("matched-trade-inst", "inst-ids", "all"),
        help="How to include funding(type=8): matched trade inst / inst-ids / all.",
    )
    parser.add_argument("--dump-csv", default="", help="Optional output csv for fetched bills")
    args = parser.parse_args()

    start_ms = parse_utc_to_ms(args.start, is_end=False)
    end_ms = parse_utc_to_ms(args.end, is_end=True if re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.end.strip()) else False)
    if end_ms <= start_ms:
        raise SystemExit("--end must be greater than --start")

    inst_ids = [x.strip().upper() for x in args.inst_ids.split(",") if x.strip()]
    link_index = load_trade_order_link_index(
        args.order_link_path,
        start_ms=start_ms,
        end_ms=end_ms,
        inst_ids=inst_ids or None,
    )
    link_row_count = int(link_index.get("rows", 0) or 0)
    link_ord_ids: set[str] = set(link_index.get("ord_ids", set()) or set())
    link_cl_ord_ids: set[str] = set(link_index.get("cl_ord_ids", set()) or set())

    load_dotenv(args.env)
    cfg = read_config(None)
    client = OKXClient(cfg)

    bills = fetch_bills(
        client,
        inst_type=args.inst_type,
        start_ms=start_ms,
        end_ms=end_ms,
        inst_ids=inst_ids or None,
        endpoint=args.endpoint,
        limit=max(1, min(100, int(args.limit))),
        max_pages=max(1, int(args.max_pages)),
    )

    if args.dump_csv:
        if bills:
            keys = sorted({k for row in bills for k in row.keys()})
        else:
            keys = ["ts", "billId", "instId", "type", "subType", "ordId", "balChg", "pnl", "fee", "ccy"]
        with open(args.dump_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            for row in bills:
                writer.writerow({k: row.get(k, "") for k in keys})

    summary = summarize_bills(
        bills,
        trade_clord_prefix=args.trade_clord_prefix,
        trade_filter_mode=args.trade_filter_mode,
        allowed_ord_ids=link_ord_ids,
        allowed_clord_ids=link_cl_ord_ids,
        inst_ids_scope=inst_ids or None,
        funding_scope=args.funding_scope,
    )
    j_count, j_pnl = load_trade_journal_close_pnl(
        args.journal_path,
        start_ms=start_ms,
        end_ms=end_ms,
        inst_ids=inst_ids or None,
    )
    journal_by_trade_id = load_trade_journal_close_by_trade_id(
        args.journal_path,
        start_ms=start_ms,
        end_ms=end_ms,
        inst_ids=inst_ids or None,
    )
    trade_id_report = summarize_selected_trade_rows_by_trade_id(
        summary.get("selected_trade_rows_data", []) or [],
        link_index=link_index,
        journal_by_trade_id=journal_by_trade_id,
    )

    print("=== OKX Bills Reconcile ===")
    print(f"range: {_ms_to_utc(start_ms)} -> {_ms_to_utc(end_ms)}")
    print(f"inst_type={args.inst_type} inst_ids={','.join(inst_ids) if inst_ids else '-'} endpoint={args.endpoint}")
    print(f"fetched_rows={summary['raw_rows']}")
    print(
        f"trade_filter: mode={args.trade_filter_mode} clOrdId_prefix={args.trade_clord_prefix or '-'} "
        f"order_link_rows={link_row_count} order_link_ordIds={len(link_ord_ids)} order_link_clOrdIds={len(link_cl_ord_ids)} "
        f"| selected_trade_rows={summary['selected_trade_rows']}"
    )
    print(
        f"funding_filter: {args.funding_scope} "
        f"| selected_funding_rows={summary['selected_funding_rows']} "
        f"| matched_trade_insts={','.join(sorted(summary['matched_trade_insts'])) or '-'}"
    )
    print(f"trade_gross_pnl(sum pnl)={_fmt_usdt(summary['trade_pnl'])} USDT")
    print(f"trade_fee(sum fee)={_fmt_usdt(summary['trade_fee'])} USDT")
    print(f"trade_net(pnl+fee)={_fmt_usdt(summary['trade_net'])} USDT")
    print(f"funding_net(type=8 balChg)={_fmt_usdt(summary['funding_bal'])} USDT")
    print(f"recommended_net(trade_net+funding)={_fmt_usdt(summary['recommended_net'])} USDT")
    print(f"journal_close_rows={j_count} journal_pnl_sum={_fmt_usdt(j_pnl)} USDT")
    print(f"delta(recommended_net - journal_pnl)={_fmt_usdt(summary['recommended_net'] - j_pnl)} USDT")
    if "6" in summary["trade_subtype_net"]:
        close_sub = summary["trade_subtype_net"]["6"]
        close_net = close_sub["net"]
        print(
            "close_subtype(6) net(pnl+fee)={} USDT | delta(vs journal)={} USDT".format(
                _fmt_usdt(close_net),
                _fmt_usdt(close_net - j_pnl),
            )
        )
    print("")
    print("[raw account reference]")
    print(f"raw_balChg_all={_fmt_usdt(summary['raw_total_bal'])} USDT")
    print(f"raw_pnl_all={_fmt_usdt(summary['raw_total_pnl'])} raw_fee_all={_fmt_usdt(summary['raw_total_fee'])}")
    print(f"raw_interest={_fmt_usdt(summary['raw_total_interest'])} raw_earnAmt={_fmt_usdt(summary['raw_total_earn'])}")
    print("")

    print("by_type_subType (top 12):")
    for (typ, sub), cnt in summary["by_type_sub"].most_common(12):
        print(f"  type={typ} subType={sub} count={cnt}")
    print("")

    print("by_inst:")
    inst_rows = []
    for inst, s in summary["by_inst"].items():
        inst_rows.append(
            (
                inst,
                s["trade_net"] + s["funding_bal"],
                int(s["trade_rows"] + s["funding_rows"]),
                s["trade_pnl"],
                s["trade_fee"],
                s["trade_net"],
                s["funding_bal"],
            )
        )
    inst_rows.sort(key=lambda x: x[1], reverse=True)
    for inst, net, cnt, trade_pnl, trade_fee, trade_net, funding_bal in inst_rows:
        print(
            f"  {inst}: rows={cnt} net={_fmt_usdt(net)} trade_pnl={_fmt_usdt(trade_pnl)} "
            f"trade_fee={_fmt_usdt(trade_fee)} trade_net={_fmt_usdt(trade_net)} funding={_fmt_usdt(funding_bal)}"
        )

    print("")
    print("trade_subtype_breakdown(type=2):")
    for sub, vals in sorted(summary["trade_subtype_net"].items(), key=lambda x: int(x[0] or "0")):
        print(
            f"  subType={sub}: rows={int(vals['rows'])} pnl={_fmt_usdt(vals['pnl'])} "
            f"fee={_fmt_usdt(vals['fee'])} net={_fmt_usdt(vals['net'])}"
        )

    print("")
    print("by_trade_id:")
    per_trade = trade_id_report.get("per_trade", {}) or {}
    mapped_rows = int(trade_id_report.get("mapped_rows", 0) or 0)
    ambiguous_rows = int(trade_id_report.get("ambiguous_rows", 0) or 0)
    unmapped_rows = int(trade_id_report.get("unmapped_rows", 0) or 0)
    mapped_net = _to_decimal(trade_id_report.get("mapped_net"))
    ambiguous_net = _to_decimal(trade_id_report.get("ambiguous_net"))
    unmapped_net = _to_decimal(trade_id_report.get("unmapped_net"))
    journal_only_ids = trade_id_report.get("journal_only_trade_ids", set()) or set()
    match_counts: Counter[str] = trade_id_report.get("match_counts", Counter())  # type: ignore[assignment]
    print(
        "  link_rows={} link_trade_ids={} mapped_rows={} ambiguous_rows={} unmapped_rows={} mapped_trade_ids={}".format(
            link_row_count,
            len(link_index.get("trade_ids", set()) or set()),
            mapped_rows,
            ambiguous_rows,
            unmapped_rows,
            len(per_trade),
        )
    )
    print(
        "  mapped_net={} ambiguous_net={} unmapped_net={} match(ord/cl/union)={}/{}/{}".format(
            _fmt_usdt(mapped_net),
            _fmt_usdt(ambiguous_net),
            _fmt_usdt(unmapped_net),
            int(match_counts.get("ord", 0)),
            int(match_counts.get("cl_ord", 0)),
            int(match_counts.get("union", 0)),
        )
    )
    print(
        "  journal_only_trade_ids={} journal_only_rows={} journal_only_pnl={}".format(
            len(journal_only_ids),
            int(trade_id_report.get("journal_only_rows", 0) or 0),
            _fmt_usdt(_to_decimal(trade_id_report.get("journal_only_pnl"))),
        )
    )

    if int(args.show_trade_ids) > 0 and per_trade:
        n = max(1, int(args.show_trade_ids))
        ranked = list(per_trade.values())
        ranked.sort(key=lambda x: _to_decimal(x.get("bill_net")), reverse=True)
        print(f"  top_{n}_by_bill_net:")
        for rec in ranked[:n]:
            print(
                "    {} | inst={} bill_rows={} bill_net={} journal_pnl={} delta={}".format(
                    rec.get("trade_id", ""),
                    ",".join(sorted(set(rec.get("inst_ids", set()) or set()))) or "-",
                    int(rec.get("bill_rows", 0) or 0),
                    _fmt_usdt(_to_decimal(rec.get("bill_net"))),
                    _fmt_usdt(_to_decimal(rec.get("journal_pnl"))),
                    _fmt_usdt(_to_decimal(rec.get("delta_bill_minus_journal"))),
                )
            )
        print(f"  bottom_{n}_by_bill_net:")
        for rec in list(reversed(ranked[-n:])):
            print(
                "    {} | inst={} bill_rows={} bill_net={} journal_pnl={} delta={}".format(
                    rec.get("trade_id", ""),
                    ",".join(sorted(set(rec.get("inst_ids", set()) or set()))) or "-",
                    int(rec.get("bill_rows", 0) or 0),
                    _fmt_usdt(_to_decimal(rec.get("bill_net"))),
                    _fmt_usdt(_to_decimal(rec.get("journal_pnl"))),
                    _fmt_usdt(_to_decimal(rec.get("delta_bill_minus_journal"))),
                )
            )
    elif int(args.show_trade_ids) > 0:
        print("  no mapped trade_id rows under current filter/window.")

    if args.dump_csv:
        print("")
        print(f"dump_csv={args.dump_csv}")
    if args.dump_trade_id_csv:
        dump_trade_id_report_csv(args.dump_trade_id_csv, trade_id_report)
        print(f"dump_trade_id_csv={args.dump_trade_id_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
