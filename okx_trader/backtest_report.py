from __future__ import annotations

from typing import Any, Dict, List

from .common import format_duration


def new_level_perf() -> Dict[int, Dict[str, float]]:
    return {
        1: {"signals": 0.0, "sum_r": 0.0, "tp1": 0.0, "tp2": 0.0, "stop": 0.0, "none": 0.0},
        2: {"signals": 0.0, "sum_r": 0.0, "tp1": 0.0, "tp2": 0.0, "stop": 0.0, "none": 0.0},
        3: {"signals": 0.0, "sum_r": 0.0, "tp1": 0.0, "tp2": 0.0, "stop": 0.0, "none": 0.0},
    }


def update_level_perf(level_perf: Dict[int, Dict[str, float]], level: int, outcome: str, r_value: float) -> None:
    if level not in {1, 2, 3}:
        return
    bucket = level_perf[level]
    bucket["signals"] += 1.0
    bucket["sum_r"] += float(r_value)
    if outcome == "TP2":
        bucket["tp2"] += 1.0
        bucket["tp1"] += 1.0
    elif outcome == "TP1":
        bucket["tp1"] += 1.0
    elif outcome == "STOP":
        bucket["stop"] += 1.0
    else:
        bucket["none"] += 1.0


def finalize_level_perf(level_perf: Dict[int, Dict[str, float]]) -> Dict[int, Dict[str, float]]:
    out: Dict[int, Dict[str, float]] = {}
    for lv in (1, 2, 3):
        b = level_perf.get(lv, {})
        signals = int(b.get("signals", 0.0))
        sum_r = float(b.get("sum_r", 0.0))
        out[lv] = {
            "signals": signals,
            "tp1": int(b.get("tp1", 0.0)),
            "tp2": int(b.get("tp2", 0.0)),
            "stop": int(b.get("stop", 0.0)),
            "none": int(b.get("none", 0.0)),
            "avg_r": (sum_r / signals) if signals > 0 else 0.0,
        }
    return out


def level_perf_brief(level_perf_final: Dict[int, Dict[str, float]]) -> str:
    parts: List[str] = []
    for lv in (1, 2, 3):
        b = level_perf_final.get(lv, {})
        sig = int(b.get("signals", 0))
        avg_r = float(b.get("avg_r", 0.0))
        parts.append(f"L{lv}:{sig}/{avg_r:.3f}")
    return " ".join(parts)


def rate_str(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0%"
    return f"{(numerator / denominator * 100.0):.1f}%"


def normalize_backtest_result(res: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(res) if isinstance(res, dict) else {}
    by_level_raw = out.get("by_level", {})
    by_side_raw = out.get("by_side", {})
    lp_raw = out.get("level_perf", {})

    by_level = {
        1: int((by_level_raw or {}).get(1, 0)),
        2: int((by_level_raw or {}).get(2, 0)),
        3: int((by_level_raw or {}).get(3, 0)),
    }
    by_side = {
        "LONG": int((by_side_raw or {}).get("LONG", 0)),
        "SHORT": int((by_side_raw or {}).get("SHORT", 0)),
    }
    level_perf: Dict[int, Dict[str, float]] = {}
    for lv in (1, 2, 3):
        b = lp_raw.get(lv, {}) if isinstance(lp_raw, dict) else {}
        level_perf[lv] = {
            "signals": int((b or {}).get("signals", 0)),
            "tp1": int((b or {}).get("tp1", 0)),
            "tp2": int((b or {}).get("tp2", 0)),
            "stop": int((b or {}).get("stop", 0)),
            "none": int((b or {}).get("none", 0)),
            "avg_r": float((b or {}).get("avg_r", 0.0)),
        }

    out["max_level"] = int(out.get("max_level", 0) or 0)
    out["min_level"] = int(out.get("min_level", 1) or 1)
    out["exact_level"] = int(out.get("exact_level", 0) or 0)
    out["signals"] = int(out.get("signals", 0) or 0)
    out["tp1"] = int(out.get("tp1", 0) or 0)
    out["tp2"] = int(out.get("tp2", 0) or 0)
    out["stop"] = int(out.get("stop", 0) or 0)
    out["none"] = int(out.get("none", 0) or 0)
    out["skip_gap"] = int(out.get("skip_gap", 0) or 0)
    out["skip_daycap"] = int(out.get("skip_daycap", 0) or 0)
    out["skip_unresolved"] = int(out.get("skip_unresolved", 0) or 0)
    out["avg_r"] = float(out.get("avg_r", 0.0) or 0.0)
    out["elapsed_s"] = float(out.get("elapsed_s", 0.0) or 0.0)
    out["by_level"] = by_level
    out["by_side"] = by_side
    out["level_perf"] = level_perf
    if not isinstance(out.get("per_inst"), list):
        out["per_inst"] = []
    return out


def format_backtest_result_line(res: Dict[str, Any]) -> str:
    n = normalize_backtest_result(res)

    signals = n["signals"]
    tp1 = n["tp1"]
    tp2 = n["tp2"]
    stop = n["stop"]
    none = n["none"]
    skip_gap = n["skip_gap"]
    skip_daycap = n["skip_daycap"]
    skip_unresolved = n["skip_unresolved"]
    avg_r = n["avg_r"]
    by_level = n["by_level"]
    by_side = n["by_side"]
    level_perf = n["level_perf"]
    elapsed = n["elapsed_s"]
    max_level = n["max_level"]
    min_level = n["min_level"]
    exact_level = n["exact_level"]

    mode = f"max={max_level}"
    if exact_level in {1, 2, 3}:
        mode = f"exact={exact_level}"
    elif min_level > 1:
        mode = f"range={min_level}-{max_level}"

    lp1 = level_perf.get(1, {})
    lp2 = level_perf.get(2, {})
    lp3 = level_perf.get(3, {})
    lv_txt = (
        f"L1={int(lp1.get('signals',0))}/{float(lp1.get('avg_r',0.0)):.3f} "
        f"L2={int(lp2.get('signals',0))}/{float(lp2.get('avg_r',0.0)):.3f} "
        f"L3={int(lp3.get('signals',0))}/{float(lp3.get('avg_r',0.0)):.3f}"
    )
    return (
        f"{mode} | signals={signals} L1/L2/L3={by_level[1]}/{by_level[2]}/{by_level[3]} "
        f"long/short={by_side['LONG']}/{by_side['SHORT']} "
        f"tp1={tp1}({rate_str(tp1, signals)}) tp2={tp2}({rate_str(tp2, signals)}) "
        f"stop={stop}({rate_str(stop, signals)}) none={none} avgR={avg_r:.3f} {lv_txt} "
        f"skipGap={skip_gap} skipDayCap={skip_daycap} skipUnresolved={skip_unresolved} "
        f"elapsed={format_duration(elapsed)}"
    )


def format_backtest_inst_line(row: Dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return ""
    inst_id = str(row.get("inst_id", ""))
    status = str(row.get("status", "ok"))
    if status != "ok":
        err = str(row.get("error", "unknown"))[:120]
        return f"- {inst_id} 数据异常: {err}"

    sig_n = int(row.get("signals", 0) or 0)
    avg_r = float(row.get("avg_r", 0.0) or 0.0)
    stop_n = int(row.get("stop", 0) or 0)
    tp2_n = int(row.get("tp2", 0) or 0)
    return (
        f"- {inst_id}: signals={sig_n} tp2={tp2_n}({rate_str(tp2_n, sig_n)}) "
        f"stop={stop_n}({rate_str(stop_n, sig_n)}) avgR={avg_r:.3f}"
    )
