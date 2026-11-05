"""
전종목 스크리너 — "거래량은 늘었는데 주가는 아직 안 움직인 종목" 찾기.

server.py 가 import 해서 MCP 도구로 노출합니다.

조건 (기본값, 모두 파라미터로 조정 가능)
  1. 거래량: 20일 평균(최근 5일 제외) 대비 vol_mult(2.0)배 이상인 날이
     최근 5거래일 중 min_days(3)일 이상
  2. 20일 등락률: ret_lo(-3%) ~ ret_hi(+5%)
  3. 60일 고점(종가) 대비 현재가: dd_max(-10%) 이내
  4. 수급: 외국인 또는 기관의 20일 누적 순매수 > 0 (require_flow=True 일 때)
  5. 시가총액 >= min_cap_억(1000)

데이터는 KRX 전종목 일별 OHLCV 를 날짜별로 받아 메모리에 캐시합니다.
첫 실행은 60거래일치를 받아야 해서 1~3분 걸릴 수 있고, 이후엔 새 거래일만 추가로 받습니다.
서버 시작 시 백그라운드로 미리 받아 두므로(prefetch) 보통은 바로 응답합니다.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from pykrx import stock

log = logging.getLogger("screener")
KST = timezone(timedelta(hours=9))

# ────────────────────────────────────────────────────────────
# 캐시
# ────────────────────────────────────────────────────────────
_lock = threading.Lock()
_ohlcv_by_date: dict[str, pd.DataFrame] = {}   # "YYYYMMDD" -> DataFrame(index=ticker)
_holidays: set[str] = set()                     # 비어 있던 날짜 (휴장)
_cap_cache: tuple[str, pd.DataFrame] | None = None
_name_cache: dict[str, str] = {}
_prefetch_state: dict[str, Any] = {"running": False, "done": 0, "target": 0, "last_error": None}


def _today() -> datetime:
    return datetime.now(KST)


def _ymd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def _retry(fn, *args, tries: int = 3, **kwargs):
    last = None
    out = None
    for i in range(tries):
        try:
            out = fn(*args, **kwargs)
            if out is not None and not (isinstance(out, pd.DataFrame) and out.empty):
                return out
        except Exception as e:  # noqa: BLE001
            last = e
            log.warning("KRX 호출 실패 (%d/%d): %s", i + 1, tries, e)
        if i < tries - 1:
            time.sleep(1.5 * (i + 1))
    if out is None and last is not None:
        raise last
    return out if out is not None else pd.DataFrame()


def _market_ohlcv_one_day(ymd: str) -> pd.DataFrame:
    """해당 날짜 전종목 OHLCV (index=티커). 휴장이면 빈 DF."""
    fn = getattr(stock, "get_market_ohlcv", None) or getattr(stock, "get_market_ohlcv_by_ticker")
    try:
        df = _retry(fn, ymd, market="ALL")
    except TypeError:
        df = _retry(fn, ymd, "ALL")
    if df is None or df.empty:
        return pd.DataFrame()
    # 거래량 0 이고 시가 0 이면 사실상 휴장 데이터
    if "거래량" in df.columns and int(df["거래량"].sum()) == 0:
        return pd.DataFrame()
    keep = [c for c in ["시가", "고가", "저가", "종가", "거래량", "거래대금"] if c in df.columns]
    return df[keep].copy()


def _ensure_days(n_days: int, max_calendar_back: int = 130) -> list[str]:
    """최근 n_days 거래일의 날짜 목록(오름차순)을 캐시에 채우고 반환."""
    dates: list[str] = []
    d = _today()
    # 장 마감(15:30) 전이면 당일 데이터가 불완전하므로 전일부터
    if d.hour < 16:
        d = d - timedelta(days=1)
    back = 0
    while len(dates) < n_days and back < max_calendar_back:
        cur = d - timedelta(days=back)
        back += 1
        if cur.weekday() >= 5:
            continue
        ymd = _ymd(cur)
        if ymd in _holidays:
            continue
        with _lock:
            cached = _ohlcv_by_date.get(ymd)
        if cached is None:
            df = _market_ohlcv_one_day(ymd)
            if df.empty:
                _holidays.add(ymd)
                continue
            with _lock:
                _ohlcv_by_date[ymd] = df
            cached = df
        dates.append(ymd)
    dates.sort()
    return dates


def _market_caps(ymd: str) -> pd.DataFrame:
    global _cap_cache
    if _cap_cache and _cap_cache[0] == ymd:
        return _cap_cache[1]
    fn = getattr(stock, "get_market_cap", None) or getattr(stock, "get_market_cap_by_ticker")
    try:
        df = _retry(fn, ymd, market="ALL")
    except TypeError:
        df = _retry(fn, ymd, "ALL")
    if df is None or df.empty:
        df = pd.DataFrame(columns=["시가총액"])
    _cap_cache = (ymd, df)
    return df


def _net_purchases(start: str, end: str, market: str, investor: str) -> pd.DataFrame:
    fn = (getattr(stock, "get_market_net_purchases_of_equities", None)
          or getattr(stock, "get_market_net_purchases_of_equities_by_ticker", None))
    if fn is None:
        return pd.DataFrame()
    try:
        return _retry(fn, start, end, market, investor)
    except Exception as e:  # noqa: BLE001
        log.warning("수급 조회 실패 %s/%s: %s", market, investor, e)
        return pd.DataFrame()


def _flows(start: str, end: str) -> tuple[pd.Series, pd.Series, dict[str, str]]:
    """외국인·기관 20일 누적 순매수 금액(원), 종목명 사전."""
    foreign = {}
    inst = {}
    names: dict[str, str] = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        for inv, bucket in (("외국인", foreign), ("기관합계", inst)):
            df = _net_purchases(start, end, mkt, inv)
            if df is None or df.empty:
                continue
            col = "순매수거래대금" if "순매수거래대금" in df.columns else df.columns[-1]
            for t, row in df.iterrows():
                bucket[str(t)] = float(row[col])
                if "종목명" in df.columns:
                    names[str(t)] = str(row["종목명"])
    return pd.Series(foreign, dtype="float64"), pd.Series(inst, dtype="float64"), names


def _name(ticker: str, names: dict[str, str]) -> str:
    if ticker in names:
        return names[ticker]
    if ticker in _name_cache:
        return _name_cache[ticker]
    try:
        n = stock.get_market_ticker_name(ticker)
        _name_cache[ticker] = str(n)
        return str(n)
    except Exception:  # noqa: BLE001
        return ticker


# ────────────────────────────────────────────────────────────
# 핵심 계산 (순수 함수 — 테스트 가능)
# ────────────────────────────────────────────────────────────
def compute_metrics(close: pd.Series, volume: pd.Series,
                    lookback_ret: int = 20, lookback_high: int = 60,
                    base_window: int = 20, recent_window: int = 5,
                    vol_mult: float = 2.0) -> dict[str, Any] | None:
    """close/volume 은 날짜 오름차순. 데이터가 부족하면 None."""
    need = max(lookback_high, base_window + recent_window, lookback_ret + 1)
    if len(close) < need or len(volume) < need:
        return None
    c = close.astype(float).values
    v = volume.astype(float).values
    last = c[-1]
    if last <= 0:
        return None
    base = v[-(base_window + recent_window):-recent_window]
    base_avg = float(base.mean()) if len(base) else 0.0
    if base_avg <= 0:
        return None
    recent = v[-recent_window:]
    ratios = recent / base_avg
    spike_days = int((ratios >= vol_mult).sum())
    ret = last / c[-(lookback_ret + 1)] - 1.0
    high = float(c[-lookback_high:].max())
    dd = last / high - 1.0
    return {
        "close": last,
        "ret20": ret,
        "high60": high,
        "dd60": dd,
        "base_avg_vol": base_avg,
        "recent_vol_ratio": [round(float(r), 2) for r in ratios],
        "spike_days": spike_days,
        "max_ratio": float(ratios.max()),
        "today_ratio": float(ratios[-1]),
    }


def screen_frame(panel_close: pd.DataFrame, panel_vol: pd.DataFrame,
                 caps: pd.Series | None, foreign: pd.Series, inst: pd.Series,
                 *, min_cap_won: float, vol_mult: float, min_days: int,
                 ret_lo: float, ret_hi: float, dd_max: float, require_flow: bool,
                 names: dict[str, str] | None = None) -> tuple[list[dict], list[dict]]:
    """패널(열=티커, 행=날짜 오름차순)에서 통과/근접 목록 반환."""
    names = names or {}
    passed: list[dict] = []
    near: list[dict] = []
    for t in panel_close.columns:
        cap = float(caps.get(t, 0.0)) if caps is not None else 0.0
        if caps is not None and cap < min_cap_won:
            continue
        m = compute_metrics(panel_close[t].dropna(), panel_vol[t].dropna(), vol_mult=vol_mult)
        if m is None:
            continue
        f = float(foreign.get(t, 0.0))
        i = float(inst.get(t, 0.0))
        checks = {
            "거래량지속": bool(m["spike_days"] >= min_days),
            "20일등락": bool(ret_lo <= m["ret20"] <= ret_hi),
            "60일고점": bool(m["dd60"] >= dd_max),
            "수급": bool(f > 0 or i > 0) if require_flow else True,
        }
        row = {
            "code": t,
            "name": names.get(t, t),
            "close": int(m["close"]),
            "cap_억": round(cap / 1e8) if cap else None,
            "ret20_pct": round(float(m["ret20"]) * 100, 1),
            "dd60_pct": round(float(m["dd60"]) * 100, 1),
            "spike_days_of_5": m["spike_days"],
            "vol_ratio_recent5": m["recent_vol_ratio"],
            "foreign20_억": round(f / 1e8, 1),
            "inst20_억": round(i / 1e8, 1),
            "checks": checks,
        }
        n_ok = sum(checks.values())
        if n_ok == len(checks):
            passed.append(row)
        elif n_ok == len(checks) - 1 and m["spike_days"] >= 1:
            row["failed"] = [k for k, ok in checks.items() if not ok]
            near.append(row)
    passed.sort(key=lambda r: (-r["spike_days_of_5"], -max(r["vol_ratio_recent5"])))
    near.sort(key=lambda r: (-r["spike_days_of_5"], -max(r["vol_ratio_recent5"])))
    return passed, near


# ────────────────────────────────────────────────────────────
# 공개 API
# ────────────────────────────────────────────────────────────
def run_screen(min_cap_억: float = 1000, vol_mult: float = 2.0, min_days: int = 3,
               ret_lo_pct: float = -3.0, ret_hi_pct: float = 5.0, dd_max_pct: float = -10.0,
               require_flow: bool = True, lookback_days: int = 60,
               max_results: int = 30, include_near: bool = True) -> dict[str, Any]:
    t0 = time.time()
    dates = _ensure_days(lookback_days)
    if len(dates) < 30:
        return {"ok": False, "error": f"거래일 데이터 부족 ({len(dates)}일). KRX 접근 또는 KRX_ID/KRX_PW 설정을 확인하세요.",
                "prefetch": _prefetch_state}
    with _lock:
        frames = {d: _ohlcv_by_date[d] for d in dates}
    panel_close = pd.DataFrame({d: f["종가"] for d, f in frames.items()}).T.sort_index()
    panel_vol = pd.DataFrame({d: f["거래량"] for d, f in frames.items()}).T.sort_index()

    last = dates[-1]
    caps_df = _market_caps(last)
    caps = caps_df["시가총액"].astype(float) if "시가총액" in caps_df.columns else None
    caps = caps.rename(index=str) if caps is not None else None

    start20 = dates[-20] if len(dates) >= 20 else dates[0]
    foreign, inst, names = _flows(start20, last)
    flow_ok = not (foreign.empty and inst.empty)
    if require_flow and not flow_ok:
        log.warning("수급 데이터가 비어 있어 수급 조건을 건너뜁니다 (KRX 로그인 필요할 수 있음)")

    passed, near = screen_frame(
        panel_close, panel_vol, caps, foreign, inst,
        min_cap_won=min_cap_억 * 1e8, vol_mult=vol_mult, min_days=min_days,
        ret_lo=ret_lo_pct / 100, ret_hi=ret_hi_pct / 100, dd_max=dd_max_pct / 100,
        require_flow=require_flow and flow_ok, names=names,
    )
    for r in passed + near:
        if r["name"] == r["code"]:
            r["name"] = _name(r["code"], names)

    return {
        "ok": True,
        "as_of": f"{last[:4]}-{last[4:6]}-{last[6:]}",
        "universe_days": len(dates),
        "conditions": {
            "시가총액_억_이상": min_cap_억,
            "거래량": f"20일평균(최근5일 제외) 대비 {vol_mult}배 이상이 최근 5일 중 {min_days}일 이상",
            "20일등락률_pct": [ret_lo_pct, ret_hi_pct],
            "60일고점대비_pct_이내": dd_max_pct,
            "수급": "외국인 또는 기관 20일 누적 순매수 > 0" if (require_flow and flow_ok) else "미적용",
        },
        "passed_count": len(passed),
        "passed": passed[:max_results],
        "near_miss": near[:max_results] if include_near else [],
        "elapsed_sec": round(time.time() - t0, 1),
        "note": "사실 데이터 집계이며 매매 권유가 아닙니다. 통과 종목은 공시·뉴스로 거래량 증가 원인을 확인하세요.",
        "source": "KRX via pykrx",
    }


# ────────────────────────────────────────────────────────────
# 모멘텀 스크리너 (규칙 기반 점수, 예측 아님)
# ────────────────────────────────────────────────────────────
def momentum_metrics(close: pd.Series, value: pd.Series | None, lookback: int = 120) -> dict[str, Any] | None:
    """close/value(거래대금) 는 날짜 오름차순. 최소 61일 필요."""
    if len(close) < 61:
        return None
    c = close.astype(float).values
    last = c[-1]
    if last <= 0:
        return None
    def ret(n):
        return last / c[-(n + 1)] - 1.0 if len(c) > n else None
    r20, r60 = ret(20), ret(60)
    r120 = ret(120) if len(c) > 120 else None
    ma20 = float(c[-20:].mean()); ma60 = float(c[-60:].mean())
    high = float(c[-min(len(c), 250):].max())
    near_high = last / high - 1.0
    tv_ratio = None
    if value is not None and len(value) >= 80:
        v = value.astype(float).values
        prev = float(v[-80:-20].mean())
        tv_ratio = float(v[-20:].mean()) / prev if prev > 0 else None
    return {"close": last, "r20": r20, "r60": r60, "r120": r120,
            "above_ma": last > ma20 > ma60, "near_high": near_high, "tv_ratio": tv_ratio}


def momentum_score(m: dict[str, Any], f: float, i: float) -> tuple[int, dict[str, bool]]:
    checks = {
        "60일수익률>10%": (m["r60"] or 0) > 0.10,
        "20일수익률>0": (m["r20"] or 0) > 0,
        "정배열(종가>20일선>60일선)": bool(m["above_ma"]),
        "52주고점-5%이내": m["near_high"] >= -0.05,
        "거래대금증가(20일>직전60일)": (m["tv_ratio"] or 0) > 1.0,
        "외국인또는기관20일순매수": (f > 0 or i > 0),
    }
    checks = {k: bool(v) for k, v in checks.items()}
    return sum(checks.values()), checks


def run_momentum(min_cap_억: float = 1000, min_score: int = 4, max_results: int = 20,
                 exclude_overheated_pct: float = 40.0) -> dict[str, Any]:
    """규칙 기반 모멘텀 점수(0~6) 상위 종목. exclude_overheated_pct: 20일 수익률이 이보다 크면 과열로 제외."""
    t0 = time.time()
    dates = _ensure_days(125)
    if len(dates) < 61:
        return {"ok": False, "error": f"거래일 데이터 부족 ({len(dates)}일).", "prefetch": _prefetch_state}
    with _lock:
        frames = {d: _ohlcv_by_date[d] for d in dates}
    panel_close = pd.DataFrame({d: f["종가"] for d, f in frames.items()}).T.sort_index()
    panel_val = (pd.DataFrame({d: f["거래대금"] for d, f in frames.items() if "거래대금" in f.columns}).T.sort_index()
                 if all("거래대금" in f.columns for f in frames.values()) else None)
    last = dates[-1]
    caps_df = _market_caps(last)
    caps = caps_df["시가총액"].astype(float).rename(index=str) if "시가총액" in caps_df.columns else None
    foreign, inst, names = _flows(dates[-20], last)
    rows = []
    for t in panel_close.columns:
        cap = float(caps.get(t, 0.0)) if caps is not None else 0.0
        if caps is not None and cap < min_cap_억 * 1e8:
            continue
        m = momentum_metrics(panel_close[t].dropna(), panel_val[t].dropna() if panel_val is not None else None)
        if m is None:
            continue
        if (m["r20"] or 0) * 100 > exclude_overheated_pct:
            continue
        f, i = float(foreign.get(t, 0.0)), float(inst.get(t, 0.0))
        score, checks = momentum_score(m, f, i)
        if score < min_score:
            continue
        rows.append({
            "code": t, "name": names.get(t, t), "close": int(m["close"]), "cap_억": round(cap / 1e8) if cap else None,
            "score": score,
            "ret20_pct": round(float(m["r20"]) * 100, 1), "ret60_pct": round(float(m["r60"]) * 100, 1),
            "ret120_pct": round(float(m["r120"]) * 100, 1) if m["r120"] is not None else None,
            "from_52w_high_pct": round(float(m["near_high"]) * 100, 1),
            "turnover_ratio_20v60": round(float(m["tv_ratio"]), 2) if m["tv_ratio"] else None,
            "foreign20_억": round(f / 1e8, 1), "inst20_억": round(i / 1e8, 1),
            "checks": {k: bool(v) for k, v in checks.items()},
        })
    rows.sort(key=lambda r: (-r["score"], -(r["ret60_pct"] or 0)))
    for r in rows[:max_results]:
        if r["name"] == r["code"]:
            r["name"] = _name(r["code"], names)
    return {
        "ok": True, "as_of": f"{last[:4]}-{last[4:6]}-{last[6:]}",
        "method": "규칙 기반 모멘텀 점수(6개 조건 충족 수). 예측이 아니라 현재 상태의 분류입니다.",
        "conditions": ["60일 수익률 > 10%", "20일 수익률 > 0", "종가 > 20일선 > 60일선", "52주 고점 대비 -5% 이내",
                       "20일 평균 거래대금 > 직전 60일 평균", "외국인 또는 기관 20일 순매수 > 0",
                       f"과열 제외: 20일 수익률 > {exclude_overheated_pct}%"],
        "count": len(rows), "results": rows[:max_results],
        "elapsed_sec": round(time.time() - t0, 1),
        "note": "모멘텀 전략은 상승장에서 유리하고 추세 전환기에 큰 손실이 날 수 있습니다. 매매 권유가 아닙니다.",
        "source": "KRX via pykrx",
    }


# ────────────────────────────────────────────────────────────
# 백테스트: 과거 날짜마다 같은 조건을 적용하고 이후 수익률 측정
# ────────────────────────────────────────────────────────────
def _index_close(start: str, end: str, code: str = "1001") -> pd.Series | None:
    try:
        df = _retry(stock.get_index_ohlcv, start, end, code)
        if df is None or df.empty or "종가" not in df.columns:
            return None
        s = df["종가"].astype(float)
        s.index = [d.strftime("%Y%m%d") for d in pd.to_datetime(s.index)]
        return s
    except Exception as e:  # noqa: BLE001
        log.warning("지수 조회 실패: %s", e)
        return None


def run_backtest(min_cap_억: float = 1000, vol_mult: float = 2.0, min_days: int = 3,
                 ret_lo_pct: float = -3.0, ret_hi_pct: float = 5.0, dd_max_pct: float = -10.0,
                 hold_days: int = 20, step: int = 1, require_flow: bool = False,
                 max_signals_listed: int = 60) -> dict[str, Any]:
    """캐시된 거래일 전체를 대상으로 날짜별 신호 → hold_days 뒤 수익률(시장 대비)."""
    t0 = time.time()
    dates = _ensure_days(125)
    need = 60 + hold_days + 1
    if len(dates) < need:
        return {"ok": False, "error": f"거래일 부족 ({len(dates)}일, 최소 {need}일 필요)"}
    with _lock:
        frames = {d: _ohlcv_by_date[d] for d in dates}
    panel_close = pd.DataFrame({d: f["종가"] for d, f in frames.items()}).T.sort_index()
    panel_vol = pd.DataFrame({d: f["거래량"] for d, f in frames.items()}).T.sort_index()
    caps_df = _market_caps(dates[-1])
    caps = caps_df["시가총액"].astype(float).rename(index=str) if "시가총액" in caps_df.columns else None
    bench = _index_close(dates[0], dates[-1])

    signals: list[dict] = []
    per_date: list[dict] = []
    flow_cache: dict[str, tuple[pd.Series, pd.Series]] = {}
    empty = pd.Series(dtype="float64")
    for i in range(59, len(dates) - hold_days, step):
        d = dates[i]
        d_fwd = dates[i + hold_days]
        d_half = dates[i + max(1, hold_days // 2)]
        pc = panel_close.iloc[: i + 1]
        pv = panel_vol.iloc[: i + 1]
        if require_flow:
            if d not in flow_cache:
                f, ins, _ = _flows(dates[i - 19], d)
                flow_cache[d] = (f, ins)
            foreign, inst = flow_cache[d]
        else:
            foreign, inst = empty, empty
        passed, _near = screen_frame(
            pc, pv, caps, foreign, inst,
            min_cap_won=min_cap_억 * 1e8, vol_mult=vol_mult, min_days=min_days,
            ret_lo=ret_lo_pct / 100, ret_hi=ret_hi_pct / 100, dd_max=dd_max_pct / 100,
            require_flow=require_flow,
        )
        b_ret = None
        if bench is not None and d in bench.index and d_fwd in bench.index:
            b_ret = float(bench[d_fwd] / bench[d] - 1.0)
        day_rets = []
        for r in passed:
            t = r["code"]
            c0 = float(panel_close.at[d, t]) if t in panel_close.columns else None
            c1 = float(panel_close.at[d_fwd, t]) if t in panel_close.columns else None
            ch = float(panel_close.at[d_half, t]) if t in panel_close.columns else None
            if not c0 or not c1 or pd.isna(c0) or pd.isna(c1) or c0 <= 0:
                continue
            fwd = c1 / c0 - 1.0
            half = (ch / c0 - 1.0) if ch and not pd.isna(ch) else None
            # 보유기간 중 최고/최저 (종가 기준)
            path = panel_close[t].iloc[i: i + hold_days + 1].astype(float)
            mx = float(path.max() / c0 - 1.0); mn = float(path.min() / c0 - 1.0)
            signals.append({"date": d, "code": t, "close": int(c0), "fwd_pct": round(fwd * 100, 1),
                            "half_pct": round(half * 100, 1) if half is not None else None,
                            "max_pct": round(mx * 100, 1), "min_pct": round(mn * 100, 1),
                            "bench_pct": round(b_ret * 100, 1) if b_ret is not None else None,
                            "excess_pct": round((fwd - b_ret) * 100, 1) if b_ret is not None else None,
                            "spike_days": r["spike_days_of_5"]})
            day_rets.append(fwd)
        per_date.append({"date": d, "signals": len(passed), "avg_fwd_pct": round(sum(day_rets) / len(day_rets) * 100, 1) if day_rets else None,
                         "bench_pct": round(b_ret * 100, 1) if b_ret is not None else None})

    n = len(signals)
    if n == 0:
        return {"ok": True, "signals": 0, "dates_tested": len(per_date), "message": "조건을 통과한 신호가 없었습니다.",
                "elapsed_sec": round(time.time() - t0, 1)}
    fw = [s["fwd_pct"] for s in signals]
    ex = [s["excess_pct"] for s in signals if s["excess_pct"] is not None]
    hf = [s["half_pct"] for s in signals if s["half_pct"] is not None]
    def _avg(x): return round(sum(x) / len(x), 2) if x else None
    def _med(x):
        if not x: return None
        y = sorted(x); m = len(y) // 2
        return round((y[m] if len(y) % 2 else (y[m - 1] + y[m]) / 2), 2)
    summary = {
        "signals": n, "dates_tested": len(per_date), "hold_days": hold_days,
        "avg_fwd_pct": _avg(fw), "median_fwd_pct": _med(fw),
        "win_rate_pct": round(sum(1 for x in fw if x > 0) / n * 100, 1),
        "avg_half_pct": _avg(hf),
        "avg_bench_pct": _avg([s["bench_pct"] for s in signals if s["bench_pct"] is not None]),
        "avg_excess_pct": _avg(ex), "median_excess_pct": _med(ex),
        "beat_bench_rate_pct": round(sum(1 for x in ex if x > 0) / len(ex) * 100, 1) if ex else None,
        "avg_max_runup_pct": _avg([s["max_pct"] for s in signals]),
        "avg_max_drawdown_pct": _avg([s["min_pct"] for s in signals]),
        "big_winners_10pct_plus": sum(1 for x in fw if x >= 10),
        "big_losers_10pct_minus": sum(1 for x in fw if x <= -10),
    }
    by_spike = {}
    for k in (3, 4, 5):
        sub = [s["fwd_pct"] for s in signals if s["spike_days"] == k]
        if sub:
            by_spike[f"spike_days={k}"] = {"n": len(sub), "avg_fwd_pct": _avg(sub),
                                           "win_rate_pct": round(sum(1 for x in sub if x > 0) / len(sub) * 100, 1)}
    signals_sorted = sorted(signals, key=lambda s: s["date"])
    return {
        "ok": True,
        "period": f"{dates[59]} ~ {dates[len(dates) - hold_days - 1]} (신호일 기준)",
        "conditions": {"시가총액_억_이상": min_cap_억, "거래량배수": vol_mult, "지속일": min_days,
                       "20일등락률_pct": [ret_lo_pct, ret_hi_pct], "60일고점대비_pct": dd_max_pct,
                       "수급조건": "적용" if require_flow else "미적용(속도 때문에 기본 제외)"},
        "benchmark": "KOSPI(1001)" if bench is not None else "없음",
        "summary": summary, "by_spike_days": by_spike,
        "per_date": per_date[-40:],
        "signals": signals_sorted[-max_signals_listed:],
        "caveats": ["시가총액은 최신 기준으로 고정(과거 시총 변동 미반영)", "생존편향: 상장폐지 종목은 캐시에 없음",
                    "종가 체결 가정, 거래비용 미반영", "표본이 작으면 결과를 신뢰하기 어려움"],
        "elapsed_sec": round(time.time() - t0, 1),
        "note": "과거 통계이며 미래 수익을 보장하지 않습니다. 매매 권유가 아닙니다.",
    }


def prefetch(days: int = 125) -> None:
    """서버 시작 시 백그라운드로 캐시를 채웁니다."""
    if _prefetch_state["running"]:
        return

    def _job():
        _prefetch_state.update(running=True, done=0, target=days, last_error=None)
        try:
            dates = _ensure_days(days)
            _prefetch_state["done"] = len(dates)
        except Exception as e:  # noqa: BLE001
            _prefetch_state["last_error"] = str(e)
        finally:
            _prefetch_state["running"] = False

    threading.Thread(target=_job, name="screener-prefetch", daemon=True).start()


def cache_status() -> dict[str, Any]:
    with _lock:
        n = len(_ohlcv_by_date)
        latest = max(_ohlcv_by_date) if _ohlcv_by_date else None
    return {"cached_days": n, "latest": latest, "prefetch": dict(_prefetch_state)}


if os.environ.get("SCREENER_PREFETCH", "1") != "0":
    prefetch()
