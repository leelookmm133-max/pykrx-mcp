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
서버 시작 시 백그라운드로 PREFETCH_DAYS(기본 250거래일 ≈ 1년)를 미리 받아 두므로(prefetch)
스크리너는 보통 바로 응답하고, 백테스트(run_backtest)는 1년치 신호를 한 번에 집계합니다.
캐시가 아직 부족하면 백테스트는 기다리지 않고 status="preparing" 을 돌려줍니다.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
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
_prefetch_state: dict[str, Any] = {"running": False, "done": 0, "target": 0, "last_error": None,
                                   "finished_target": 0}
# 백테스트용 보조 캐시
_flow_cache: dict[tuple[str, str], tuple[pd.Series, pd.Series]] = {}   # (start,end) -> (외국인, 기관)
_flow_job: dict[str, Any] = {"running": False, "done": 0, "target": 0, "last_error": None}
_index_cache: dict[tuple[str, str, str], pd.Series | None] = {}
_kosdaq_cache: tuple[str, set[str]] | None = None

# 서버 시작 시 미리 받아둘 거래일 수. 백테스트 1년치(신호 약 170거래일)를 바로 돌리려면 250 이상.
PREFETCH_DAYS = int(os.environ.get("SCREENER_PREFETCH_DAYS", "250"))
# KRX 연속 호출 사이 간격(초). 수백 일을 연달아 받을 때 차단을 피하기 위한 여유.
FETCH_DELAY = float(os.environ.get("SCREENER_FETCH_DELAY", "0.3"))


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
    # 휴장일은 정상적으로 빈 응답이므로 재시도는 1회만(1.5초). 1년치에 휴장일이 15일 안팎이라 시간 절약.
    try:
        df = _retry(fn, ymd, market="ALL", tries=2)
    except TypeError:
        df = _retry(fn, ymd, "ALL", tries=2)
    if df is None or df.empty:
        return pd.DataFrame()
    # 거래량 0 이고 시가 0 이면 사실상 휴장 데이터
    if "거래량" in df.columns and int(df["거래량"].sum()) == 0:
        return pd.DataFrame()
    keep = [c for c in ["시가", "고가", "저가", "종가", "거래량", "거래대금"] if c in df.columns]
    return df[keep].copy()


def _ensure_days(n_days: int, max_calendar_back: int | None = None) -> list[str]:
    """최근 n_days 거래일의 날짜 목록(오름차순)을 캐시에 채우고 반환.

    max_calendar_back 은 달력 기준 최대 소급 일수. 예전엔 130으로 고정돼 있어 거래일 88일에서
    멈췄으므로(125일 목표 미달), 지금은 n_days 에 비례해 자동 계산합니다(1년≈365일 → 250거래일).
    """
    if max_calendar_back is None:
        max_calendar_back = int(n_days * 1.6) + 40
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
            if FETCH_DELAY > 0:
                time.sleep(FETCH_DELAY)
            if df.empty:
                _holidays.add(ymd)
                continue
            with _lock:
                _ohlcv_by_date[ymd] = df
            cached = df
        dates.append(ymd)
    dates.sort()
    return dates


def _cached_dates() -> list[str]:
    with _lock:
        return sorted(_ohlcv_by_date)


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
#
# 설계
#   - 조건 계산은 compute_metrics 와 같은 정의를 패널(행=날짜, 열=티커) 전체에 벡터화해
#     한 번에 구합니다. 1년치(250거래일 × 전종목)도 수 초면 끝나므로 MCP 호출이 시간 초과되지 않습니다.
#   - 시가총액은 최신 시총을 가격 비율로 환산해 신호일 기준으로 씁니다(cap_mode="scaled").
#     최신 시총을 그대로 쓰면 "그동안 오른 종목만 포함"되는 미래 참조 편향이 생깁니다.
#   - 벤치마크는 종목이 속한 시장 지수(KOSPI/KOSDAQ)와, 같은 날 시총 조건을 만족한 전체 종목의 평균
#     수익률(universe) 두 가지를 같이 냅니다. 후자가 "아무 종목이나 골랐을 때"와의 비교입니다.
#   - 같은 종목이 연속으로 신호가 나면 표본이 중복되므로, 직전 cooldown 거래일 안에 신호가 없었던
#     첫 신호(entries)만 따로 집계합니다.
#   - 캐시가 부족하면 백그라운드로 받기 시작하고 즉시 status="preparing" 을 돌려줍니다.
# ────────────────────────────────────────────────────────────
def _index_close(start: str, end: str, code: str = "1001") -> pd.Series | None:
    key = (start, end, code)
    if key in _index_cache:
        return _index_cache[key]
    out: pd.Series | None = None
    try:
        df = _retry(stock.get_index_ohlcv, start, end, code)
        if df is not None and not df.empty and "종가" in df.columns:
            s = df["종가"].astype(float)
            s.index = [d.strftime("%Y%m%d") for d in pd.to_datetime(s.index)]
            out = s
    except Exception as e:  # noqa: BLE001
        log.warning("지수 조회 실패(%s): %s", code, e)
    _index_cache[key] = out
    return out


def _kosdaq_tickers(ymd: str) -> set[str]:
    """해당 날짜 코스닥 상장 종목 집합. 실패하면 빈 집합(→ 전부 KOSPI 벤치마크)."""
    global _kosdaq_cache
    if _kosdaq_cache and _kosdaq_cache[0] == ymd:
        return _kosdaq_cache[1]
    out: set[str] = set()
    try:
        lst = _retry(stock.get_market_ticker_list, ymd, market="KOSDAQ")
        out = {str(t) for t in (lst if lst is not None else [])}
    except Exception as e:  # noqa: BLE001
        log.warning("코스닥 종목 목록 조회 실패: %s", e)
    _kosdaq_cache = (ymd, out)
    return out


def _name_map(ymd: str) -> dict[str, str]:
    """전종목 티커→종목명 (KRX 1회 호출, 캐시). 실패 시 빈 dict."""
    if _name_cache.get("__asof__") == ymd:
        return _name_cache
    try:
        from pykrx.website import krx as _krx  # pykrx 내부 모듈 (전종목시세 1회 호출)
        s = _retry(_krx.get_market_ticker_and_name, ymd, "ALL")
        if s is not None and len(s):
            _name_cache.update({str(k): str(v) for k, v in s.items()})
            _name_cache["__asof__"] = ymd
    except Exception as e:  # noqa: BLE001
        log.warning("종목명 일괄 조회 실패: %s", e)
    return _name_cache


def _parse_horizons(horizons: Any, hold_days: int) -> list[int]:
    if horizons is None or horizons == "":
        hs = [5, 10, 20, 40]
    elif isinstance(horizons, str):
        hs = [int(x) for x in horizons.replace(" ", "").split(",") if x]
    else:
        hs = [int(x) for x in horizons]
    hs.append(int(hold_days))
    hs = sorted({h for h in hs if 1 <= h <= 120})
    return hs or [int(hold_days)]


def signal_matrix(panel_close: pd.DataFrame, panel_vol: pd.DataFrame, *,
                  vol_mult: float, min_days: int, ret_lo: float, ret_hi: float, dd_max: float,
                  lookback_ret: int = 20, lookback_high: int = 60,
                  base_window: int = 20, recent_window: int = 5) -> dict[str, pd.DataFrame]:
    """compute_metrics 와 같은 정의를 패널 전체에 벡터화. 반환값은 모두 panel 과 같은 모양.

    - base: 최근 recent_window 일을 제외한 직전 base_window 일 평균 거래량
    - spike_days: 최근 recent_window 일 중 (거래량 >= vol_mult × base) 인 날 수 (base 는 당일 기준 고정)
    - ret: lookback_ret 일 등락률, dd: lookback_high 일 종가 고점 대비
    - ok: 거래량·등락률·고점 세 조건(수급·시총 제외) 통과 여부
    """
    c = panel_close.astype(float)
    v = panel_vol.astype(float)
    base = v.shift(recent_window).rolling(base_window, min_periods=base_window).mean()
    thresh = base * vol_mult
    spike = None
    for k in range(recent_window):
        hit = (v.shift(k) >= thresh).astype(int)
        spike = hit if spike is None else spike + hit
    ret = c / c.shift(lookback_ret) - 1.0
    high = c.rolling(lookback_high, min_periods=lookback_high).max()
    dd = c / high - 1.0
    today_ratio = v / base
    valid = base.notna() & (base > 0) & high.notna() & ret.notna() & (c > 0)
    ok = valid & (spike >= min_days) & (ret >= ret_lo) & (ret <= ret_hi) & (dd >= dd_max)
    return {"ok": ok, "valid": valid, "spike_days": spike, "ret": ret, "dd": dd, "today_ratio": today_ratio}


def _pick(df: pd.DataFrame, mask: pd.DataFrame) -> np.ndarray:
    """mask 가 True 인 칸의 값만 1차원 배열로 (결측 제거). pandas 버전에 무관."""
    vals = df.values[mask.values]
    return vals[~np.isnan(vals)]


def _stats(x) -> dict[str, Any]:
    x = np.asarray(x, dtype=float)
    x = x[~np.isnan(x)]
    n = int(len(x))
    if n == 0:
        return {"n": 0}
    return {"n": n, "avg_pct": round(float(x.mean()) * 100, 2), "median_pct": round(float(np.median(x)) * 100, 2),
            "win_rate_pct": round(float((x > 0).mean()) * 100, 1)}


def _flows_for_dates(dates: list[str], idx_list: list[int]) -> None:
    """백그라운드: 신호일마다 20일 수급(외국인·기관)을 받아 _flow_cache 에 채움."""
    def _job():
        _flow_job.update(running=True, done=0, target=len(idx_list), last_error=None)
        try:
            for n, i in enumerate(idx_list):
                key = (dates[i - 19], dates[i])
                if key not in _flow_cache:
                    f, ins, _ = _flows(*key)
                    _flow_cache[key] = (f, ins)
                    if FETCH_DELAY > 0:
                        time.sleep(FETCH_DELAY)
                _flow_job["done"] = n + 1
        except Exception as e:  # noqa: BLE001
            _flow_job["last_error"] = str(e)
        finally:
            _flow_job["running"] = False
    threading.Thread(target=_job, name="backtest-flows", daemon=True).start()


def run_backtest(min_cap_억: float = 1000, vol_mult: float = 2.0, min_days: int = 3,
                 ret_lo_pct: float = -3.0, ret_hi_pct: float = 5.0, dd_max_pct: float = -10.0,
                 hold_days: int = 20, step: int = 1, require_flow: bool = False,
                 max_signals_listed: int = 40, lookback_days: int = 250,
                 horizons: Any = None, cap_mode: str = "scaled", cooldown: int = 20) -> dict[str, Any]:
    """캐시된 거래일(lookback_days)을 대상으로 날짜별 신호 → 여러 보유기간(horizons) 뒤 수익률 집계."""
    t0 = time.time()
    hold_days = int(hold_days)
    hs = _parse_horizons(horizons, hold_days)
    max_h = max(hs)
    lookback_days = int(max(lookback_days, 60 + max_h + 5))

    # 1) 데이터 준비 상태 확인 — 부족하면 백그라운드로 받고 바로 상태만 돌려준다 (MCP 시간 초과 방지)
    have = _cached_dates()
    if len(have) < lookback_days:
        st = dict(_prefetch_state)
        if st["running"]:
            return {"ok": False, "status": "preparing", "cached_days": len(have), "target_days": st["target"],
                    "message": f"거래일 데이터를 받는 중입니다 ({st['done']}/{st['target']}). 1~2분 뒤 다시 실행하세요.",
                    "hint": "screener_cache_status 로 진행률을 볼 수 있습니다."}
        if st.get("finished_target", 0) < lookback_days:
            prefetch(lookback_days)
            return {"ok": False, "status": "preparing", "cached_days": len(have), "target_days": lookback_days,
                    "message": f"{lookback_days}거래일 데이터를 받기 시작했습니다(현재 {len(have)}일). "
                               "하루당 1~2초이므로 몇 분 뒤 다시 실행하세요.",
                    "hint": "screener_cache_status 로 진행률을 볼 수 있습니다."}
        # 이미 받아봤는데 모자란 경우(KRX 오류 등): 있는 만큼으로 진행
    dates = have[-lookback_days:]
    need = 60 + max_h + 1
    if len(dates) < need:
        return {"ok": False, "error": f"거래일 부족 ({len(dates)}일, 최소 {need}일 필요)", "prefetch": dict(_prefetch_state)}

    with _lock:
        frames = {d: _ohlcv_by_date[d] for d in dates}
    panel_close = pd.DataFrame({d: f["종가"] for d, f in frames.items()}).T.sort_index()
    panel_vol = pd.DataFrame({d: f["거래량"] for d, f in frames.items()}).T.sort_index()
    panel_close.columns = [str(c) for c in panel_close.columns]
    panel_vol.columns = [str(c) for c in panel_vol.columns]
    panel_close = panel_close.astype(float)
    panel_vol = panel_vol.astype(float)
    # 거래정지 등으로 종가 0 이 들어오면 결측 처리
    panel_close = panel_close.where(panel_close > 0)
    tickers = panel_close.columns

    # 2) 시가총액: 최신 시총을 가격 비율로 환산(scaled) 또는 최신값 고정(latest)
    caps_df = _market_caps(dates[-1])
    caps = caps_df["시가총액"].astype(float).rename(index=str) if "시가총액" in caps_df.columns else None
    if caps is not None and len(caps):
        caps = caps.reindex(tickers)
        if cap_mode == "scaled":
            last_close = panel_close.ffill().iloc[-1]
            cap_panel = panel_close.div(last_close, axis=1).mul(caps, axis=1)
        else:
            cap_panel = pd.DataFrame(1.0, index=panel_close.index, columns=tickers).mul(caps, axis=1)
        cap_ok = cap_panel >= (min_cap_억 * 1e8)
    else:
        cap_panel = None
        cap_ok = pd.DataFrame(True, index=panel_close.index, columns=tickers)

    # 3) 조건 행렬
    sm = signal_matrix(panel_close, panel_vol, vol_mult=vol_mult, min_days=int(min_days),
                       ret_lo=ret_lo_pct / 100, ret_hi=ret_hi_pct / 100, dd_max=dd_max_pct / 100)
    ok = sm["ok"] & cap_ok
    universe = sm["valid"] & cap_ok

    # 신호일 범위: 60일째부터(지표 계산에 60일 필요), step 간격
    sig_idx = list(range(59, len(dates), max(1, int(step))))
    row_mask = pd.Series(False, index=panel_close.index)
    row_mask.iloc[sig_idx] = True
    ok.loc[~row_mask.values, :] = False
    universe.loc[~row_mask.values, :] = False

    # 4) 수급 조건 (선택): 신호 후보가 있는 날짜마다 20일 수급을 받아야 하므로 백그라운드 + 캐시
    flow_note = "미적용"
    if require_flow:
        cand_days = [i for i in sig_idx if bool(ok.iloc[i].any())]
        missing = [i for i in cand_days if (dates[i - 19], dates[i]) not in _flow_cache]
        if missing:
            if not _flow_job["running"]:
                _flows_for_dates(dates, missing)
            return {"ok": False, "status": "preparing_flows",
                    "message": f"신호일 {len(cand_days)}일 중 {len(cand_days) - len(missing)}일 수급 준비됨. "
                               f"남은 {len(missing)}일을 받는 중입니다(하루당 4회 KRX 조회). 잠시 후 다시 실행하세요.",
                    "flow_job": dict(_flow_job)}
        for i in cand_days:
            f, ins = _flow_cache[(dates[i - 19], dates[i])]
            fl = f.reindex(tickers).fillna(0.0)
            il = ins.reindex(tickers).fillna(0.0)
            ok.iloc[i] = ok.iloc[i].values & ((fl > 0) | (il > 0)).values
        flow_note = "적용(외국인 또는 기관 20일 누적 순매수 > 0)"

    # 5) 첫 신호(entries): 직전 cooldown 거래일 안에 신호가 없었던 경우
    okf = ok.astype(float)
    prev = okf.rolling(int(cooldown), min_periods=1).sum().shift(1).fillna(0.0)
    entry = ok & (prev == 0)

    # 6) 벤치마크: 종목 시장별 지수 + 같은 날 유니버스 평균
    kosdaq = _kosdaq_tickers(dates[-1])
    kospi_idx = _index_close(dates[0], dates[-1], "1001")
    kosdaq_idx = _index_close(dates[0], dates[-1], "2001")
    is_kosdaq = pd.Series([c in kosdaq for c in tickers], index=tickers, dtype=bool)

    def _bench_fwd(h: int) -> pd.DataFrame:
        """행=날짜, 열=티커: 해당 종목 시장 지수의 h일 수익률 (지수 없으면 NaN)."""
        out = pd.DataFrame(float("nan"), index=panel_close.index, columns=tickers)
        for s, mask in ((kospi_idx, ~is_kosdaq), (kosdaq_idx, is_kosdaq)):
            if s is None or not bool(mask.any()):
                continue
            s2 = s.reindex(panel_close.index)
            r = (s2.shift(-h) / s2 - 1.0).values
            cols = tickers[mask.values]
            out.loc[:, cols] = pd.DataFrame(r[:, None].repeat(len(cols), axis=1), index=panel_close.index, columns=cols)
        return out

    # 7) horizon 별 집계
    c = panel_close
    results: dict[str, Any] = {}
    entries_results: dict[str, Any] = {}
    main: dict[str, Any] = {}
    for h in hs:
        fwd = c.shift(-h) / c - 1.0
        # 보유 중 최고/최저 (신호일 포함 h+1 일, 종가 기준)
        rev = c.iloc[::-1]
        mx = rev.rolling(h + 1, min_periods=h + 1).max().iloc[::-1] / c - 1.0
        mn = rev.rolling(h + 1, min_periods=h + 1).min().iloc[::-1] / c - 1.0
        bench = _bench_fwd(h)
        excess = fwd - bench
        sel = ok & fwd.notna()
        esel = entry & fwd.notna()
        f_all = _pick(fwd, sel)
        if len(f_all) == 0:
            results[f"{h}d"] = {"n": 0}
            entries_results[f"{h}d"] = {"n": 0}
            if h == hold_days:
                main = {}
            continue
        ex_all = _pick(excess, sel)
        mx_all = _pick(mx, sel)
        mn_all = _pick(mn, sel)
        uni = fwd.where(universe & fwd.notna())
        uni_by_date = uni.mean(axis=1)                      # 날짜별 유니버스 평균
        sig_dates = sel.any(axis=1)
        uni_avg = float(uni_by_date[sig_dates].mean()) if bool(sig_dates.any()) else None
        if uni_avg is not None and pd.isna(uni_avg):
            uni_avg = None
        st = _stats(f_all)
        st.update({
            "avg_excess_vs_index_pct": round(float(ex_all.mean()) * 100, 2) if len(ex_all) else None,
            "beat_index_rate_pct": round(float((ex_all > 0).mean()) * 100, 1) if len(ex_all) else None,
            "universe_avg_pct": round(uni_avg * 100, 2) if uni_avg is not None else None,
            "avg_vs_universe_pct": round((float(f_all.mean()) - uni_avg) * 100, 2) if uni_avg is not None else None,
            "avg_max_runup_pct": round(float(mx_all.mean()) * 100, 2) if len(mx_all) else None,
            "avg_max_drawdown_pct": round(float(mn_all.mean()) * 100, 2) if len(mn_all) else None,
            "share_ge_10pct": round(float((f_all >= 0.10).mean()) * 100, 1),
            "share_le_m10pct": round(float((f_all <= -0.10).mean()) * 100, 1),
        })
        results[f"{h}d"] = st
        f_ent = _pick(fwd, esel)
        est = _stats(f_ent)
        if est["n"]:
            ex_ent = _pick(excess, esel)
            est["avg_excess_vs_index_pct"] = round(float(ex_ent.mean()) * 100, 2) if len(ex_ent) else None
            est["avg_vs_universe_pct"] = round((float(f_ent.mean()) - uni_avg) * 100, 2) if uni_avg is not None else None
            est["share_ge_10pct"] = round(float((f_ent >= 0.10).mean()) * 100, 1)
        entries_results[f"{h}d"] = est
        if h == hold_days:
            main = {"fwd": fwd, "excess": excess, "mx": mx, "mn": mn, "sel": sel, "esel": esel,
                    "uni_by_date": uni_by_date, "bench": bench}

    n_signals = int(ok.values.sum())
    n_entries = int(entry.values.sum())
    sig_from = dates[sig_idx[0]] if sig_idx else None
    sig_to = dates[sig_idx[-1]] if sig_idx else None
    base_out = {
        "ok": True,
        "period": {"signal_from": sig_from, "signal_to": sig_to, "signal_dates": len(sig_idx),
                   "data_from": dates[0], "data_to": dates[-1], "trading_days_used": len(dates)},
        "conditions": {"시가총액_억_이상": min_cap_억, "시가총액_기준": "신호일 가격으로 환산" if cap_mode == "scaled" else "최신 고정",
                       "거래량배수": vol_mult, "지속일": f"최근 5일 중 {int(min_days)}일 이상",
                       "20일등락률_pct": [ret_lo_pct, ret_hi_pct], "60일고점대비_pct": dd_max_pct,
                       "수급조건": flow_note},
        "signals_total": n_signals, "entries_first_signal_only": n_entries, "cooldown_days": int(cooldown),
        "benchmark": {"index": "종목 시장별 KOSPI(1001)/KOSDAQ(2001)" if (kospi_idx is not None) else "없음",
                      "universe": "같은 날 시총 조건을 만족한 전종목 평균(아무 종목이나 골랐을 때)"},
        "summary_by_horizon": results,
        "entries_by_horizon": entries_results,
    }
    if n_signals == 0 or not main:
        base_out.update({"message": "조건을 통과한 신호가 없었습니다." if n_signals == 0
                         else f"보유기간 {hold_days}일 뒤 가격이 있는 신호가 없습니다.",
                         "elapsed_sec": round(time.time() - t0, 1)})
        return base_out

    fwd, excess, mx, mn, sel, esel = (main[k] for k in ("fwd", "excess", "mx", "mn", "sel", "esel"))
    uni_by_date, bench = main["uni_by_date"], main["bench"]
    # 8) 세부: 지속일(3/4/5)별, 월별, 날짜별, 개별 신호
    spike = sm["spike_days"]
    by_spike = {}
    for k in (3, 4, 5):
        x = _pick(fwd, sel & (spike == k))
        if len(x):
            by_spike[f"spike_days={k}"] = _stats(x)
    sel_rows = sel.any(axis=1)
    sel_dates = list(sel.index[sel_rows.values])
    by_month_acc: dict[str, dict[str, float]] = {}
    per_date = []
    for d in sel_dates:
        row = fwd.loc[d].where(sel.loc[d]).dropna()
        u = uni_by_date.get(d)
        u = None if (u is None or pd.isna(u)) else float(u)
        mth = f"{d[:4]}-{d[4:6]}"
        b = by_month_acc.setdefault(mth, {"n": 0, "sum": 0.0, "wins": 0, "uni_sum": 0.0, "uni_n": 0})
        b["n"] += int(len(row)); b["sum"] += float(row.sum()); b["wins"] += int((row > 0).sum())
        if u is not None:
            b["uni_sum"] += u * len(row); b["uni_n"] += len(row)
        per_date.append([d, int(len(row)), round(float(row.mean()) * 100, 1),
                         round(u * 100, 1) if u is not None else None])
    by_month = {k: {"n": v["n"], "avg_pct": round(v["sum"] / v["n"] * 100, 2),
                    "win_rate_pct": round(v["wins"] / v["n"] * 100, 1),
                    "universe_avg_pct": round(v["uni_sum"] / v["uni_n"] * 100, 2) if v["uni_n"] else None}
                for k, v in by_month_acc.items() if v["n"]}

    def _num(val, scale: float = 100.0, nd: int = 1):
        return None if pd.isna(val) else round(float(val) * scale, nd)

    sig_list = []
    ri, ci = np.nonzero(sel.values)
    didx = list(panel_close.index)
    for r, cc in zip(ri.tolist(), ci.tolist()):
        d, t = didx[r], tickers[cc]
        sig_list.append({"date": d, "code": t, "close": int(round(float(c.values[r, cc]))),
                         "fwd_pct": _num(fwd.values[r, cc]), "max_pct": _num(mx.values[r, cc]),
                         "min_pct": _num(mn.values[r, cc]), "index_pct": _num(bench.values[r, cc]),
                         "spike_days": int(spike.values[r, cc]), "first_signal": bool(esel.values[r, cc]),
                         "cap_억": None if cap_panel is None else _num(cap_panel.values[r, cc], 1e-8, 0)})
    sig_list.sort(key=lambda s: s["date"])
    listed = sig_list[-int(max_signals_listed):] if max_signals_listed else []
    best = sorted(sig_list, key=lambda s: -(s["fwd_pct"] or 0))[:5]
    worst = sorted(sig_list, key=lambda s: (s["fwd_pct"] or 0))[:5]
    names = _name_map(dates[-1])
    for s in listed + best + worst:
        s["name"] = names.get(s["code"], s["code"])

    base_out.update({
        "hold_days": hold_days,
        "by_spike_days": by_spike,
        "by_month": by_month,
        "per_date_columns": ["date", "signals", "avg_fwd_pct", "universe_avg_pct"],
        "per_date": per_date,
        "best5": best, "worst5": worst,
        "signals_latest": listed,
        "caveats": ["시가총액은 최신 시총을 신호일 가격으로 환산(상장주식수 변동 미반영)",
                    "생존편향: 최신 시총 목록에 없는(상장폐지 등) 종목은 제외",
                    "신호일 종가 매수·보유기간 종가 매도 가정, 거래비용·호가 미반영",
                    "같은 종목의 연속 신호는 중복 표본 → entries_* 집계를 함께 볼 것",
                    "표본 기간이 1년 안팎이면 시장 국면 하나에 좌우될 수 있음"],
        "elapsed_sec": round(time.time() - t0, 1),
        "note": "과거 통계이며 미래 수익을 보장하지 않습니다. 매매 권유가 아닙니다.",
    })
    return base_out


def prefetch(days: int | None = None) -> None:
    """서버 시작 시(또는 백테스트 요청 시) 백그라운드로 캐시를 채웁니다."""
    days = int(days or PREFETCH_DAYS)
    if _prefetch_state["running"]:
        return

    def _job():
        _prefetch_state.update(running=True, done=0, target=days, last_error=None)
        try:
            dates = _ensure_days(days)
            _prefetch_state["done"] = len(dates)
            _prefetch_state["finished_target"] = max(_prefetch_state.get("finished_target", 0), days)
        except Exception as e:  # noqa: BLE001
            _prefetch_state["last_error"] = str(e)
        finally:
            _prefetch_state["running"] = False

    threading.Thread(target=_job, name="screener-prefetch", daemon=True).start()


def cache_status() -> dict[str, Any]:
    with _lock:
        n = len(_ohlcv_by_date)
        latest = max(_ohlcv_by_date) if _ohlcv_by_date else None
        earliest = min(_ohlcv_by_date) if _ohlcv_by_date else None
    return {"cached_days": n, "earliest": earliest, "latest": latest, "prefetch": dict(_prefetch_state),
            "flow_job": dict(_flow_job), "flow_cached_dates": len(_flow_cache)}


if os.environ.get("SCREENER_PREFETCH", "1") != "0":
    prefetch()
