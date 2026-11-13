"""
macro.py — 2단계: 매크로·신용 국면 (FRED 일별 시장 지표 → 월말 시점 고정 지표 → 시장 불안정도 점수)

무엇을 하나
  1. FRED 에서 시장 가격 지표 9개를 받아 캐시한다(하루 한 번 갱신, GitHub Release 캐시에 보관).
       신용   BAA10Y(미국 Baa 회사채 − 10년 국채 스프레드), BAMLH0A0HYM2(하이일드 OAS — FRED 가 최근 3년만 제공해 대시보드 표시용)
       금리   DGS10, DGS2, T10Y2Y(장단기 금리차)
       위험   VIXCLS
       환율   DEXKOUS(원/달러), DTWEXBGS(달러 지수)
       원자재 DCOILWTICO(WTI)
     모두 시장에서 실시간으로 정해지는 가격이라 사후 개정이 없다(개정되는 경기지표·금융여건지수는 과거 검증에 쓰면 미래 정보가 섞임).
  2. 월말 시점 고정: 한국 월말 d 의 값 = 관측일이 d '이전'인 마지막 값(미국 전일 종가까지). 20일 넘게 묵은 값은 쓰지 않는다.
  3. 지표(월말): credit_chg3(신용 스프레드 3개월 변화), vix_ratio(VIX ÷ 12개월 평균), krw_chg3(원/달러 3개월 변화율),
     usd_chg3, oil_chg3, rate_chg3(10년 금리 3개월 변화), curve(장단기 금리차 수준), hy_chg3
  4. 시장 불안정도 점수(stress) = credit_chg3 · vix_ratio · krw_chg3 의 z-점수 평균(셋 중 둘 이상).
     z-점수는 그 시점까지의 최근 60개월(최소 24개월) 평균·표준편차로만 계산 → 미래 정보 없음.
     국면: stress > 0 → '불안', 그 외 → '안정'. 정의는 결과를 보기 전에 LEDGER.md 에 고정했다.
  5. factors 엔진에 구간 분할 "macro" 를 등록 → backtest_factors(split="macro") 로 어떤 전략이든 국면별 성과를 나눠 본다.

환경 변수
  FRED_API_KEY  (선택) FRED 무료 API 키(소문자·숫자 32자). 없으면 공개 CSV(fredgraph.csv)로 받는다 — 키가 있으면 더 안정적.
  MACRO_PREFETCH=0 이면 시작할 때 자동 수집하지 않음.

한계
  - 10년(120개월)에 큰 국면 전환은 몇 번뿐이라 국면 검정은 통계적 힘이 약하다. 방향 확인과 위험 관리용 참고 지표로 본다.
  - 환율·달러 지수는 FRED 가 주 1회 갱신해 실시간 값보다 최대 1주 늦을 수 있다(과거 검증은 관측일 기준).
  - 한국 회사채 스프레드는 FRED 에 없다(한국은행 ECOS 키가 있으면 추가 가능).

결과는 과거 통계이며 매매 권유가 아닙니다.
"""
from __future__ import annotations

import calendar
import logging
import math
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import requests

import factors
from screener import KST

try:
    import cachestore
except Exception:  # noqa: BLE001
    cachestore = None

log = logging.getLogger("macro")
CACHE_NAME = "macro"
FRED_KEY = os.environ.get("FRED_API_KEY", "").strip()
FRED_API = "https://api.stlouisfed.org/fred/series/observations"
FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"
START = "2010-01-01"
REFRESH_HOURS = 20
STALE_DAYS = 20                 # 월말 시점에 이보다 오래된 관측값은 쓰지 않는다

SERIES: dict[str, dict[str, Any]] = {
    "BAA10Y": {"name": "미국 Baa 회사채 − 10년 국채 스프레드", "unit": "%p", "group": "신용"},
    "BAMLH0A0HYM2": {"name": "미국 하이일드 스프레드(OAS)", "unit": "%p", "group": "신용", "note": "FRED 가 최근 3년만 제공 → 표시용"},
    "T10Y2Y": {"name": "미국 장단기 금리차(10년−2년)", "unit": "%p", "group": "금리"},
    "DGS10": {"name": "미국 10년 국채 금리", "unit": "%", "group": "금리"},
    "DGS2": {"name": "미국 2년 국채 금리", "unit": "%", "group": "금리"},
    "VIXCLS": {"name": "VIX(미국 주식 변동성 지수)", "unit": "", "group": "위험"},
    "DEXKOUS": {"name": "원/달러 환율", "unit": "원", "group": "환율", "chg": "pct"},
    "DTWEXBGS": {"name": "달러 지수(광의)", "unit": "", "group": "환율", "chg": "pct"},
    "DCOILWTICO": {"name": "WTI 유가", "unit": "달러", "group": "원자재", "chg": "pct"},
}
STRESS_COMPONENTS = ["credit_chg3", "vix_ratio", "krw_chg3"]       # 사전 등록(LEDGER.md) — 바꾸지 않는다
Z_WINDOW, Z_MIN = 60, 24
LABEL_STRESS, LABEL_CALM = "불안", "안정"
INDICATOR_DESC = {
    "credit_chg3": "신용 스프레드(Baa−10년) 3개월 변화(%p) — 벌어지면 위험 회피",
    "vix_ratio": "VIX ÷ 최근 12개월 평균 — 1 보다 크면 평소보다 불안",
    "krw_chg3": "원/달러 3개월 변화율 — 플러스면 원화 약세(외국인 자금 이탈 압력)",
    "usd_chg3": "달러 지수 3개월 변화율",
    "oil_chg3": "WTI 3개월 변화율",
    "rate_chg3": "미국 10년 금리 3개월 변화(%p)",
    "curve": "미국 장단기 금리차(10년−2년, %p) — 음수면 역전",
    "hy_chg3": "하이일드 스프레드 3개월 변화(%p) — 최근 3년만",
}

_lock = threading.Lock()
_series: dict[str, pd.Series] = {}             # id -> 값(index='YYYYMMDD' 오름차순)
_meta: dict[str, dict[str, Any]] = {}          # id -> {"n", "first", "last", "fetched_ts", "source"}
_state: dict[str, Any] = {"running": False, "last_refresh": None, "last_error": None, "last_try": 0.0}


def _today() -> datetime:
    return datetime.now(KST)


def _redact(s: Any) -> str:
    """오류 문구에 API 키가 섞여 나가지 않게 가린다(requests 예외 메시지에는 전체 URL 이 들어갈 수 있음)."""
    t = str(s)
    return t.replace(FRED_KEY, "***") if FRED_KEY else t


# ────────────────────────────────────────────────────────────
# 수집
# ────────────────────────────────────────────────────────────
def _to_series(pairs: list[tuple[str, str]]) -> pd.Series:
    idx, vals = [], []
    for d, v in pairs:
        d = str(d).strip().replace("-", "")
        v = str(v).strip()
        if len(d) != 8 or not d.isdigit() or v in ("", ".", "NA", "nan"):
            continue
        try:
            vals.append(float(v))
            idx.append(d)
        except ValueError:
            continue
    s = pd.Series(vals, index=idx, dtype=float)
    return s[~s.index.duplicated(keep="last")].sort_index()


def _fetch_api(sid: str, start: str) -> pd.Series | None:
    r = requests.get(FRED_API, params={"series_id": sid, "api_key": FRED_KEY, "file_type": "json", "observation_start": start}, timeout=30)
    if r.status_code != 200:
        msg = ""
        try:
            msg = str((r.json() or {}).get("error_message", ""))[:160]
        except Exception:  # noqa: BLE001
            pass
        raise RuntimeError(f"FRED API HTTP {r.status_code} {msg}")
    obs = (r.json() or {}).get("observations") or []
    return _to_series([(o.get("date", ""), o.get("value", "")) for o in obs])


def _fetch_csv(sid: str, start: str) -> pd.Series | None:
    r = requests.get(FRED_CSV, params={"id": sid, "cosd": start}, timeout=30,
                     headers={"User-Agent": "Mozilla/5.0 (compatible; pykrx-mcp macro)"})
    if r.status_code != 200:
        raise RuntimeError(f"FRED CSV HTTP {r.status_code}")
    pairs = []
    for line in r.text.splitlines()[1:]:          # 첫 줄은 머리글(observation_date,<ID> 또는 DATE,<ID>)
        parts = line.split(",")
        if len(parts) >= 2:
            pairs.append((parts[0], parts[1]))
    return _to_series(pairs)


def fetch_series(sid: str, start: str = START) -> tuple[pd.Series | None, str | None]:
    """한 계열을 받는다. 키가 있으면 API, 실패하거나 키가 없으면 공개 CSV. 반환: (값, 출처)."""
    errs = []
    for src, fn in ((("api", _fetch_api),) if FRED_KEY else ()) + (("csv", _fetch_csv),):
        try:
            s = fn(sid, start)
            if s is not None and len(s) > 0:
                return s, src
            errs.append(f"{src}: 빈 응답")
        except Exception as e:  # noqa: BLE001
            errs.append(f"{src}: {type(e).__name__}: {_redact(e)}")
    _state["last_error"] = f"{sid} → " + " | ".join(errs)
    log.warning("FRED 수집 실패 %s", _state["last_error"])
    return None, None


def refresh(force: bool = False) -> dict[str, Any]:
    """모든 계열을 다시 받는다(받은 지 REFRESH_HOURS 시간이 안 됐으면 건너뜀). 새 관측값이 있으면 캐시 저장."""
    now = time.time()
    if not force and _state.get("last_refresh") and now - float(_state.get("last_refresh_ts") or 0) < REFRESH_HOURS * 3600:
        return status()
    if _state["running"]:
        return status()
    _state["running"] = True
    _state["last_try"] = now
    changed = False
    ok = 0
    try:
        for sid in SERIES:
            s, src = fetch_series(sid)
            if s is None:
                continue
            with _lock:
                old = _series.get(sid)
                if old is not None and len(s) < 0.9 * len(old):
                    _state["last_error"] = f"{sid}: 새 응답이 기존보다 짧아({len(s)} < {len(old)}) 버림"
                    continue                          # 이상하게 짧은 응답은 버린다
                if old is None or len(s) != len(old) or s.index[-1] != old.index[-1]:
                    changed = True
                ok += 1
                _series[sid] = s
                _meta[sid] = {"n": int(len(s)), "first": s.index[0], "last": s.index[-1], "last_value": float(s.iloc[-1]),
                              "fetched_ts": _today().strftime("%Y-%m-%d %H:%M"), "source": src}
            time.sleep(0.3)
        if ok:
            _state["last_refresh"] = _today().strftime("%Y-%m-%d %H:%M")
            _state["last_refresh_ts"] = time.time()
            if ok == len(SERIES):
                _state["last_error"] = None
        if changed:
            save_cache()
    finally:
        _state["running"] = False
    return status()


def refresh_async(force: bool = False) -> None:
    if _state["running"]:
        return
    if not force and _state.get("last_refresh_ts") and time.time() - float(_state["last_refresh_ts"]) < REFRESH_HOURS * 3600:
        return
    if time.time() - float(_state.get("last_try") or 0) < 600:        # 실패가 이어질 때 10분에 한 번만
        return
    threading.Thread(target=lambda: refresh(force), name="macro-refresh", daemon=True).start()


def save_cache() -> bool:
    if cachestore is None:
        return False
    with _lock:
        payload = {"version": 1, "series": dict(_series), "meta": dict(_meta), "last_refresh": _state.get("last_refresh"),
                   "last_refresh_ts": _state.get("last_refresh_ts"), "saved_at": _today().strftime("%Y-%m-%d %H:%M:%S")}
    return cachestore.save(CACHE_NAME, payload)


def load_cache() -> int:
    if cachestore is None:
        return 0
    obj = cachestore.load(CACHE_NAME)
    if not obj or not isinstance(obj, dict) or not isinstance(obj.get("series"), dict):
        return 0
    with _lock:
        for sid, s in obj["series"].items():
            if sid in SERIES and isinstance(s, pd.Series) and len(s):
                _series[sid] = s
                _meta[sid] = (obj.get("meta") or {}).get(sid, {})
    _state["last_refresh"] = obj.get("last_refresh")
    _state["last_refresh_ts"] = obj.get("last_refresh_ts")
    return len(_series)


# ────────────────────────────────────────────────────────────
# 월말 시점 고정 → 지표 → 불안정도 점수
# ────────────────────────────────────────────────────────────
def _ext_month_ends(first: str, since: int = 2010) -> list[str]:
    """first(YYYYMMDD) 이전 달들의 월말(마지막 평일) 목록 — z-점수 학습 기간용."""
    out = []
    fy, fm = int(first[:4]), int(first[4:6])
    y, m = since, 1
    while (y, m) < (fy, fm):
        d = datetime(y, m, calendar.monthrange(y, m)[1])
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        out.append(d.strftime("%Y%m%d"))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def _asof(sid: str, dates: list[str]) -> np.ndarray:
    """각 날짜 d 에 대해 관측일 < d 인 마지막 값(STALE_DAYS 넘게 묵었으면 NaN)."""
    with _lock:
        s = _series.get(sid)
    if s is None or len(s) == 0:
        return np.full(len(dates), np.nan)
    idx = s.index.to_numpy().astype(str)
    vals = s.to_numpy(dtype=float)
    pos = np.searchsorted(idx, np.array([str(d) for d in dates], dtype=str), side="left") - 1
    out = np.full(len(dates), np.nan)
    for i, (d, p) in enumerate(zip(dates, pos)):
        if p < 0:
            continue
        gap = (datetime.strptime(d, "%Y%m%d") - datetime.strptime(str(idx[p]), "%Y%m%d")).days
        if gap <= STALE_DAYS:
            out[i] = vals[p]
    return out


def monthly_panel(dates: list[str]) -> pd.DataFrame:
    """월말(dates) + 그 이전 준비 기간의 시점 고정 값. 행=월말, 열=계열."""
    dates = sorted(dates)
    allm = (_ext_month_ends(dates[0]) if dates else []) + dates
    return pd.DataFrame({sid: _asof(sid, allm) for sid in SERIES}, index=allm)


def indicators(M: pd.DataFrame) -> pd.DataFrame:
    I = pd.DataFrame(index=M.index)
    I["credit_chg3"] = M["BAA10Y"] - M["BAA10Y"].shift(3)
    I["vix_ratio"] = M["VIXCLS"] / M["VIXCLS"].rolling(12, min_periods=9).mean()
    I["krw_chg3"] = M["DEXKOUS"] / M["DEXKOUS"].shift(3) - 1.0
    I["usd_chg3"] = M["DTWEXBGS"] / M["DTWEXBGS"].shift(3) - 1.0
    oil = M["DCOILWTICO"].where(M["DCOILWTICO"] > 0)
    I["oil_chg3"] = oil / oil.shift(3) - 1.0
    I["rate_chg3"] = M["DGS10"] - M["DGS10"].shift(3)
    I["curve"] = M["T10Y2Y"]
    I["hy_chg3"] = M["BAMLH0A0HYM2"] - M["BAMLH0A0HYM2"].shift(3)
    return I


def stress_table(dates: list[str]) -> pd.DataFrame:
    """월말별 지표·z-점수·불안정도 점수·국면 라벨. 모든 값은 그 월말까지의 자료로만 계산."""
    M = monthly_panel(dates)
    I = indicators(M)
    comp = I[STRESS_COMPONENTS]
    mu = comp.rolling(Z_WINDOW, min_periods=Z_MIN).mean()
    sd = comp.rolling(Z_WINDOW, min_periods=Z_MIN).std(ddof=1)
    Z = (comp - mu) / sd.where(sd > 0)
    score = Z.mean(axis=1).where(Z.notna().sum(axis=1) >= 2)
    out = I.copy()
    for c in STRESS_COMPONENTS:
        out[f"z_{c}"] = Z[c]
    out["stress"] = score
    out["label"] = np.where(score.isna(), None, np.where(score > 0, LABEL_STRESS, LABEL_CALM))
    # 최근 60개월 안에서 지금 점수의 백분위(100 = 가장 불안)
    def _pct_last(x: np.ndarray) -> float:
        v = x[~np.isnan(x)]
        return float("nan") if (len(v) == 0 or np.isnan(x[-1])) else float((v <= x[-1]).mean()) * 100

    out["stress_pctile_5y"] = score.rolling(Z_WINDOW, min_periods=1).apply(_pct_last, raw=True).where(score.notna())
    return out


def regime_labels(dates: list[str]) -> dict[str, Any]:
    """factors.register_split 용: {월말: '안정'|'불안'|None}."""
    if not _series:
        return {d: None for d in dates}
    T = stress_table(list(dates))
    return {d: (T.at[d, "label"] if d in T.index else None) for d in dates}


# ────────────────────────────────────────────────────────────
# 보고
# ────────────────────────────────────────────────────────────
def status() -> dict[str, Any]:
    with _lock:
        meta = {sid: dict(_meta.get(sid) or {}) for sid in SERIES}
    return {"ok": bool(_series), "series": {sid: {"name": SERIES[sid]["name"], **meta[sid]} for sid in SERIES},
            "have": len([1 for sid in SERIES if sid in _series]), "target": len(SERIES),
            "key_set": bool(FRED_KEY), "key_len": len(FRED_KEY),
            "source_note": "FRED_API_KEY 가 있으면 API, 없으면 공개 CSV(fredgraph.csv)로 받습니다",
            "refresh": {"running": bool(_state["running"]), "last_refresh": _state.get("last_refresh"), "every_hours": REFRESH_HOURS,
                        "last_error": _state.get("last_error")},
            "persist": cachestore.status(CACHE_NAME) if cachestore is not None else {"enabled": False}}


def _r(x: Any, nd: int = 2) -> Any:
    try:
        return None if x is None or (isinstance(x, float) and math.isnan(x)) or pd.isna(x) else round(float(x), nd)
    except Exception:  # noqa: BLE001
        return None


def _welch(a: pd.Series, b: pd.Series) -> float | None:
    a, b = a.dropna(), b.dropna()
    if len(a) < 6 or len(b) < 6:
        return None
    se = math.sqrt(float(a.var(ddof=1)) / len(a) + float(b.var(ddof=1)) / len(b))
    return round(float(a.mean() - b.mean()) / se, 2) if se > 0 else None


def _base_rates(T: pd.DataFrame, min_cap_eok: float = 1000) -> dict[str, Any] | None:
    """편입 시점 국면별 '다음 1·3개월' 유니버스 동일가중·KOSPI 수익률(과거 통계)과 가설 M1 검정."""
    ms = factors.market_series(min_cap_억=min_cap_eok)
    if not ms:
        return None
    dates, uni, kospi = ms["dates"], ms["uni"], ms["kospi"]
    rows = []
    for i, d in enumerate(dates[:-1]):
        lab = T.at[d, "label"] if d in T.index else None
        nxt = dates[i + 1]
        if lab is None or nxt not in uni.index or pd.isna(uni[nxt]):
            continue
        u1, k1 = float(uni[nxt]), float(kospi.get(nxt, np.nan))
        u3 = k3 = np.nan
        if i + 3 < len(dates):
            m3 = dates[i + 1:i + 4]
            if all(m in uni.index and not pd.isna(uni[m]) for m in m3):
                u3 = float(np.prod([1 + uni[m] for m in m3]) - 1)
            if all(not pd.isna(kospi.get(m, np.nan)) for m in m3):
                k3 = float(np.prod([1 + kospi[m] for m in m3]) - 1)
        rows.append((d, lab, u1, k1, u3, k3))
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["formation", "label", "uni1", "kospi1", "uni3", "kospi3"]).set_index("formation")
    groups = {}
    for lab in (LABEL_CALM, LABEL_STRESS):
        g = df[df["label"] == lab]
        if g.empty:
            continue
        groups[lab] = {"months": int(len(g)),
                       "universe_next_1m_pct": _r(g["uni1"].mean() * 100), "universe_up_months_pct": _r((g["uni1"] > 0).mean() * 100, 1),
                       "kospi_next_1m_pct": _r(g["kospi1"].mean() * 100),
                       "universe_next_3m_pct": _r(g["uni3"].mean() * 100), "kospi_next_3m_pct": _r(g["kospi3"].mean() * 100)}
    out: dict[str, Any] = {"period": f"편입 {df.index[0]} ~ {df.index[-1]} ({len(df)}개월)", "by_regime": groups,
                           "universe": f"보통주 시총 {min_cap_eok:g}억↑ 동일가중(백테스트와 같은 정의)"}
    calm, strs = df[df["label"] == LABEL_CALM], df[df["label"] == LABEL_STRESS]
    if len(calm) >= 6 and len(strs) >= 6:
        half = len(df) // 2
        h1, h2 = df.iloc[:half], df.iloc[half:]

        def _diff(x: pd.DataFrame, col: str) -> float | None:
            a, b = x[x["label"] == LABEL_CALM][col], x[x["label"] == LABEL_STRESS][col]
            return None if (a.dropna().empty or b.dropna().empty) else float(a.mean() - b.mean())

        d_all, d1, d2 = _diff(df, "uni1"), _diff(h1, "uni1"), _diff(h2, "uni1")
        t = _welch(calm["uni1"], strs["uni1"])
        passed = bool(d_all is not None and d_all > 0 and t is not None and t >= 2.0 and d1 is not None and d2 is not None and d1 > 0 and d2 > 0)
        out["test_M1"] = {"hypothesis": "편입 시점이 '안정'일 때 유니버스의 다음 1개월 수익률이 '불안'일 때보다 높다",
                          "calm_minus_stress_pp": _r(d_all * 100 if d_all is not None else None), "t_welch": t,
                          "first_half_pp": _r(d1 * 100 if d1 is not None else None), "second_half_pp": _r(d2 * 100 if d2 is not None else None),
                          "kospi_calm_minus_stress_pp": _r((_diff(df, "kospi1") or np.nan) * 100),
                          "criteria": "차이 > 0, t ≥ 2, 전·후반 모두 > 0 (LEDGER.md 사전 등록)", "passed": passed}
    return out


def regime_report(months: int = 24, min_cap_eok: float = 1000) -> dict[str, Any]:
    """현재 매크로·신용 지표와 시장 불안정도 점수, 최근 추이, 국면별 과거 통계."""
    refresh_async()
    if not _series:
        st = status()
        return {"ok": False, "status": "preparing" if _state["running"] else "no_data",
                "message": "FRED 자료를 받는 중이거나 아직 받지 못했습니다. 잠시 후 다시 실행하세요(macro_status 로 확인).", "detail": st["refresh"]}
    with factors._lock:
        fdates = sorted(factors._snaps)
    today = _today().strftime("%Y%m%d")
    dates = [d for d in fdates if d <= today]
    live = today not in dates                       # 오늘이 캐시된 월말이 아니면 '월중(잠정)' 행을 하나 더한다
    T = stress_table(dates + ([today] if live else []))
    cur = T.iloc[-1]
    prev = T.iloc[-2] if len(T) >= 2 else None
    with _lock:
        latest = {sid: (s.index[-1], float(s.iloc[-1])) for sid, s in _series.items() if len(s)}
        hist = {sid: s for sid, s in _series.items()}
    dash = []
    for sid, info in SERIES.items():
        if sid not in latest:
            continue
        s = hist[sid]
        d_last, v = latest[sid]

        def _ago(days: int) -> float | None:
            cut = (datetime.strptime(d_last, "%Y%m%d") - timedelta(days=days)).strftime("%Y%m%d")
            sub = s[s.index <= cut]
            return float(sub.iloc[-1]) if len(sub) else None

        v1, v3 = _ago(30), _ago(91)
        five = s[s.index >= (datetime.strptime(d_last, "%Y%m%d") - timedelta(days=365 * 5)).strftime("%Y%m%d")]
        is_price = info.get("chg") == "pct"
        row = {"id": sid, "name": info["name"], "group": info["group"], "date": f"{d_last[:4]}-{d_last[4:6]}-{d_last[6:]}", "value": _r(v, 2),
               "unit": info["unit"], "pctile_5y": _r(float((five <= v).mean()) * 100, 0) if len(five) > 60 else None}
        if is_price:
            row["chg_1m_pct"] = _r((v / v1 - 1) * 100, 1) if v1 else None
            row["chg_3m_pct"] = _r((v / v3 - 1) * 100, 1) if v3 else None
        else:
            row["chg_1m_pp"] = _r(v - v1, 2) if v1 is not None else None
            row["chg_3m_pp"] = _r(v - v3, 2) if v3 is not None else None
        if info.get("note"):
            row["note"] = info["note"]
        dash.append(row)
    comp = {c: {"value": _r(cur[c], 4), "z": _r(cur[f"z_{c}"], 2), "desc": INDICATOR_DESC[c]} for c in STRESS_COMPONENTS}
    others = {c: {"value": _r(cur[c], 4), "desc": INDICATOR_DESC[c]} for c in ("usd_chg3", "oil_chg3", "rate_chg3", "curve", "hy_chg3")}
    tail = T.dropna(subset=["stress"]).iloc[-int(max(3, months)):]
    out: dict[str, Any] = {
        "ok": True, "as_of": f"{T.index[-1][:4]}-{T.index[-1][4:6]}-{T.index[-1][6:]}" + (" (월중, 잠정)" if live else " (월말)"),
        "regime": {"label": cur["label"], "stress": _r(cur["stress"], 2), "stress_pctile_5y": _r(cur["stress_pctile_5y"], 0),
                   "prev_month_end": None if prev is None else {"date": T.index[-2], "label": prev["label"], "stress": _r(prev["stress"], 2)},
                   "definition": "stress = z(신용 스프레드 3개월 변화) · z(VIX ÷ 12개월 평균) · z(원/달러 3개월 변화율) 의 평균. z 는 그 시점까지 최근 60개월 기준. "
                                 "stress > 0 → '불안', 그 외 '안정'. stress_pctile_5y: 최근 5년 중 지금보다 덜 불안했던 달의 비율(100 = 가장 불안)"},
        "components": comp, "other_indicators": others, "dashboard": dash,
        "history_columns": ["월말", "stress", "국면"],
        "history": [[d, _r(r["stress"], 2), r["label"]] for d, r in tail.iterrows()],
    }
    try:
        br = _base_rates(T, float(min_cap_eok))
        if br:
            out["base_rates"] = br
    except Exception as e:  # noqa: BLE001
        out["base_rates_error"] = f"{type(e).__name__}: {e}"
    st = status()
    if st["have"] < st["target"] or st["refresh"]["last_error"]:
        out["warnings"] = [f"FRED 계열 {st['have']}/{st['target']}개 수집됨" + (f" — {st['refresh']['last_error']}" if st["refresh"]["last_error"] else "")]
    out["note"] = ("과거 통계이며 미래 수익을 보장하지 않습니다. 매매 권유가 아닙니다. 국면 전환은 10년에 몇 번뿐이라 통계적 힘이 약하므로 "
                   "위험 수준을 가늠하는 참고 지표로 보세요. 전략별 국면 성과는 backtest_factors(split=\"macro\").")
    out["source"] = "FRED (Federal Reserve Bank of St. Louis)"
    return out


def _split_fn(dates: list[str]) -> dict[str, Any]:
    refresh_async()
    return regime_labels(dates)


factors.register_split("macro", "매크로·신용 국면(편입 시점의 시장 불안정도 점수: stress > 0 → 불안, 그 외 안정)", _split_fn, by="formation")


def _startup() -> None:
    try:
        load_cache()
    except Exception as e:  # noqa: BLE001
        log.warning("macro 캐시 복구 중 오류(무시): %s", e)
    if os.environ.get("MACRO_PREFETCH", "1") != "0":
        try:
            refresh()
        except Exception as e:  # noqa: BLE001
            _state["last_error"] = _redact(f"{type(e).__name__}: {e}")
            _state["running"] = False


if os.environ.get("MACRO_PREFETCH", "1") != "0":
    threading.Thread(target=_startup, name="macro-startup", daemon=True).start()
else:
    try:
        load_cache()
    except Exception as _e:  # noqa: BLE001
        log.warning("macro 캐시 복구 중 오류(무시): %s", _e)
