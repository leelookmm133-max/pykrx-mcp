"""
팩터(횡단면 순위) 백테스트·스크리너 — 학계·실전 퀀트가 쓰는 방식 그대로.

server.py 가 import 해서 MCP 도구(factor_prefetch, factor_cache_status, backtest_factors, screen_factors)로 노출합니다.
기존 screener.py(일별 캐시, 절대 조건 신호)와는 별도 캐시를 쓰며, screener 의 공용 함수(_retry, _intern, _malloc_trim …)만 빌려 씁니다.

무엇이 다른가 (왜 이 방식인가)
  - 절대 조건("60일 수익률 > 10%") 대신, 매월 말 유니버스 전종목을 지표로 순위 매겨 상위 X% 를 삽니다(횡단면 순위).
    절대 조건은 강세장에 신호가 몰려 결과가 종목선별이 아닌 시장 타이밍에 좌우되지만, 순위 방식은 매달 같은 비율을
    고르므로 같은 달의 유니버스 평균과 공정하게 비교됩니다.
  - 월간 리밸런싱, 10년(120개월) 표본, 10분위 포트폴리오, IC(순위상관), t-통계량으로 평가합니다.
  - 팩터는 학계에서 반복 검증된 것만: 가치(1/PBR, EPS/P, 배당수익률), 퀄리티(ROE=EPS/BPS), 모멘텀(12-1개월),
    단기반전(1개월), 저변동성(12개월), 52주고가 근접, 소형주, 저회전율.

데이터 (월말마다 KRX 3회 호출, 10년+준비기간 13개월 ≈ 133개 월말 ≈ 400회, 약 15분)
  1. 전종목시세(MDCSTAT01501, ALL): 종목명·시장·종가·시가총액·상장주식수·소속부   → 유니버스·시총·우선주/스팩/관리 판별
  2. 전종목등락률(MDCSTAT01602, ALL, 수정주가): 직전 월말→이번 월말 수정주가 수익률, 월간 거래량·거래대금
     → 액면분할·무상증자 등이 보정된 월간 수익률. 시작일 기준가가 직전 월말 종가와 맞는지 자동 검증(90% 이상 일치해야
       인정)하고, 어긋나면 종가 기반(상장주식수 변동 보정)으로 대체합니다. 시작일 해석(직전월말 당일/다음날)은 첫 달에 자동 판별.
  3. PER/PBR/배당수익률(ALL): PER·PBR·EPS·BPS·DIV (KRX 가 그날 공표한 값 → 미래 정보 없음)
  메모리: 월말 1개 ≈ 0.35MB, 133개 ≈ 50MB. 종목코드는 screener._intern 으로 공유해 문자열 누수를 막습니다.
  서버 시작 시 screener 의 일별 prefetch 가 끝난 뒤에 시작합니다(KRX 동시 호출 방지). FACTOR_PREFETCH=0 이면 자동 시작 안 함.

평가 기준 (학계·실전 눈높이; 결과에 통과/미달로 표시)
  ① 상위 포트폴리오 월평균 초과수익(유니버스 동일가중 대비) ≥ +0.3%p
  ② t-통계량 ≥ 2.0           ③ 월 적중률(유니버스를 이긴 달 비율) ≥ 55%
  ④ 전반·후반 기간 모두 초과수익 플러스   ⑤ 10분위 단조성(분위-수익률 스피어만 상관 ≥ 0.7, D10 > D1)
  참고: IC(순위상관) 평균 ≥ 0.02 면 실무에서 '쓸 만한 신호'로 봅니다.

결과는 과거 통계이며 매매 권유가 아닙니다.
"""
from __future__ import annotations

import calendar
import logging
import math
import os
import re
import threading
import time
import warnings
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

import screener
from screener import _intern, _malloc_trim, _retry, _rss_mb, FETCH_DELAY, KST

log = logging.getLogger("factors")

try:  # pykrx 내부 KRX 엔드포인트 (pykrx 1.2.x). 없으면 공개 API 로 폴백.
    from pykrx.website.krx.market import core as _krx_core
except Exception:  # noqa: BLE001
    _krx_core = None
try:
    from pykrx.website.krx.market import wrap as _krx_wrap
except Exception:  # noqa: BLE001
    _krx_wrap = None
from pykrx import stock

# ────────────────────────────────────────────────────────────
# 캐시
# ────────────────────────────────────────────────────────────
_lock = threading.Lock()
_snaps: dict[str, pd.DataFrame] = {}          # 월말 "YYYYMMDD" -> DataFrame(index=ticker) 열=SNAP_COLS
_snap_meta: dict[str, dict[str, Any]] = {}    # 월말 -> {"prev", "ret_source", "base_match_pct", "n", "fund_ok"}
_month_end: dict[tuple[int, int], str | None] = {}   # (연, 월) -> 월말 거래일 (None: 찾기 실패)
_holidays: set[str] = set()
_names_latest: tuple[str, dict[str, str]] | None = None     # (날짜, 티커->종목명) — 최신 월말만 보관
_live: dict[str, Any] = {}                    # 스크리너용 최근 거래일 스냅샷 {"date", "df", "names", "meta"}
_index_cache: dict[tuple[str, str, str], pd.Series | None] = {}
_chg_mode: dict[str, Any] = {"mode": None}    # 전종목등락률 시작일 해석: "next_day"(직전월말+1일) | "same_day"(직전월말)
_state: dict[str, Any] = {"running": False, "done": 0, "target": 0, "last_error": None, "finished_target": 0,
                          "started_at": None, "finished_at": None}

SNAP_COLS = ["close", "cap", "shares", "ret", "tvol_m", "tval_m", "per", "pbr", "eps", "bps", "div",
             "kosdaq", "common", "managed"]
WARMUP_MONTHS = 13          # 모멘텀 12-1·변동성 12개월 계산용 준비 기간
DEFAULT_YEARS = float(os.environ.get("FACTOR_YEARS", "10"))
BASE_MATCH_MIN = 90.0       # 전종목등락률 기준가가 직전 월말 종가와 일치해야 하는 종목 비율(%)

# 팩터 정의: 이름 -> (높을수록 좋은가, 설명)
ATOMIC_FACTORS: dict[str, tuple[bool, str]] = {
    "bm":      (True,  "가치: 장부가/시가 = 1/PBR (높을수록 저평가)"),
    "ep":      (True,  "가치: 이익수익률 = EPS/주가 (적자·미공표는 KRX 가 EPS 0 으로 제공 → 하위)"),
    "dy":      (True,  "가치: 배당수익률(%)"),
    "roe":     (True,  "퀄리티: ROE = EPS/BPS"),
    "mom":     (True,  "모멘텀: 최근 1개월을 뺀 직전 12개월 수익률(12-1)"),
    "rev":     (False, "단기반전: 최근 1개월 수익률 (낮을수록 좋음 — 급등 종목 회피)"),
    "lowvol":  (False, "저변동성: 최근 12개월 월간수익률 표준편차 (낮을수록 좋음)"),
    "high52":  (True,  "52주 고가 근접: 현재가 / 12개월 월말 최고가"),
    "size":    (False, "소형주: 시가총액 (작을수록 좋음)"),
    "lowturn": (False, "저회전율: 월간 거래대금/시가총액 (낮을수록 좋음 — 과열·로또형 회피)"),
}
GROUP_FACTORS: dict[str, list[str]] = {
    "value": ["bm", "ep", "dy"],      # 가치 복합: 세 지표 순위 평균
    "quality": ["roe"],
}
DEFAULT_FACTORS = "value,quality,lowvol,mom"
ALL_FOR_TABLE = ["value", "bm", "ep", "dy", "roe", "mom", "rev", "lowvol", "high52", "size", "lowturn"]

CRITERIA = {"excess_pp": 0.3, "t": 2.0, "hit_pct": 55.0, "mono_rho": 0.7, "ic_ref": 0.02}


# ────────────────────────────────────────────────────────────
# 날짜 유틸
# ────────────────────────────────────────────────────────────
def _today() -> datetime:
    return datetime.now(KST)


def _ymd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def _parse(ymd: str) -> datetime:
    return datetime.strptime(ymd, "%Y%m%d")


def _last_complete_month(now: datetime | None = None) -> tuple[int, int]:
    """직전 '완료된' 달 (연, 월). 이번 달은 월말이 아직 안 지났을 수 있으므로 항상 제외."""
    d = now or _today()
    y, m = d.year, d.month - 1
    if m == 0:
        y, m = y - 1, 12
    return y, m


def _month_list(n_months: int, now: datetime | None = None) -> list[tuple[int, int]]:
    """직전 완료 달부터 거슬러 n_months 개의 (연, 월), 오름차순."""
    y, m = _last_complete_month(now)
    out = []
    for _ in range(n_months):
        out.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out[::-1]


# ────────────────────────────────────────────────────────────
# KRX 호출 (문자열 숫자 파싱 포함)
# ────────────────────────────────────────────────────────────
def _num(s: pd.Series) -> pd.Series:
    """'1,234' / '-' / '' 를 float 로. 파싱 불가는 NaN."""
    if s.dtype.kind in "fiu":
        return s.astype(float)
    t = s.astype(str).str.replace(",", "", regex=False).str.strip()
    t = t.where(~t.isin(["", "-", "nan", "None"]), other="nan")
    return pd.to_numeric(t, errors="coerce")


def _sleep() -> None:
    if FETCH_DELAY > 0:
        time.sleep(FETCH_DELAY)


def _fetch_prices(ymd: str) -> pd.DataFrame:
    """전종목시세(ALL). 반환: index=ticker, 열: name, market, sect, close, cap, shares, vol, val. 휴장이면 빈 DF."""
    df = None
    if _krx_core is not None:
        try:
            df = _retry(_krx_core.전종목시세().fetch, ymd, "ALL", tries=2)
        except Exception as e:  # noqa: BLE001
            log.warning("전종목시세 실패 %s: %s", ymd, e)
            df = None
    if df is None or df.empty or "ISU_SRT_CD" not in df.columns:
        return _fetch_prices_fallback(ymd)

    def col(name: str, default=np.nan):
        return _num(df[name]).values if name in df.columns else default

    out = pd.DataFrame({
        "name": df["ISU_ABBRV"].astype(str).values if "ISU_ABBRV" in df.columns else "",
        "market": df["MKT_NM"].astype(str).values if "MKT_NM" in df.columns else "",
        "sect": df["SECT_TP_NM"].astype(str).values if "SECT_TP_NM" in df.columns else "",
        "close": col("TDD_CLSPRC"), "cap": col("MKTCAP"), "shares": col("LIST_SHRS"),
        "vol": col("ACC_TRDVOL"), "val": col("ACC_TRDVAL"),
    }, index=pd.Index(df["ISU_SRT_CD"].astype(str).str.strip().values, name="ticker"))
    out = out[~out.index.duplicated()]
    if out["close"].fillna(0).sum() == 0 or out["vol"].fillna(0).sum() == 0:
        return pd.DataFrame()      # 휴장일: KRX 가 0/'-' 으로 채운 표를 돌려줌
    return out


def _fetch_prices_fallback(ymd: str) -> pd.DataFrame:
    """공개 API 폴백: 시총(KOSPI·KOSDAQ 각 1회) + 종목명(1회)."""
    fn = getattr(stock, "get_market_cap", None) or getattr(stock, "get_market_cap_by_ticker")
    frames = []
    for mkt in ("KOSPI", "KOSDAQ"):
        try:
            df = _retry(fn, ymd, market=mkt, tries=2)
        except TypeError:
            df = _retry(fn, ymd, mkt, tries=2)
        except Exception as e:  # noqa: BLE001
            log.warning("시총 조회 실패 %s/%s: %s", ymd, mkt, e)
            df = None
        if df is None or df.empty:
            continue
        d = pd.DataFrame({"close": _num(df["종가"]), "cap": _num(df["시가총액"]), "shares": _num(df["상장주식수"]),
                          "vol": _num(df["거래량"]), "val": _num(df["거래대금"])}, index=df.index.astype(str))
        d["market"] = mkt
        d["sect"] = ""
        frames.append(d)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames)
    out = out[~out.index.duplicated()]
    if out["close"].fillna(0).sum() == 0 or out["vol"].fillna(0).sum() == 0:
        return pd.DataFrame()
    names = screener._name_map(ymd)
    out["name"] = [names.get(t, "") for t in out.index]
    out.index.name = "ticker"
    return out


def _fetch_change(start: str, end: str) -> pd.DataFrame:
    """전종목등락률(ALL, 수정주가). 반환: index=ticker, 열: base(시작일 기준가), close_e, ret(소수), vol, val. 실패 시 빈 DF."""
    df = None
    try:
        if _krx_wrap is not None and hasattr(_krx_wrap, "get_market_price_change_by_ticker"):
            df = _retry(_krx_wrap.get_market_price_change_by_ticker, start, end, "ALL", True, tries=2)
        else:
            df = _retry(stock.get_market_price_change, start, end, market="ALL", adjusted=True, tries=2)
    except Exception as e:  # noqa: BLE001
        log.warning("전종목등락률 실패 %s~%s: %s", start, end, e)
        df = None
    if df is None or df.empty or "시가" not in df.columns or "종가" not in df.columns:
        return pd.DataFrame()
    base = _num(df["시가"])            # pykrx 는 BAS_PRC(시작일 기준가)를 '시가'로 이름 붙임
    close = _num(df["종가"])
    ret = close / base.where(base > 0) - 1.0
    if "등락률" in df.columns:
        r2 = _num(df["등락률"]) / 100.0
        ret = r2.where(r2.notna() & (base > 0), ret)
    out = pd.DataFrame({"base": base.values, "close_e": close.values, "ret": ret.values,
                        "vol": _num(df["거래량"]).values if "거래량" in df.columns else np.nan,
                        "val": _num(df["거래대금"]).values if "거래대금" in df.columns else np.nan},
                       index=pd.Index(df.index.astype(str).str.strip(), name="ticker"))
    return out[~out.index.duplicated()]


def _fetch_fund(ymd: str) -> pd.DataFrame:
    fn = getattr(stock, "get_market_fundamental", None) or getattr(stock, "get_market_fundamental_by_ticker", None)
    df = None
    if fn is not None:
        try:
            df = _retry(fn, ymd, market="ALL", tries=2)
        except TypeError:
            df = _retry(fn, ymd, "ALL", tries=2)
        except Exception as e:  # noqa: BLE001
            log.warning("펀더멘털 조회 실패 %s: %s", ymd, e)
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame({k.lower(): _num(df[k]).values for k in ("PER", "PBR", "EPS", "BPS", "DIV") if k in df.columns},
                       index=pd.Index(df.index.astype(str).str.strip(), name="ticker"))
    return out[~out.index.duplicated()]


def _index_series(code: str, start: str, end: str) -> pd.Series | None:
    """지수 종가(일별, 인덱스 'YYYYMMDD'). 긴 기간은 1년 단위로 나눠 받는다."""
    key = (code, start, end)
    if key in _index_cache:
        return _index_cache[key]
    parts = []
    s, e = _parse(start), _parse(end)
    cur = s
    try:
        while cur <= e:
            nxt = min(datetime(cur.year, 12, 31), e)
            df = _retry(stock.get_index_ohlcv, _ymd(cur), _ymd(nxt), code, tries=2)
            if df is not None and not df.empty and "종가" in df.columns:
                ser = df["종가"].astype(float)
                ser.index = [d.strftime("%Y%m%d") for d in pd.to_datetime(ser.index)]
                parts.append(ser)
            _sleep()
            cur = nxt + timedelta(days=1)
    except Exception as ex:  # noqa: BLE001
        log.warning("지수 조회 실패(%s): %s", code, ex)
    out = pd.concat(parts).sort_index() if parts else None
    if out is not None:
        out = out[~out.index.duplicated()]
    _index_cache[key] = out
    return out


# ────────────────────────────────────────────────────────────
# 스냅샷 만들기
# ────────────────────────────────────────────────────────────
def _is_common(ticker: str, name: str, market: str) -> bool:
    """보통주만: 국내 종목코드 끝자리 0 = 보통주(우선주는 5·7·9 등). 스팩·코넥스 제외."""
    mk = market.upper()
    return ticker.endswith("0") and ("스팩" not in name) and ("KOSPI" in mk or "KOSDAQ" in mk)


def _close_based_ret(cur_close: pd.Series, cur_shares: pd.Series, prev: pd.DataFrame) -> pd.Series:
    """종가 기반 월간 수익률(미보정). 상장주식수가 1.4배 넘게 변하면(분할·무상증자·감자) 시총 비율과 가격 비율 중
    변동이 작은 쪽을 쓴다(분할이면 시총 비율이, 유상증자면 대개 가격 비율이 실제 수익률에 가깝다)."""
    pc = prev["close"].reindex(cur_close.index)
    ps = prev["shares"].reindex(cur_close.index)
    pr = cur_close / pc.where(pc > 0)
    sr = cur_shares / ps.where(ps > 0)
    cr = pr * sr
    big = sr.notna() & ((sr > 1.4) | (sr < 1 / 1.4))
    with np.errstate(invalid="ignore", divide="ignore"):
        use_cap = big & (np.log(cr.where(cr > 0)).abs() < np.log(pr.where(pr > 0)).abs())
    return pr.where(~use_cap, cr) - 1.0


def _base_match_pct(chg: pd.DataFrame, prev: pd.DataFrame | None, idx: pd.Index) -> float | None:
    """전종목등락률의 시작일 기준가가 직전 월말 종가와 일치(±0.5%)하는 종목 비율(%)."""
    if chg is None or chg.empty or prev is None or prev.empty:
        return None
    c = chg.reindex(idx)
    pc = prev["close"].reindex(idx)
    both = c["base"].notna() & pc.notna() & (pc > 0) & (c["base"] > 0)
    n = int(both.sum())
    if n == 0:
        return None
    match = ((c["base"] / pc - 1.0).abs() < 0.005) & both
    return round(float(match.sum()) / n * 100, 1)


def _build_snapshot(ymd: str, prices: pd.DataFrame, chg: pd.DataFrame | None, fund: pd.DataFrame | None,
                    prev: pd.DataFrame | None) -> tuple[pd.DataFrame, dict[str, Any]]:
    """월말 하나의 표(SNAP_COLS)와 메타(수익률 출처 등)."""
    idx = prices.index
    snap = pd.DataFrame(index=idx)
    snap["close"] = prices["close"].astype(float)
    snap["cap"] = prices["cap"].astype(float)
    snap["shares"] = prices["shares"].astype(float)
    names = prices["name"].astype(str)
    markets = prices["market"].astype(str)
    snap["kosdaq"] = markets.str.upper().str.contains("KOSDAQ").astype(float)
    snap["common"] = np.array([1.0 if _is_common(t, n, m) else 0.0 for t, n, m in zip(idx, names, markets)])
    snap["managed"] = prices["sect"].astype(str).str.contains("관리|환기").astype(float)

    meta: dict[str, Any] = {"ret_source": "none", "base_match_pct": None}
    has_prev = prev is not None and len(prev) > 0
    ret_close = _close_based_ret(snap["close"], snap["shares"], prev) if has_prev else pd.Series(np.nan, index=idx)
    ret = ret_close.copy()
    if has_prev:
        meta["ret_source"] = "close"
    if chg is not None and len(chg):
        c = chg.reindex(idx)
        krx_ret = c["ret"]
        if has_prev:
            pct = _base_match_pct(chg, prev, idx)
            meta["base_match_pct"] = pct
            # 기준가의 90% 이상이 직전 월말 종가와 일치해야 '직전월말→이번월말' 구간으로 인정 (나머지 불일치는 분할 등 보정분).
            if pct is not None and pct >= BASE_MATCH_MIN:
                ret = krx_ret.where(krx_ret.notna(), ret_close)
                meta["ret_source"] = "krx_adj"
        else:
            ret = krx_ret                       # 첫 스냅샷: 검증 불가, KRX 값 그대로
            meta["ret_source"] = "krx_adj_unverified"
        snap["tvol_m"] = c["vol"].astype(float)
        snap["tval_m"] = c["val"].astype(float)
    else:
        snap["tvol_m"] = np.nan
        snap["tval_m"] = np.nan
    snap["ret"] = ret.astype(float)
    for k in ("per", "pbr", "eps", "bps", "div"):
        snap[k] = fund[k].reindex(idx).astype(float) if (fund is not None and len(fund) and k in fund.columns) else np.nan
    snap = snap[SNAP_COLS]
    # 캐시에 오래 남는 객체는 새로 만든 배열 + 공유 문자열 인덱스만 (screener 의 메모리 누수 대책과 동일)
    data = {c: np.ascontiguousarray(snap[c].to_numpy(dtype=float)) for c in snap.columns}
    out = pd.DataFrame(data, index=pd.Index([_intern(t) for t in snap.index], dtype=object, name="ticker"))
    meta["n"] = int(len(out))
    meta["n_common"] = int(out["common"].sum())
    meta["fund_ok"] = bool(out["pbr"].notna().any())
    return out, meta


def _find_month_end(y: int, m: int) -> tuple[str | None, pd.DataFrame | None]:
    """(연, 월)의 마지막 거래일과 그날의 전종목시세. 월말부터 거슬러 최대 10일."""
    last = calendar.monthrange(y, m)[1]
    d = datetime(y, m, last)
    for _ in range(10):
        ymd = _ymd(d)
        if d.weekday() >= 5 or ymd in _holidays:
            d -= timedelta(days=1)
            continue
        df = _fetch_prices(ymd)
        _sleep()
        if df is None or df.empty:
            _holidays.add(ymd)
            d -= timedelta(days=1)
            continue
        _month_end[(y, m)] = ymd
        return ymd, df
    _month_end[(y, m)] = None
    return None, None


def _fetch_change_verified(prev_ymd: str | None, ymd: str, prev_df: pd.DataFrame | None,
                           idx: pd.Index) -> tuple[pd.DataFrame, float | None]:
    """직전 월말→이번 월말 등락률. 시작일 해석 모드를 첫 달에 자동 판별(기준가 일치율이 높은 쪽)."""
    if prev_ymd is None:
        start = _ymd(_parse(ymd).replace(day=1))
        chg = _fetch_change(start, ymd)
        _sleep()
        return chg, None
    modes = [_chg_mode["mode"]] if _chg_mode["mode"] else ["next_day", "same_day"]
    best: tuple[str, pd.DataFrame, float | None] | None = None
    for mode in modes:
        start = _ymd(_parse(prev_ymd) + timedelta(days=1)) if mode == "next_day" else prev_ymd
        chg = _fetch_change(start, ymd)
        _sleep()
        pct = _base_match_pct(chg, prev_df, idx)
        if best is None or (pct if pct is not None else -1) > (best[2] if best[2] is not None else -1):
            best = (mode, chg, pct)
        if pct is not None and pct >= BASE_MATCH_MIN:
            break
    assert best is not None
    if _chg_mode["mode"] is None and best[2] is not None and best[2] >= BASE_MATCH_MIN:
        _chg_mode["mode"] = best[0]
        log.info("전종목등락률 시작일 해석: %s (기준가 일치 %.1f%%)", best[0], best[2])
    return best[1], best[2]


def ensure_months(n_months: int) -> list[str]:
    """최근 n_months 개 월말 스냅샷을 (없으면 받아서) 캐시에 채우고, 날짜 목록(오름차순)을 돌려준다."""
    global _names_latest
    months = _month_list(n_months)
    dates: list[str] = []
    prev_ymd: str | None = None
    prev_df: pd.DataFrame | None = None
    for (y, m) in months:
        known = _month_end.get((y, m), "?")
        if known is None:
            continue
        if known != "?" and known in _snaps:
            dates.append(known)
            prev_ymd = known
            with _lock:
                prev_df = _snaps[known]
            _state["done"] = len(dates)
            continue
        ymd, prices = _find_month_end(y, m)
        if ymd is None or prices is None:
            _state["last_error"] = f"{y}-{m:02d} 월말 거래일을 찾지 못함"
            continue
        chg, _pct = _fetch_change_verified(prev_ymd, ymd, prev_df, prices.index)
        fund = _fetch_fund(ymd)
        _sleep()
        snap, meta = _build_snapshot(ymd, prices, chg, fund, prev_df)
        meta["prev"] = prev_ymd
        names = {_intern(t): str(n) for t, n in zip(prices.index, prices["name"].astype(str))}
        with _lock:
            _snaps[ymd] = snap
            _snap_meta[ymd] = meta
            if _names_latest is None or ymd > _names_latest[0]:
                _names_latest = (ymd, names)
        del prices, chg, fund
        _malloc_trim()
        dates.append(ymd)
        prev_ymd, prev_df = ymd, snap
        _state["done"] = len(dates)
    dates.sort()
    return dates


def _wait_for_screener_prefetch(max_wait_sec: int = 1800) -> None:
    """screener 의 일별 prefetch 가 돌고 있으면 끝날 때까지(최대 30분) 기다린다 — KRX 동시 호출 방지."""
    time.sleep(10)          # screener 의 prefetch 스레드가 running 표시를 올릴 시간을 준다(시작 직후 경쟁 방지)
    t0 = time.time()
    while time.time() - t0 < max_wait_sec:
        st = getattr(screener, "_prefetch_state", {})
        if not st.get("running"):
            return
        time.sleep(5)


def prefetch(years: float | None = None) -> dict[str, Any]:
    """백그라운드로 월말 스냅샷을 채운다(이미 돌고 있으면 그대로)."""
    years = float(years or DEFAULT_YEARS)
    n = int(round(years * 12)) + WARMUP_MONTHS
    if _state["running"]:
        return cache_status()

    def _job():
        _state.update(running=True, done=0, target=n, last_error=None, started_at=_today().strftime("%Y-%m-%d %H:%M"))
        try:
            _wait_for_screener_prefetch()
            ensure_months(n)
            _state["finished_target"] = max(_state.get("finished_target", 0), n)
        except Exception as e:  # noqa: BLE001
            _state["last_error"] = str(e)
            log.exception("factor prefetch 실패")
        finally:
            _state["running"] = False
            _state["finished_at"] = _today().strftime("%Y-%m-%d %H:%M")

    threading.Thread(target=_job, name="factor-prefetch", daemon=True).start()
    return cache_status()


def cache_status() -> dict[str, Any]:
    with _lock:
        dates = sorted(_snaps)
        srcs = [m.get("ret_source") for m in _snap_meta.values()]
        matches = [m.get("base_match_pct") for m in _snap_meta.values() if m.get("base_match_pct") is not None]
        fund_ok = sum(1 for m in _snap_meta.values() if m.get("fund_ok"))
    st = dict(_state)
    out = {"cached_month_ends": len(dates), "earliest": dates[0] if dates else None, "latest": dates[-1] if dates else None,
           "prefetch": st, "process_rss_mb": _rss_mb(),
           "return_source": {"krx_adjusted": srcs.count("krx_adj"), "close_fallback": srcs.count("close"),
                             "unverified_first": srcs.count("krx_adj_unverified"), "none": srcs.count("none"),
                             "base_match_pct_avg": round(float(np.mean(matches)), 1) if matches else None,
                             "start_date_mode": _chg_mode["mode"]},
           "fundamental_months": fund_ok,
           "note": "backtest_factors 는 (years×12 + 13)개 월말이 있어야 전 기간을 계산합니다. 부족하면 있는 만큼 계산합니다."}
    if st["running"] and st["target"]:
        out["progress"] = f"{st['done']}/{st['target']} 월말 (월말당 KRX 3회, 약 5~7초)"
    return out


# ────────────────────────────────────────────────────────────
# 패널 → 팩터 → 순위
# ────────────────────────────────────────────────────────────
def _panels(dates: list[str], extra: pd.DataFrame | None = None, extra_date: str | None = None) -> dict[str, pd.DataFrame]:
    """월말 스냅샷들을 행=날짜, 열=티커 패널로. extra 는 스크리너용 최근 거래일 스냅샷(마지막 행으로 추가)."""
    with _lock:
        frames = {d: _snaps[d] for d in dates if d in _snaps}
    if extra is not None and extra_date is not None:
        frames[extra_date] = extra
    keys = sorted(frames)
    tickers = sorted(set().union(*[set(f.index) for f in frames.values()])) if frames else []
    pos = {t: i for i, t in enumerate(tickers)}
    out: dict[str, pd.DataFrame] = {}
    for col in SNAP_COLS:
        mat = np.full((len(keys), len(tickers)), np.nan)
        for r, d in enumerate(keys):
            f = frames[d]
            mat[r, [pos[t] for t in f.index]] = f[col].to_numpy(dtype=float)
        out[col] = pd.DataFrame(mat, index=keys, columns=tickers)
    return out


def _parse_factors(spec: Any) -> list[str]:
    if spec is None or str(spec).strip() == "":
        spec = DEFAULT_FACTORS
    items = [s.strip().lower() for s in re.split(r"[,\s+/]+", str(spec)) if s.strip()]
    out: list[str] = []
    for it in items:
        if it in ATOMIC_FACTORS or it in GROUP_FACTORS:
            if it not in out:
                out.append(it)
        else:
            raise ValueError(f"알 수 없는 팩터 '{it}'. 가능: {', '.join(list(GROUP_FACTORS) + list(ATOMIC_FACTORS))}")
    return out


def raw_factors(P: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """원시 팩터 값 패널(행=월말, 열=티커). 방향은 ATOMIC_FACTORS 참고."""
    close, cap, ret = P["close"], P["cap"], P["ret"]
    pbr, eps, bps, div = P["pbr"], P["eps"], P["bps"], P["div"]
    logret = np.log1p(ret.where(ret > -0.999))
    F: dict[str, pd.DataFrame] = {}
    F["bm"] = 1.0 / pbr.where(pbr > 0)
    F["ep"] = eps / close.where(close > 0)
    F["dy"] = div.where(div >= 0)
    F["roe"] = eps / bps.where(bps > 0)
    F["mom"] = np.exp(logret.shift(1).rolling(11, min_periods=10).sum()) - 1.0
    F["rev"] = ret
    F["lowvol"] = ret.rolling(12, min_periods=9).std()
    adj = np.exp(logret.fillna(0.0).cumsum()).where(close.notna())
    F["high52"] = adj / adj.rolling(12, min_periods=9).max()
    F["size"] = cap
    F["lowturn"] = P["tval_m"] / cap.where(cap > 0)
    return F


def _pct_rank(df: pd.DataFrame, higher_better: bool, mask: pd.DataFrame) -> pd.DataFrame:
    """유니버스(mask) 안에서 행별 백분위 순위 (0,1], 1 이 가장 좋음."""
    x = df.where(mask)
    if not higher_better:
        x = -x
    return x.rank(axis=1, pct=True, method="average")


def _nanmean_stack(frames: list[pd.DataFrame], min_frac: float = 0.67) -> pd.DataFrame:
    stacked = np.stack([f.to_numpy() for f in frames])
    cnt = np.sum(~np.isnan(stacked), axis=0)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)      # 전부 NaN 인 칸(준비기간 등)의 'Mean of empty slice'
        avg = np.nanmean(stacked, axis=0)
    avg[cnt < max(1, math.ceil(len(frames) * min_frac))] = np.nan
    return pd.DataFrame(avg, index=frames[0].index, columns=frames[0].columns)


def _decile_of(score: pd.DataFrame, mask: pd.DataFrame) -> np.ndarray:
    """점수(동점 허용)를 유니버스 안에서 10분위(1~10, 10 이 최상위)로. 동점은 열 순서로 나눠 분위 크기를 맞춘다
    (적자 EPS=0 같은 큰 동점 묶음이 한 분위를 비우는 것을 막는다). 유니버스 밖은 0."""
    r = score.where(mask).rank(axis=1, pct=True, method="first")
    dec = np.clip(np.nan_to_num(np.ceil(r.to_numpy() * 10), nan=0), 0, 10)
    return dec


def factor_ranks(F: dict[str, pd.DataFrame], names: list[str], mask: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """요청한 팩터(그룹 포함)의 백분위 순위 패널. 그룹은 구성 지표 순위의 평균을 다시 백분위로."""
    R: dict[str, pd.DataFrame] = {}
    atomic_cache: dict[str, pd.DataFrame] = {}

    def _atomic(a: str) -> pd.DataFrame:
        if a not in atomic_cache:
            atomic_cache[a] = _pct_rank(F[a], ATOMIC_FACTORS[a][0], mask)
        return atomic_cache[a]

    for n in names:
        if n in GROUP_FACTORS:
            parts = [_atomic(a) for a in GROUP_FACTORS[n]]
            R[n] = _nanmean_stack(parts).where(mask).rank(axis=1, pct=True, method="average") if len(parts) > 1 else parts[0]
        else:
            R[n] = _atomic(n)
    return R


def composite_rank(R: dict[str, pd.DataFrame], names: list[str], mask: pd.DataFrame) -> pd.DataFrame:
    if len(names) == 1:
        return R[names[0]]
    return _nanmean_stack([R[n] for n in names]).where(mask).rank(axis=1, pct=True, method="average")


# ────────────────────────────────────────────────────────────
# 통계
# ────────────────────────────────────────────────────────────
def _tstat(x: pd.Series) -> float | None:
    x = x.dropna()
    if len(x) < 3 or float(x.std(ddof=1)) == 0:
        return None
    return round(float(x.mean() / x.std(ddof=1) * math.sqrt(len(x))), 2)


def _cagr(r: pd.Series) -> float | None:
    r = r.dropna()
    if len(r) == 0:
        return None
    return round((float(np.prod(1 + r.to_numpy())) ** (12 / len(r)) - 1) * 100, 1)


def _series_stats(port: pd.Series, uni: pd.Series, idx: pd.Series | None) -> dict[str, Any]:
    """월간 수익률 시리즈(소수) → 요약. port/uni/idx 는 같은 인덱스(수익률 월)."""
    uni = uni.reindex(port.index)
    ok = port.notna() & uni.notna()
    p, u = port[ok], uni[ok]
    n = int(len(p))
    if n == 0:
        return {"months": 0}
    ex = p - u
    half = n // 2
    cum = (1 + p).cumprod()
    out = {
        "months": n,
        "avg_month_pct": round(float(p.mean()) * 100, 2),
        "universe_avg_month_pct": round(float(u.mean()) * 100, 2),
        "excess_vs_universe_pp": round(float(ex.mean()) * 100, 2),
        "excess_t_stat": _tstat(ex),
        "hit_rate_vs_universe_pct": round(float((ex > 0).mean()) * 100, 1),
        "excess_first_half_pp": round(float(ex.iloc[:half].mean()) * 100, 2) if half else None,
        "excess_second_half_pp": round(float(ex.iloc[half:].mean()) * 100, 2) if half else None,
        "cagr_pct": _cagr(p),
        "universe_cagr_pct": _cagr(u),
        "ann_vol_pct": round(float(p.std(ddof=1)) * math.sqrt(12) * 100, 1) if n > 1 else None,
        "max_drawdown_pct": round(float((cum / cum.cummax() - 1).min()) * 100, 1),
    }
    if idx is not None:
        i = idx.reindex(p.index)
        oki = i.notna()
        if bool(oki.any()):
            exi = p[oki] - i[oki]
            out["index_avg_month_pct"] = round(float(i[oki].mean()) * 100, 2)
            out["excess_vs_index_pp"] = round(float(exi.mean()) * 100, 2)
            out["excess_vs_index_t_stat"] = _tstat(exi)
            out["hit_rate_vs_index_pct"] = round(float((exi > 0).mean()) * 100, 1)
            out["index_cagr_pct"] = _cagr(i[oki])
    return out


def _criteria(st: dict[str, Any], decile_means: list[float | None] | None) -> dict[str, Any]:
    """학계·실전 기준 5개 판정."""
    rows = []
    ex, t, hit = st.get("excess_vs_universe_pp"), st.get("excess_t_stat"), st.get("hit_rate_vs_universe_pct")
    h1, h2 = st.get("excess_first_half_pp"), st.get("excess_second_half_pp")
    rows.append({"기준": "① 월평균 초과수익(유니버스 대비)", "값": None if ex is None else f"{ex:+.2f}%p",
                 "기준값": f"+{CRITERIA['excess_pp']}%p 이상", "통과": bool(ex is not None and ex >= CRITERIA["excess_pp"])})
    rows.append({"기준": "② t-통계량", "값": t, "기준값": f"{CRITERIA['t']} 이상", "통과": bool(t is not None and t >= CRITERIA["t"])})
    rows.append({"기준": "③ 월 적중률(유니버스를 이긴 달)", "값": None if hit is None else f"{hit}%",
                 "기준값": f"{CRITERIA['hit_pct']:.0f}% 이상", "통과": bool(hit is not None and hit >= CRITERIA["hit_pct"])})
    rows.append({"기준": "④ 전반·후반 모두 플러스", "값": None if h1 is None else f"전반 {h1:+.2f} / 후반 {h2:+.2f}%p",
                 "기준값": "둘 다 > 0", "통과": bool(h1 is not None and h2 is not None and h1 > 0 and h2 > 0)})
    rho, mono_ok = None, False
    if decile_means and len(decile_means) == 10 and all(v is not None for v in decile_means):
        d = pd.Series(decile_means, dtype=float)
        rho = round(float(d.rank().corr(pd.Series(range(1, 11), dtype=float))), 2)
        mono_ok = bool(rho >= CRITERIA["mono_rho"] and decile_means[-1] > decile_means[0])
    rows.append({"기준": "⑤ 10분위 단조성(스피어만 ρ, D10>D1)", "값": rho,
                 "기준값": f"ρ ≥ {CRITERIA['mono_rho']} 그리고 D10 > D1", "통과": mono_ok})
    passed = sum(1 for r in rows if r["통과"])
    return {"rows": rows, "passed": f"{passed}/5",
            "verdict": "통과(5/5)" if passed == 5 else ("부분 통과" if passed >= 3 else "미달")}


# ────────────────────────────────────────────────────────────
# 백테스트
# ────────────────────────────────────────────────────────────
def _index_monthly(dates: list[str]) -> tuple[pd.Series, pd.Series]:
    """월말 날짜 목록에 맞춘 KOSPI·KOSDAQ 월간 수익률(해당 월말까지 한 달). 실패 시 NaN."""
    out = []
    for code in ("1001", "2001"):
        s = _index_series(code, dates[0], dates[-1])
        if s is None or len(s) == 0:
            out.append(pd.Series(np.nan, index=dates))
            continue
        vals = []
        for d in dates:
            sub = s[s.index <= d]
            vals.append(float(sub.iloc[-1]) if len(sub) else np.nan)
        lv = pd.Series(vals, index=dates, dtype=float)
        out.append(lv / lv.shift(1) - 1.0)
    return out[0], out[1]


def _universe_mask(P: dict[str, pd.DataFrame], min_cap: float, exclude_managed: bool, min_price: float) -> pd.DataFrame:
    close, cap = P["close"], P["cap"]
    m = (P["common"] > 0) & close.notna() & (close >= min_price) & cap.notna() & (cap >= min_cap)
    if exclude_managed:
        m &= ~(P["managed"] > 0)
    return m


def _to_return_month(s: pd.Series, dates: list[str], j: int) -> pd.Series:
    """형성월(인덱스)의 값을 j 개월 뒤 '수익률 월' 인덱스로 옮긴다."""
    vals = s.reindex(dates).to_numpy(dtype=float)
    out_idx, out_vals = [], []
    for i in range(len(dates)):
        if i + j < len(dates):
            out_idx.append(dates[i + j]); out_vals.append(vals[i])
    return pd.Series(out_vals, index=out_idx, dtype=float)


def _ic_series(score: pd.DataFrame, fwd: pd.DataFrame, mask: pd.DataFrame, min_n: int = 30) -> pd.Series:
    """월별 IC = 점수 순위와 다음 달 수익률 순위의 상관(스피어만)."""
    s, f = score.where(mask), fwd.where(mask)
    out = {}
    for d in s.index:
        a, b = s.loc[d], f.loc[d]
        ok = a.notna() & b.notna()
        if int(ok.sum()) < min_n:
            continue
        out[d] = float(a[ok].rank().corr(b[ok].rank()))
    return pd.Series(out, dtype=float)


def _ic_summary(ic: pd.Series) -> dict[str, Any]:
    if len(ic) == 0:
        return {"months": 0}
    sd = float(ic.std(ddof=1)) if len(ic) > 1 else None
    return {"months": int(len(ic)), "mean": round(float(ic.mean()), 4), "std": round(sd, 4) if sd is not None else None,
            "icir": round(float(ic.mean()) / sd, 2) if sd else None, "t": _tstat(ic),
            "positive_rate_pct": round(float((ic > 0).mean()) * 100, 1),
            "reference": f"실무에서 IC 평균 ≥ {CRITERIA['ic_ref']} 이면 쓸 만한 신호, ICIR ≥ 0.5 면 안정적"}


def _decile_table(score: pd.DataFrame, fwd: pd.DataFrame, uni_ret: pd.Series, mask: pd.DataFrame) -> dict[str, Any]:
    dec = _decile_of(score, mask)
    rows, means = [], []
    for d in range(1, 11):
        m = pd.DataFrame(dec == d, index=score.index, columns=score.columns) & mask
        pr = fwd.where(m).mean(axis=1)
        ok = pr.notna() & uni_ret.notna()
        if int(ok.sum()) == 0:
            rows.append({"decile": d, "avg_month_pct": None}); means.append(None); continue
        ex = (pr - uni_ret)[ok]
        avg = round(float(pr[ok].mean()) * 100, 2)
        rows.append({"decile": d, "avg_month_pct": avg, "excess_pp": round(float(ex.mean()) * 100, 2),
                     "t": _tstat(ex), "avg_n": int(round(float(m.sum(axis=1)[ok].mean())))})
        means.append(avg)
    spread = round(means[-1] - means[0], 2) if (means[0] is not None and means[-1] is not None) else None
    return {"rows": rows, "d10_minus_d1_pp": spread, "note": "D10 = 점수 최상위 10%. 월간 평균수익률(%)과 유니버스 대비 초과(%p), 월간 리밸런싱"}


def _single_factor_table(F: dict[str, pd.DataFrame], fwd: pd.DataFrame, uni_ret: pd.Series, mask: pd.DataFrame,
                         dates: list[str]) -> dict[str, Any]:
    """각 팩터를 단독으로 썼을 때: 상위 10%(D10) 초과수익·t·적중률·전후반, D10-D1, IC. '한국시장에서 무엇이 먹히나' 표."""
    table: dict[str, Any] = {}
    R = factor_ranks(F, ALL_FOR_TABLE, mask)
    uni_series = _to_return_month(uni_ret, dates, 1)
    for n in ALL_FOR_TABLE:
        r = R[n]
        dec = _decile_of(r, mask)
        d10 = pd.DataFrame(dec == 10, index=r.index, columns=r.columns)
        d1 = pd.DataFrame(dec == 1, index=r.index, columns=r.columns)
        p10 = _to_return_month(fwd.where(d10).mean(axis=1), dates, 1)
        p1 = _to_return_month(fwd.where(d1).mean(axis=1), dates, 1)
        st = _series_stats(p10, uni_series, None)
        ic = _ic_summary(_ic_series(r, fwd, mask))
        ok = p10.notna() & p1.notna()
        spread = round(float((p10[ok] - p1[ok]).mean()) * 100, 2) if int(ok.sum()) else None
        passed = sum([
            bool(st.get("excess_vs_universe_pp") is not None and st["excess_vs_universe_pp"] >= CRITERIA["excess_pp"]),
            bool(st.get("excess_t_stat") is not None and st["excess_t_stat"] >= CRITERIA["t"]),
            bool(st.get("hit_rate_vs_universe_pct") is not None and st["hit_rate_vs_universe_pct"] >= CRITERIA["hit_pct"]),
            bool(st.get("excess_first_half_pp") is not None and st.get("excess_second_half_pp") is not None
                 and st["excess_first_half_pp"] > 0 and st["excess_second_half_pp"] > 0)])
        desc = "가치 복합(1/PBR·EPS/P·배당 순위 평균)" if n == "value" else ATOMIC_FACTORS[n][1]
        table[n] = {"설명": desc, "months": st.get("months"),
                    "d10_excess_pp": st.get("excess_vs_universe_pp"), "d10_t": st.get("excess_t_stat"),
                    "d10_hit_pct": st.get("hit_rate_vs_universe_pct"),
                    "d10_first_half_pp": st.get("excess_first_half_pp"), "d10_second_half_pp": st.get("excess_second_half_pp"),
                    "d10_minus_d1_pp": spread, "ic_mean": ic.get("mean"), "icir": ic.get("icir"), "ic_t": ic.get("t"),
                    "passed_of_4": passed}
    return {"note": "각 팩터 단독 상위 10% 포트폴리오(월간 리밸런싱). 기준 ①~④ 통과 수(passed_of_4)와 IC 로 "
                    "'한국시장에서 실제로 먹히는 팩터'를 가려낸 뒤 복합 구성을 정할 것",
            "table": table}


def _data_quality(dates: list[str]) -> dict[str, Any]:
    with _lock:
        metas = [_snap_meta.get(d, {}) for d in dates]
    src = [m.get("ret_source") for m in metas]
    bm = [m.get("base_match_pct") for m in metas if m.get("base_match_pct") is not None]
    return {"months_krx_adjusted_return": src.count("krx_adj"), "months_close_fallback": src.count("close"),
            "base_price_match_pct_avg": round(float(np.mean(bm)), 1) if bm else None,
            "start_date_mode": _chg_mode["mode"],
            "months_with_fundamentals": sum(1 for m in metas if m.get("fund_ok")),
            "avg_listed": int(round(float(np.mean([m.get("n", 0) for m in metas])))) if metas else None}


def run_backtest(factors: Any = DEFAULT_FACTORS, min_cap_억: float = 1000, years: float = 10, top_pct: float = 10,
                 top_n: int = 0, hold_months: int = 1, exclude_managed: bool = True, min_price: float = 1000,
                 include_single_factor_table: bool = True, max_months_listed: int = 130) -> dict[str, Any]:
    t0 = time.time()
    try:
        names = _parse_factors(factors)
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    need = int(round(float(years) * 12)) + WARMUP_MONTHS
    with _lock:
        have = sorted(_snaps)
    if len(have) < need:
        st = dict(_state)
        if st["running"]:
            remain = max(1, round((st["target"] - st["done"]) * 6 / 60))
            return {"ok": False, "status": "preparing", "cached_month_ends": len(have), "target": st["target"],
                    "message": f"월말 데이터를 받는 중입니다 ({st['done']}/{st['target']}, 월말당 약 5~7초, 남은 시간 약 {remain}분). "
                               "잠시 후 다시 실행하세요.", "hint": "factor_cache_status 로 진행률을 볼 수 있습니다."}
        if st.get("finished_target", 0) < need:
            prefetch(years)
            return {"ok": False, "status": "preparing", "cached_month_ends": len(have), "target": need,
                    "message": f"{need}개 월말 스냅샷(10년이면 약 15분)을 받기 시작했습니다(현재 {len(have)}개). 잠시 후 다시 실행하세요.",
                    "hint": "factor_cache_status 로 진행률을 볼 수 있습니다."}
        # 받아봤는데 모자란 경우(KRX 오류 등): 있는 만큼으로 진행
    dates = have[-need:] if len(have) >= need else have
    if len(dates) < WARMUP_MONTHS + 6:
        return {"ok": False, "error": f"월말 데이터 부족 ({len(dates)}개, 최소 {WARMUP_MONTHS + 6}개 필요)", "prefetch": dict(_state)}

    P = _panels(dates)
    min_cap = float(min_cap_억) * 1e8
    mask = _universe_mask(P, min_cap, exclude_managed, float(min_price))
    ret = P["ret"]
    fwd = ret.shift(-1)                      # 형성월 t → 다음 한 달 수익률
    mask_f = mask & fwd.notna()              # 유니버스: 다음 달 수익률이 있는 종목
    row_ok = np.zeros(len(dates), dtype=bool)
    row_ok[WARMUP_MONTHS - 1:-1] = True      # 형성 가능 월: 준비기간 이후 ~ 마지막 바로 앞
    mask_f.loc[~row_ok, :] = False

    F = raw_factors(P)
    kospi_m, kosdaq_m = _index_monthly(dates)
    kflag = (P["kosdaq"] > 0).to_numpy()

    def _idx_fwd(j: int) -> pd.DataFrame:
        """행=형성월, 열=티커: 그 종목 시장 지수의 j 개월 뒤 월간 수익률."""
        return pd.DataFrame(np.where(kflag, kosdaq_m.shift(-j).to_numpy()[:, None], kospi_m.shift(-j).to_numpy()[:, None]),
                            index=ret.index, columns=ret.columns)

    uni_ret = fwd.where(mask_f).mean(axis=1)          # 형성월 기준 → 다음 달 유니버스 동일가중 평균
    uni_n = mask_f.sum(axis=1)
    uni_series = _to_return_month(uni_ret, dates, 1)

    def _port_series(sel: pd.DataFrame, k: int) -> tuple[pd.Series, pd.Series]:
        """선택 종목(행=형성월)의 월간 수익률(인덱스=수익률 월)과 그 종목들 시장 지수 수익률. k>1 이면 겹치는 코호트 평균."""
        parts, iparts = [], []
        for j in range(1, k + 1):
            parts.append(_to_return_month(ret.shift(-j).where(sel).mean(axis=1), dates, j))
            iparts.append(_to_return_month(_idx_fwd(j).where(sel).mean(axis=1), dates, j))
        return pd.concat(parts, axis=1).mean(axis=1), pd.concat(iparts, axis=1).mean(axis=1)

    # ── 복합 점수
    R = factor_ranks(F, names, mask_f)
    comp = composite_rank(R, names, mask_f)
    k = max(1, int(hold_months))
    if int(top_n) > 0:
        sel = ((-comp).rank(axis=1, method="first") <= int(top_n)) & mask_f
        top_rule = f"복합점수 상위 {int(top_n)}종목"
    else:
        sel = (comp > (1.0 - float(top_pct) / 100.0)) & mask_f
        top_rule = f"복합점수 상위 {float(top_pct):g}%"
    n_sel = sel.sum(axis=1)
    port, port_idx = _port_series(sel, k)
    st = _series_stats(port, uni_series, port_idx)
    deciles = _decile_table(comp, fwd, uni_ret, mask_f)
    crit = _criteria(st, [d.get("avg_month_pct") for d in deciles["rows"]] if deciles["rows"] else None)
    ic_stats = _ic_summary(_ic_series(comp, fwd, mask_f))

    # 월별·연도별
    monthly = []
    for i, d in enumerate(dates):
        if d not in port.index or pd.isna(port[d]) or d not in uni_series.index or pd.isna(uni_series[d]):
            continue
        fm = dates[i - 1] if i >= 1 else None
        monthly.append([f"{d[:4]}-{d[4:6]}", int(n_sel.get(fm, 0)) if fm else None,
                        round(float(port[d]) * 100, 2), round(float(uni_series[d]) * 100, 2),
                        None if (d not in port_idx.index or pd.isna(port_idx[d])) else round(float(port_idx[d]) * 100, 2),
                        round(float(port[d] - uni_series[d]) * 100, 2)])
    by_year_acc: dict[str, dict[str, list]] = {}
    for row in monthly:
        b = by_year_acc.setdefault(row[0][:4], {"port": [], "uni": [], "ex": []})
        b["port"].append(row[2]); b["uni"].append(row[3]); b["ex"].append(row[5])
    by_year = {y: {"months": len(b["ex"]), "port_avg_month_pct": round(float(np.mean(b["port"])), 2),
                   "universe_avg_month_pct": round(float(np.mean(b["uni"])), 2),
                   "excess_pp": round(float(np.mean(b["ex"])), 2),
                   "hit_rate_pct": round(float(np.mean([e > 0 for e in b["ex"]])) * 100, 0)}
               for y, b in by_year_acc.items()}

    # ── 참고: 상위 30종목 고정(개인 투자자 규모)
    sel30 = ((-comp).rank(axis=1, method="first") <= 30) & mask_f
    p30, i30 = _port_series(sel30, k)
    st30 = _series_stats(p30, uni_series, i30)

    out: dict[str, Any] = {
        "ok": True,
        "method": {"방식": "매월 말 유니버스 전종목을 팩터 백분위 순위로 점수화 → 상위 포트폴리오(동일가중) → 다음 달 수익률. "
                          "유니버스 동일가중 평균·종목별 시장 지수(KOSPI/KOSDAQ)와 비교. 학계 팩터 연구와 같은 횡단면 순위·월간 리밸런싱.",
                   "팩터": {n: (" + ".join(GROUP_FACTORS[n]) + " 순위 평균" if n in GROUP_FACTORS else ATOMIC_FACTORS[n][1]) for n in names},
                   "복합점수": "각 팩터 백분위 순위의 평균(팩터 2/3 이상 있는 종목만) → 다시 백분위(100 = 최상위)",
                   "상위_규칙": top_rule, "보유_개월": k, "리밸런싱": "월말",
                   "유니버스": f"보통주(우선주·스팩·코넥스 제외){', 관리·환기종목 제외' if exclude_managed else ''}, "
                               f"시총 {float(min_cap_억):g}억 이상, 주가 {float(min_price):g}원 이상, 다음 달 가격 존재"},
        "period": {"formation_from": dates[WARMUP_MONTHS - 1], "formation_to": dates[-2], "return_months": st.get("months"),
                   "snapshots": len(dates), "data_from": dates[0], "data_to": dates[-1],
                   "avg_universe_size": int(round(float(uni_n[row_ok].mean()))) if bool(row_ok.any()) else None},
        "criteria_academic": crit,
        "portfolio": st,
        "ic": ic_stats,
        "deciles": deciles,
        "top30_fixed": st30,
        "by_year": by_year,
        "monthly_columns": ["수익률월", "보유종목수", "포트_pct", "유니버스_pct", "지수_pct", "초과_pp"],
        "monthly": monthly[-int(max_months_listed):] if max_months_listed else [],
    }
    if include_single_factor_table:
        out["single_factors"] = _single_factor_table(F, fwd, uni_ret, mask_f, dates)
    out["data_quality"] = _data_quality(dates)
    out["caveats"] = [
        "월간 수익률은 KRX 전종목등락률(수정주가)로 액면분할·무상증자 등을 보정. 기준가 검증에 실패한 달은 종가 기반(상장주식수 변동 보정)으로 대체 — data_quality 참고",
        "다음 달 가격이 없는 종목(상장폐지·거래정지)은 그 달 유니버스·포트폴리오에서 제외 → 상장폐지 손실 미반영(약간의 상향 편향, 유니버스에도 동일 적용)",
        "PER·PBR·EPS·BPS·배당은 KRX 가 해당 월말에 공표한 값(최근 사업보고서 기준) → 미래 정보 없음. 적자·미공표 EPS 는 0 으로 들어옴",
        "저변동성은 월간 수익률 12개 기준(학계는 일간 기준도 사용). 회전율은 월간 거래대금/시총",
        "월말 종가 매수·다음 월말 종가 매도, 동일가중, 거래비용·호가·세금 미반영(상위 10% ≈ 80~150종목 보유 가정; top30_fixed 는 30종목)",
        "표본이 10년이어도 시장 국면은 몇 개뿐 → t-통계량과 전·후반 일관성을 함께 볼 것",
    ]
    out["elapsed_sec"] = round(time.time() - t0, 1)
    out["note"] = "과거 통계이며 미래 수익을 보장하지 않습니다. 매매 권유가 아닙니다."
    return out


# ────────────────────────────────────────────────────────────
# 스크리너 (최근 거래일 기준 상위 종목)
# ────────────────────────────────────────────────────────────
def _latest_trading_snapshot() -> tuple[str, pd.DataFrame, dict[str, str]] | None:
    """오늘(장 마감 후) 또는 직전 거래일 전종목시세 + 직전 월말→현재 등락률 + 펀더멘털. 하루 캐시."""
    d = _today()
    if d.hour < 16:
        d -= timedelta(days=1)
    with _lock:
        have = sorted(_snaps)
    prev_ymd = have[-1] if have else None
    prev_df = _snaps[prev_ymd] if prev_ymd else None
    for _ in range(10):
        ymd = _ymd(d)
        if d.weekday() >= 5 or ymd in _holidays:
            d -= timedelta(days=1)
            continue
        if _live.get("date") == ymd:
            return ymd, _live["df"], _live["names"]
        if prev_ymd == ymd:              # 최근 거래일이 바로 캐시된 월말이면 그 스냅샷을 그대로
            names = _names_latest[1] if (_names_latest and _names_latest[0] == ymd) else {}
            return ymd, prev_df, names
        prices = _fetch_prices(ymd)
        _sleep()
        if prices is None or prices.empty:
            _holidays.add(ymd)
            d -= timedelta(days=1)
            continue
        chg, _pct = _fetch_change_verified(prev_ymd, ymd, prev_df, prices.index)
        fund = _fetch_fund(ymd)
        snap, meta = _build_snapshot(ymd, prices, chg, fund, prev_df)
        names = {str(t): str(n) for t, n in zip(prices.index, prices["name"].astype(str))}
        _live.update(date=ymd, df=snap, names=names, meta=meta)
        _malloc_trim()
        return ymd, snap, names
    return None


def run_screen(factors: Any = DEFAULT_FACTORS, min_cap_억: float = 1000, top_n: int = 30, exclude_managed: bool = True,
               min_price: float = 1000) -> dict[str, Any]:
    t0 = time.time()
    try:
        names_f = _parse_factors(factors)
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    with _lock:
        have = sorted(_snaps)
    if len(have) < WARMUP_MONTHS:
        st = dict(_state)
        if not st["running"] and st.get("finished_target", 0) < WARMUP_MONTHS:
            prefetch(DEFAULT_YEARS)
        return {"ok": False, "status": "preparing", "cached_month_ends": len(have),
                "message": f"모멘텀·변동성 계산에 월말 {WARMUP_MONTHS}개가 필요합니다(현재 {len(have)}개). "
                           "백그라운드로 받고 있으니 잠시 후 다시 실행하세요.", "hint": "factor_cache_status 로 진행률 확인"}
    live = _latest_trading_snapshot()
    if live is None:
        return {"ok": False, "error": "최근 거래일 시세를 받지 못했습니다(KRX 응답 없음)."}
    ymd, snap, names = live
    hist = [d for d in have if d < ymd][-WARMUP_MONTHS:]
    P = _panels(hist, extra=snap, extra_date=ymd)
    mask = _universe_mask(P, float(min_cap_억) * 1e8, exclude_managed, float(min_price))
    F = raw_factors(P)
    R = factor_ranks(F, names_f, mask)
    comp = composite_rank(R, names_f, mask)
    last = comp.index[-1]
    row = comp.loc[last].dropna().sort_values(ascending=False)
    uni_n = int(mask.loc[last].sum())
    L = {k: v.loc[last] for k, v in P.items()}
    FL = {k: v.loc[last] for k, v in F.items()}
    RL = {k: v.loc[last] for k, v in R.items()}
    fallback_names = _names_latest[1] if _names_latest else {}

    def _v(s: pd.Series, t: str, nd: int = 2, scale: float = 1.0):
        x = s.get(t, np.nan)
        return None if pd.isna(x) else round(float(x) * scale, nd)

    rows = []
    for t in row.index[: int(top_n)]:
        rows.append({
            "code": t, "name": names.get(t) or fallback_names.get(t, t),
            "market": "KOSDAQ" if L["kosdaq"].get(t, 0) > 0 else "KOSPI",
            "close": _v(L["close"], t, 0), "cap_억": _v(L["cap"], t, 0, 1e-8),
            "composite_pct": round(float(row[t]) * 100, 1),
            "ranks_pct": {n: _v(RL[n], t, 1, 100) for n in names_f},
            "PBR": _v(L["pbr"], t), "PER": _v(L["per"], t, 1), "DIV_pct": _v(L["div"], t),
            "ROE_pct": _v(FL["roe"], t, 1, 100), "mom_12_1_pct": _v(FL["mom"], t, 1, 100),
            "ret_1m_pct": _v(FL["rev"], t, 1, 100), "vol_12m_pct": _v(FL["lowvol"], t, 1, 100),
            "high52_pct": _v(FL["high52"], t, 1, 100), "turnover_m_pct": _v(FL["lowturn"], t, 1, 100),
        })
    return {"ok": True, "as_of": f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}",
            "method": {"팩터": names_f, "복합점수": "팩터 백분위 순위 평균 → 백분위(100 = 최상위)",
                       "유니버스": f"보통주(우선주·스팩·코넥스 제외){', 관리·환기종목 제외' if exclude_managed else ''}, "
                                   f"시총 {float(min_cap_억):g}억 이상, 주가 {float(min_price):g}원 이상",
                       "참고": "ret_1m 은 직전 월말→최근 거래일(월중 부분) 수익률. 월말 리밸런싱·1개월 보유 전제의 점수"},
            "universe_size": uni_n, "scored_count": int(len(row)), "top": rows,
            "elapsed_sec": round(time.time() - t0, 1),
            "note": "사실 데이터 집계이며 매매 권유가 아닙니다. 백테스트(backtest_factors)에서 기준을 통과한 팩터 조합만 쓰세요.",
            "source": "KRX via pykrx"}


if os.environ.get("FACTOR_PREFETCH", "1") != "0":
    prefetch()
