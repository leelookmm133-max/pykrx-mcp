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
     → 액면분할·무상증자 등이 보정된 월간 수익률. 시작일은 '직전 월말의 다음 거래일'(KOSPI 지수 일별 데이터로 만든
       거래일 달력 사용; 주말·휴일을 시작일로 넣으면 KRX 가 빈 응답을 줌). 시작일 기준가가 직전 월말 종가와 맞는지 검증해
       90% 이상 일치하면 KRX 값을 전부 쓰고(불일치분은 분할·증자 보정분), 낮으면 종목별로 기준가가 맞거나 주식수 변동으로
       설명되는 종목만 KRX 값, 나머지는 종가 기반(상장주식수 변동 보정)으로 씁니다(hybrid). 조회 자체가 실패한 달은 종가 기반.
  3. PER/PBR/배당수익률(ALL): PER·PBR·EPS·BPS·DIV (KRX 가 그날 공표한 값 → 미래 정보 없음)
  거래일 달력은 KOSPI 지수(1001) 일별 종가에서 연 단위로 받아 캐시합니다(10년 ≈ 11회). factor_month_debug 로 월별 품질을 봅니다.
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

try:
    import cachestore            # 같은 폴더. Railway 볼륨(CACHE_DIR/RAILWAY_VOLUME_MOUNT_PATH)에 캐시 저장·복구
except Exception:  # noqa: BLE001
    cachestore = None

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

# 외부 모듈(signals.py 등)이 팩터를 추가하는 레지스트리. fn(P) -> 패널(행=날짜, 열=티커), P 는 raw_factors 가 받는 패널 dict.
EXTRA_FACTORS: dict[str, Any] = {}


def register_factor(name: str, higher_better: bool, desc: str, fn) -> None:
    name = name.lower()
    ATOMIC_FACTORS[name] = (bool(higher_better), desc)
    EXTRA_FACTORS[name] = fn
    if name not in ALL_FOR_TABLE:
        ALL_FOR_TABLE.append(name)


def register_group(name: str, members: list[str]) -> None:
    GROUP_FACTORS[name.lower()] = [m.lower() for m in members]
    if name.lower() not in ALL_FOR_TABLE:
        ALL_FOR_TABLE.append(name.lower())

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


_last_change_error: dict[str, Any] = {"when": None, "error": None}


def _fetch_change(start: str, end: str) -> pd.DataFrame:
    """전종목등락률(ALL, 수정주가). 반환: index=ticker, 열: base(시작일 기준가), close_e, ret(소수), vol, val. 실패 시 빈 DF.

    KRX 엔드포인트(MDCSTAT01602)를 직접 호출해 직접 파싱합니다(pykrx 래퍼는 오류를 삼키고 빈 표를 돌려주며,
    기준가를 int32 로 바꾸다 소수점·'-' 값에서 실패할 수 있음). 직접 호출이 안 되면 pykrx 래퍼로 폴백."""
    df = None
    if _krx_core is not None and hasattr(_krx_core, "전종목등락률"):
        try:
            raw = _retry(_krx_core.전종목등락률().fetch, start, end, "ALL", 2, tries=2)
            if raw is not None and not raw.empty and "ISU_SRT_CD" in raw.columns:
                df = pd.DataFrame({"base": _num(raw["BAS_PRC"]).values if "BAS_PRC" in raw.columns else np.nan,
                                   "close_e": _num(raw["TDD_CLSPRC"]).values,
                                   "rate": _num(raw["FLUC_RT"]).values if "FLUC_RT" in raw.columns else np.nan,
                                   "vol": _num(raw["ACC_TRDVOL"]).values if "ACC_TRDVOL" in raw.columns else np.nan,
                                   "val": _num(raw["ACC_TRDVAL"]).values if "ACC_TRDVAL" in raw.columns else np.nan},
                                  index=pd.Index(raw["ISU_SRT_CD"].astype(str).str.strip().values, name="ticker"))
        except Exception as e:  # noqa: BLE001
            _last_change_error.update(when=f"{start}~{end}", error=f"{type(e).__name__}: {e}")
            log.warning("전종목등락률(core) 실패 %s~%s: %s", start, end, e)
            df = None
    if df is None or df.empty:
        try:
            if _krx_wrap is not None and hasattr(_krx_wrap, "get_market_price_change_by_ticker"):
                w = _retry(_krx_wrap.get_market_price_change_by_ticker, start, end, "ALL", True, tries=2)
            else:
                w = _retry(stock.get_market_price_change, start, end, market="ALL", adjusted=True, tries=2)
            if w is not None and not w.empty and "시가" in w.columns and "종가" in w.columns:
                df = pd.DataFrame({"base": _num(w["시가"]).values, "close_e": _num(w["종가"]).values,
                                   "rate": _num(w["등락률"]).values if "등락률" in w.columns else np.nan,
                                   "vol": _num(w["거래량"]).values if "거래량" in w.columns else np.nan,
                                   "val": _num(w["거래대금"]).values if "거래대금" in w.columns else np.nan},
                                  index=pd.Index(w.index.astype(str).str.strip(), name="ticker"))
        except Exception as e:  # noqa: BLE001
            _last_change_error.update(when=f"{start}~{end}", error=f"{type(e).__name__}: {e}")
            log.warning("전종목등락률(wrap) 실패 %s~%s: %s", start, end, e)
            df = None
    if df is None or df.empty:
        return pd.DataFrame()
    df = df[~df.index.duplicated()]
    base, close = df["base"], df["close_e"]
    ret = close / base.where(base > 0) - 1.0
    r2 = df["rate"] / 100.0
    ret = r2.where(r2.notna() & (base > 0), ret)
    out = pd.DataFrame({"base": base.values, "close_e": close.values, "ret": ret.values,
                        "vol": df["vol"].values, "val": df["val"].values}, index=df.index)
    if out["base"].fillna(0).sum() == 0:           # 전부 '-' 면 조회 실패와 같음
        return pd.DataFrame()
    return out


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


def _index_year(code: str, year: int, upto: str) -> pd.Series | None:
    """지수 종가 1년 치(일별, 인덱스 'YYYYMMDD'). 연도 단위로 캐시. 현재 연도는 upto(오늘)까지 받고 캐시하지 않는다."""
    key = (code, str(year), "")
    if key in _index_cache:
        return _index_cache[key]
    start = f"{year}0101"
    end = f"{year}1231" if f"{year}1231" <= upto else upto
    out = None
    try:
        df = _retry(stock.get_index_ohlcv, start, end, code, tries=2)
        if df is not None and not df.empty and "종가" in df.columns:
            ser = df["종가"].astype(float)
            ser.index = [d.strftime("%Y%m%d") for d in pd.to_datetime(ser.index)]
            out = ser[~ser.index.duplicated()].sort_index()
    except Exception as ex:  # noqa: BLE001
        log.warning("지수 조회 실패(%s, %d): %s", code, year, ex)
    _sleep()
    if end == f"{year}1231":          # 지난 연도만 캐시(올해는 매일 늘어남)
        _index_cache[key] = out
    return out


def _index_series(code: str, start: str, end: str) -> pd.Series | None:
    """지수 종가(일별, 인덱스 'YYYYMMDD'), start~end. 연도 캐시를 이어 붙인다."""
    parts = []
    today = _ymd(_today())
    for y in range(int(start[:4]), int(end[:4]) + 1):
        s = _index_year(code, y, today)
        if s is not None and len(s):
            parts.append(s)
    if not parts:
        return None
    out = pd.concat(parts).sort_index()
    out = out[~out.index.duplicated()]
    return out[(out.index >= start) & (out.index <= end)]


_calendar: dict[str, Any] = {"days": [], "from": None, "to": None}     # KOSPI 지수가 있는 날 = 거래일


def _ensure_calendar(start: str, end: str) -> list[str]:
    """start~end 거래일 목록(오름차순). KOSPI(1001) 지수 일별 데이터에서 만든다. 실패하면 빈 목록."""
    if _calendar["from"] and _calendar["to"] and _calendar["from"] <= start and _calendar["to"] >= end:
        return [d for d in _calendar["days"] if start <= d <= end]
    s = _index_series("1001", start, end)
    if s is None or len(s) == 0:
        return []
    days = sorted(set(_calendar["days"]) | set(s.index))
    _calendar["days"] = days
    _calendar["from"] = min(start, _calendar["from"] or start)
    _calendar["to"] = max(end, _calendar["to"] or end)
    return [d for d in days if start <= d <= end]


def _next_trading_day(ymd: str) -> str:
    """ymd 다음 거래일(달력이 있을 때). 없으면 ymd+1일(주말이면 월요일)."""
    days = _calendar["days"]
    if days:
        i = np.searchsorted(np.array(days), ymd, side="right")
        if i < len(days):
            return days[i]
    d = _parse(ymd) + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return _ymd(d)


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

    meta: dict[str, Any] = {"ret_source": "none", "base_match_pct": None, "krx_used_pct": None}
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
            valid = krx_ret.notna() & ret_close.notna()
            if pct is not None and pct >= BASE_MATCH_MIN:
                # 기준가의 90% 이상이 직전 월말 종가와 일치 → 구간 해석이 맞음. 불일치분은 분할·증자 등 KRX 보정분이므로 KRX 값을 전부 신뢰.
                ret = krx_ret.where(krx_ret.notna(), ret_close)
                meta["ret_source"] = "krx_adj"
                meta["krx_used_pct"] = round(float(krx_ret.notna()[valid].mean()) * 100, 1) if int(valid.sum()) else None
            else:
                # 구간 해석이 불확실한 달: 종목별로 기준가가 직전 월말 종가와 맞거나(base_ok), 상장주식수 변동으로 설명되면(분할·무상증자:
                # share_ok) KRX 값을, 아니면 종가 기반을 쓴다.
                pc = prev["close"].reindex(idx)
                ps = prev["shares"].reindex(idx)
                base_ok = ((c["base"] / pc.where(pc > 0) - 1.0).abs() < 0.005)
                sr = snap["shares"] / ps.where(ps > 0)
                with np.errstate(invalid="ignore", divide="ignore"):
                    # 주식수가 실제로 바뀐(±5% 초과) 종목에서, 기준가 변화가 그 비율로 설명되면 KRX 보정값으로 인정
                    share_ok = sr.notna() & (np.log(sr).abs() > 0.05) & (np.log((c["base"] / pc.where(pc > 0)) * sr).abs() < 0.03)
                use = krx_ret.notna() & (base_ok.fillna(False) | share_ok.fillna(False))
                ret = krx_ret.where(use, ret_close)
                meta["ret_source"] = "hybrid"
                meta["krx_used_pct"] = round(float(use[valid].mean()) * 100, 1) if int(valid.sum()) else None
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
    """(연, 월)의 마지막 거래일과 그날의 전종목시세.
    거래일 달력(KOSPI 지수)이 있으면 그 달의 마지막 거래일을 바로 쓰고, 없으면 월말부터 거슬러 최대 10일 탐색."""
    last = calendar.monthrange(y, m)[1]
    first_s, last_s = f"{y}{m:02d}01", f"{y}{m:02d}{last:02d}"
    cal = [d for d in _calendar["days"] if first_s <= d <= last_s]
    if cal:
        ymd = cal[-1]
        df = _fetch_prices(ymd)
        _sleep()
        if df is not None and not df.empty:
            _month_end[(y, m)] = ymd
            return ymd, df
        # 달력과 다르면(지수는 있는데 종목 시세가 없음 — 드묾) 탐색으로
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
                           idx: pd.Index) -> tuple[pd.DataFrame, float | None, str | None]:
    """직전 월말→이번 월말 등락률. 시작일은 '직전 월말의 다음 거래일'(그날의 기준가 = 직전 월말 종가).
    기준가 일치율이 낮으면 시작일을 직전 월말 당일로 바꿔 한 번 더 시도하고, 더 잘 맞는 쪽을 쓴다.
    반환: (등락률 표, 기준가 일치율, 사용한 시작일)."""
    if prev_ymd is None:
        start = _ymd(_parse(ymd).replace(day=1))
        chg = _fetch_change(start, ymd)
        _sleep()
        return chg, None, start
    first = _chg_mode["mode"] or "next_trading_day"
    modes = [first] + [m for m in ("next_trading_day", "same_day") if m != first]
    best: tuple[str, pd.DataFrame, float | None, str] | None = None
    for mode in modes:
        start = _next_trading_day(prev_ymd) if mode == "next_trading_day" else prev_ymd
        if start > ymd:
            continue
        chg = _fetch_change(start, ymd)
        _sleep()
        pct = _base_match_pct(chg, prev_df, idx)
        if best is None or (pct if pct is not None else -1) > (best[2] if best[2] is not None else -1):
            best = (mode, chg, pct, start)
        if pct is not None and pct >= BASE_MATCH_MIN:
            break
    if best is None:
        return pd.DataFrame(), None, None
    if _chg_mode["mode"] is None and best[2] is not None and best[2] >= BASE_MATCH_MIN:
        _chg_mode["mode"] = best[0]
        log.info("전종목등락률 시작일 해석: %s (기준가 일치 %.1f%%)", best[0], best[2])
    return best[1], best[2], best[3]


CACHE_NAME = "factors"


def save_cache() -> bool:
    """월말 스냅샷·메타·달력을 디스크에 저장(저장소가 있을 때만)."""
    if cachestore is None:
        return False
    with _lock:
        payload = {"version": 1, "snaps": dict(_snaps), "meta": dict(_snap_meta), "month_end": dict(_month_end),
                   "holidays": set(_holidays), "calendar": dict(_calendar), "chg_mode": dict(_chg_mode),
                   "names_latest": _names_latest, "saved_at": _today().strftime("%Y-%m-%d %H:%M:%S")}
    return cachestore.save(CACHE_NAME, payload)


def load_cache() -> int:
    """디스크 캐시를 메모리로 복구. 복구한 월말 수를 돌려준다(저장소 없으면 0)."""
    global _names_latest
    if cachestore is None:
        return 0
    obj = cachestore.load(CACHE_NAME)
    if not obj or not isinstance(obj, dict) or "snaps" not in obj:
        return 0
    with _lock:
        for d, df in obj["snaps"].items():
            if isinstance(df, pd.DataFrame) and list(df.columns) == SNAP_COLS:
                _snaps[d] = df
                _snap_meta[d] = obj.get("meta", {}).get(d, {})
        _month_end.update(obj.get("month_end", {}))
        _holidays.update(obj.get("holidays", set()))
        cal = obj.get("calendar") or {}
        if cal.get("days"):
            _calendar.update(cal)
        if obj.get("chg_mode", {}).get("mode"):
            _chg_mode["mode"] = obj["chg_mode"]["mode"]
        if obj.get("names_latest"):
            _names_latest = obj["names_latest"]
    log.info("factors 캐시 복구: 월말 %d개 (저장 %s)", len(_snaps), obj.get("saved_at"))
    return len(_snaps)


def ensure_months(n_months: int) -> list[str]:
    """최근 n_months 개 월말 스냅샷을 (없으면 받아서) 캐시에 채우고, 날짜 목록(오름차순)을 돌려준다."""
    global _names_latest
    fetched_new = False
    months = _month_list(n_months)
    # 거래일 달력(KOSPI 지수 일별): 월말 거래일과 '직전 월말 다음 거래일'을 정확히 잡기 위해 먼저 받는다(연 1회 호출).
    y0, m0 = months[0]
    cal = _ensure_calendar(f"{y0}{m0:02d}01", _ymd(_today()))
    if not cal:
        log.warning("거래일 달력(KOSPI 지수)을 받지 못해 월말 탐색·시작일 추정으로 진행합니다")
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
        chg, _pct, chg_start = _fetch_change_verified(prev_ymd, ymd, prev_df, prices.index)
        fund = _fetch_fund(ymd)
        _sleep()
        snap, meta = _build_snapshot(ymd, prices, chg, fund, prev_df)
        meta["prev"] = prev_ymd
        meta["chg_start"] = chg_start
        meta["chg_empty"] = bool(chg is None or chg.empty)
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
        fetched_new = True
    dates.sort()
    if fetched_new:
        save_cache()
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
        metas = {d: _snap_meta.get(d, {}) for d in dates}
    srcs = [m.get("ret_source") for m in metas.values()]
    matches = [m.get("base_match_pct") for m in metas.values() if m.get("base_match_pct") is not None]
    fund_ok = sum(1 for m in metas.values() if m.get("fund_ok"))
    st = dict(_state)
    out = {"cached_month_ends": len(dates), "earliest": dates[0] if dates else None, "latest": dates[-1] if dates else None,
           "prefetch": st, "process_rss_mb": _rss_mb(),
           "calendar_days": len(_calendar["days"]),
           "return_source": {"krx_adjusted": srcs.count("krx_adj"), "hybrid": srcs.count("hybrid"),
                             "close_fallback": srcs.count("close"), "unverified_first": srcs.count("krx_adj_unverified"),
                             "none": srcs.count("none"),
                             "base_match_pct_avg": round(float(np.mean(matches)), 1) if matches else None,
                             "months_change_empty": sum(1 for m in metas.values() if m.get("chg_empty")),
                             "start_date_mode": _chg_mode["mode"], "last_change_error": dict(_last_change_error)},
           "fundamental_months": fund_ok,
           "persist": cachestore.status(CACHE_NAME) if cachestore is not None else {"enabled": False},
           "note": "backtest_factors 는 (years×12 + 13)개 월말이 있어야 전 기간을 계산합니다. 부족하면 있는 만큼 계산합니다. "
                   "월별 상세는 factor_month_debug 로 확인합니다."}
    if st["running"] and st["target"]:
        out["progress"] = f"{st['done']}/{st['target']} 월말 (월말당 KRX 3회, 약 5~7초)"
    return out


def month_debug(ymd: str = "", n: int = 20) -> dict[str, Any]:
    """월별 데이터 품질 진단. ymd 가 비면 모든 월말의 [월말, 수익률출처, 기준가일치율, KRX사용비율, 등락률시작일, 상장수] 표.
    ymd 를 주면 그 달의 전종목등락률을 다시 받아 기준가가 직전 월말 종가와 다른 종목 n 개를 보여준다."""
    with _lock:
        dates = sorted(_snaps)
        metas = {d: dict(_snap_meta.get(d, {})) for d in dates}
    if not ymd:
        rows = [[d, m.get("ret_source"), m.get("base_match_pct"), m.get("krx_used_pct"), m.get("chg_start"), m.get("n")]
                for d, m in metas.items()]
        return {"ok": True, "columns": ["month_end", "ret_source", "base_match_pct", "krx_used_pct", "chg_start", "listed"],
                "rows": rows, "calendar_days": len(_calendar["days"]), "start_date_mode": _chg_mode["mode"],
                "last_change_error": dict(_last_change_error)}
    ymd = str(ymd).replace("-", "")
    if ymd not in metas:
        return {"ok": False, "error": f"{ymd} 는 캐시된 월말이 아닙니다. 캐시된 월말: {dates[:3]} … {dates[-3:]}"}
    meta = metas[ymd]
    prev_ymd = meta.get("prev")
    if not prev_ymd:
        return {"ok": True, "month_end": ymd, "meta": meta, "note": "첫 스냅샷이라 직전 월말이 없습니다."}
    with _lock:
        prev = _snaps[prev_ymd]
        cur = _snaps[ymd]
    start = meta.get("chg_start") or _next_trading_day(prev_ymd)
    chg = _fetch_change(start, ymd)
    if chg is None or chg.empty:
        return {"ok": True, "month_end": ymd, "meta": meta, "refetch": {"start": start, "result": "빈 응답",
                                                                     "last_change_error": dict(_last_change_error)}}
    idx = cur.index
    c = chg.reindex(idx)
    pc = prev["close"].reindex(idx)
    ps = prev["shares"].reindex(idx)
    both = c["base"].notna() & pc.notna() & (pc > 0) & (c["base"] > 0)
    ratio = c["base"] / pc.where(pc > 0)
    mism = both & ((ratio - 1.0).abs() >= 0.005)
    names = _names_latest[1] if _names_latest else {}
    rows = []
    for t in idx[mism.to_numpy()][: int(n)]:
        sr = float(cur["shares"].get(t, np.nan) / ps.get(t, np.nan)) if ps.get(t, 0) else None
        rows.append({"code": t, "name": names.get(t, t), "prev_close": float(pc[t]), "krx_base": float(c["base"][t]),
                     "base_over_prev": round(float(ratio[t]), 4), "shares_ratio": None if sr is None or np.isnan(sr) else round(sr, 4),
                     "krx_ret_pct": None if pd.isna(c["ret"][t]) else round(float(c["ret"][t]) * 100, 2),
                     "cur_close": float(cur["close"].get(t, np.nan)),
                     "used_ret_pct": None if pd.isna(cur["ret"].get(t, np.nan)) else round(float(cur["ret"][t]) * 100, 2)})
    return {"ok": True, "month_end": ymd, "prev_month_end": prev_ymd, "meta": meta,
            "refetch": {"start": start, "compared": int(both.sum()), "mismatch": int(mism.sum()),
                        "match_pct": round(float((both & ~mism).sum()) / int(both.sum()) * 100, 1) if int(both.sum()) else None},
            "mismatch_samples": rows}


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
    for name, fn in EXTRA_FACTORS.items():
        try:
            out = fn(P)
            F[name] = out.reindex(index=close.index, columns=close.columns).astype(float)
        except Exception as e:  # noqa: BLE001
            log.warning("추가 팩터 %s 계산 실패: %s", name, e)
            F[name] = pd.DataFrame(np.nan, index=close.index, columns=close.columns)
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
    # 팩터 3개면 2개, 4개면 3개, 5개면 3개 이상 있어야 복합점수 계산(반올림; 이전 ceil 은 3개 중 3개를 요구했음)
    need = max(1, int(math.floor(len(frames) * min_frac + 0.5)))
    avg[cnt < need] = np.nan
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


def _tstat_nw(x: pd.Series, lags: int = 0) -> float | None:
    """Newey-West(Bartlett) 표준오차 기반 t. 겹치는 코호트(보유 k개월)의 월간 수익률은 k-1 시차까지 자기상관이 있어
    단순 t 가 과대평가되므로 lags = k-1 로 보정한다. lags=0 이면 단순 t 와 같다."""
    arr = x.dropna().to_numpy(dtype=float)
    n = len(arr)
    if n < 3:
        return None
    m = float(arr.mean())
    e = arr - m
    var = float((e * e).mean())
    L = max(0, min(int(lags), n - 2))
    for l in range(1, L + 1):
        w = 1.0 - l / (L + 1.0)
        var += 2.0 * w * float((e[l:] * e[:-l]).mean())
    var *= n / (n - 1.0)          # 소표본 보정 — lags=0 이면 단순 t(ddof=1)와 정확히 일치
    if var <= 0:
        return None
    return round(m / math.sqrt(var / n), 2)


def _cagr(r: pd.Series) -> float | None:
    r = r.dropna()
    if len(r) == 0:
        return None
    return round((float(np.prod(1 + r.to_numpy())) ** (12 / len(r)) - 1) * 100, 1)


def _series_stats(port: pd.Series, uni: pd.Series, idx: pd.Series | None, nw_lags: int = 0,
                  cost: pd.Series | None = None) -> dict[str, Any]:
    """월간 수익률 시리즈(소수) → 요약. port/uni/idx 는 같은 인덱스(수익률 월).
    nw_lags: 겹치는 코호트 보정(보유 k개월 → k-1). cost: 월별 비용(소수, 수익률 월 인덱스) → 비용 후 지표도 계산."""
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
        "excess_t_stat": _tstat_nw(ex, nw_lags),
        "hit_rate_vs_universe_pct": round(float((ex > 0).mean()) * 100, 1),
        "excess_first_half_pp": round(float(ex.iloc[:half].mean()) * 100, 2) if half else None,
        "excess_second_half_pp": round(float(ex.iloc[half:].mean()) * 100, 2) if half else None,
        "cagr_pct": _cagr(p),
        "universe_cagr_pct": _cagr(u),
        "ann_vol_pct": round(float(p.std(ddof=1)) * math.sqrt(12) * 100, 1) if n > 1 else None,
        "max_drawdown_pct": round(float((cum / cum.cummax() - 1).min()) * 100, 1),
    }
    if nw_lags > 0:
        out["excess_t_stat_plain"] = _tstat(ex)
        out["t_stat_note"] = f"excess_t_stat 은 Newey-West(시차 {nw_lags}) 보정값, _plain 은 단순 t"
    if idx is not None:
        i = idx.reindex(p.index)
        oki = i.notna()
        if bool(oki.any()):
            exi = p[oki] - i[oki]
            out["index_avg_month_pct"] = round(float(i[oki].mean()) * 100, 2)
            out["excess_vs_index_pp"] = round(float(exi.mean()) * 100, 2)
            out["excess_vs_index_t_stat"] = _tstat_nw(exi, nw_lags)
            out["hit_rate_vs_index_pct"] = round(float((exi > 0).mean()) * 100, 1)
            out["index_cagr_pct"] = _cagr(i[oki])
    if cost is not None:
        c = cost.reindex(p.index).fillna(0.0)
        pn = p - c
        exn = pn - u
        out["cost"] = {
            "avg_month_cost_pp": round(float(c.mean()) * 100, 3),
            "annual_cost_drag_pp": round(float(c.mean()) * 12 * 100, 2),
            "net_avg_month_pct": round(float(pn.mean()) * 100, 2),
            "net_excess_vs_universe_pp": round(float(exn.mean()) * 100, 2),
            "net_excess_t_stat": _tstat_nw(exn, nw_lags),
            "net_hit_rate_vs_universe_pct": round(float((exn > 0).mean()) * 100, 1),
            "net_cagr_pct": _cagr(pn),
        }
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
    out = {"rows": rows, "passed": f"{passed}/5",
           "verdict": "통과(5/5)" if passed == 5 else ("부분 통과" if passed >= 3 else "미달")}
    cost = st.get("cost")
    if cost:
        net = cost.get("net_excess_vs_universe_pp")
        out["rows"].append({"기준": "⑥ (참고) 비용 후 초과수익", "값": None if net is None else f"{net:+.2f}%p",
                            "기준값": "> 0 (판정에는 미포함)", "통과": bool(net is not None and net > 0)})
    return out


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


def _universe_mask(P: dict[str, pd.DataFrame], min_cap: float, exclude_managed: bool, min_price: float,
                   exclude_suspended: bool = True) -> pd.DataFrame:
    close, cap = P["close"], P["cap"]
    m = (P["common"] > 0) & close.notna() & (close >= min_price) & cap.notna() & (cap >= min_cap)
    if exclude_managed:
        m &= ~(P["managed"] > 0)
    if exclude_suspended and "tvol_m" in P:
        # 그 달 거래량이 0 인 종목(거래정지)은 살 수 없으므로 편입 후보에서 제외.
        # 등락률 조회가 실패해 월간 거래량이 통째로 없는 달(행의 절반 이상 NaN)에는 적용하지 않는다.
        tv = P["tvol_m"]
        row_has = tv.notna().mean(axis=1) > 0.5
        tradable = (tv > 0) | ~pd.DataFrame(np.repeat(row_has.to_numpy()[:, None], tv.shape[1], axis=1), index=tv.index, columns=tv.columns)
        m &= tradable
    return m


def _apply_delisting(P: dict[str, pd.DataFrame], delist_return_pct: float | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """다음 달 스냅샷에서 사라진 종목(상장폐지·합병 등)에 delist_return_pct 를 그 달 수익률로 부여.
    반환: (보정된 월간 수익률 패널, 사라진 종목 표시 패널[행=마지막 관측 월])."""
    close, ret = P["close"], P["ret"]
    present = close.notna()
    nxt = present.shift(-1, fill_value=False)
    vanished = present & ~nxt
    vanished.iloc[-1, :] = False                      # 마지막 월말은 다음 달이 없어 판단 불가
    ret_adj = ret.copy()
    if delist_return_pct is not None:
        hit = vanished.shift(1, fill_value=False)      # 사라진 종목의 '그 다음 달' 칸
        ret_adj = ret_adj.where(~hit, float(delist_return_pct) / 100.0)
    return ret_adj, vanished


def _turnover(sel: pd.DataFrame, k: int) -> pd.Series:
    """형성월별 편도 교체 비율(포트폴리오 전체 대비). k>1 이면 그 달에 교체되는 코호트(1/k)만큼으로 환산."""
    s = sel.to_numpy(dtype=bool)
    n_t = s.sum(axis=1)
    out = np.full(len(s), np.nan)
    for i in range(k, len(s)):
        if n_t[i] == 0 or n_t[i - k] == 0:
            continue
        overlap = int((s[i] & s[i - k]).sum())
        out[i] = (1.0 - overlap / n_t[i]) / k
    return pd.Series(out, index=sel.index)


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
    names_all = [n for n in ALL_FOR_TABLE if n in ATOMIC_FACTORS or n in GROUP_FACTORS]
    uni_series = _to_return_month(uni_ret, dates, 1)
    for n in names_all:
        r = factor_ranks(F, [n], mask)[n]          # 팩터 하나씩 계산해 메모리 피크를 낮춘다(30개 동시 보유 방지)
        if int(r.notna().to_numpy().sum()) == 0:
            table[n] = {"설명": (" + ".join(GROUP_FACTORS[n]) + " 순위 평균" if n in GROUP_FACTORS else ATOMIC_FACTORS[n][1]),
                        "months": 0, "note": "데이터 없음(신호 미수집 또는 DART 키 없음)"}
            continue
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
        desc = ("가치 복합(1/PBR·EPS/P·배당 순위 평균)" if n == "value"
                else (" + ".join(GROUP_FACTORS[n]) + " 순위 평균" if n in GROUP_FACTORS else ATOMIC_FACTORS[n][1]))
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
    ku = [m.get("krx_used_pct") for m in metas if m.get("krx_used_pct") is not None]
    return {"months_krx_adjusted_return": src.count("krx_adj"), "months_hybrid": src.count("hybrid"),
            "months_close_fallback": src.count("close"),
            "base_price_match_pct_avg": round(float(np.mean(bm)), 1) if bm else None,
            "krx_return_used_pct_avg": round(float(np.mean(ku)), 1) if ku else None,
            "start_date_mode": _chg_mode["mode"],
            "months_with_fundamentals": sum(1 for m in metas if m.get("fund_ok")),
            "avg_listed": int(round(float(np.mean([m.get("n", 0) for m in metas])))) if metas else None,
            "note": "krx_adjusted: KRX 수정주가 수익률 전부 사용. hybrid: 기준가가 맞는 종목만 KRX, 나머지 종가 기반. close_fallback: 등락률 조회 실패."}


DEFAULT_COST_PCT = 0.5          # 왕복 거래비용(%): 수수료·거래세·호가 영향 합산 가정
DEFAULT_DELIST_PCT = -50.0      # 다음 달 스냅샷에서 사라진 종목(상장폐지·합병)에 부여하는 그 달 수익률(%)


def run_backtest(factors: Any = DEFAULT_FACTORS, min_cap_억: float = 1000, years: float = 10, top_pct: float = 10,
                 top_n: int = 0, hold_months: int = 1, exclude_managed: bool = True, min_price: float = 1000,
                 include_single_factor_table: bool = True, max_months_listed: int = 130,
                 cost_pct: float = DEFAULT_COST_PCT, delist_return_pct: float | None = DEFAULT_DELIST_PCT,
                 exclude_suspended: bool = True) -> dict[str, Any]:
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
    mask = _universe_mask(P, min_cap, exclude_managed, float(min_price), exclude_suspended)
    ret, vanished = _apply_delisting(P, delist_return_pct)     # 사라진 종목 → 그 달 수익률 delist_return_pct
    fwd = ret.shift(-1)                      # 형성월 t → 다음 한 달 수익률
    mask_f = mask & fwd.notna()              # 유니버스: 다음 달 수익률이 있는 종목(상폐 규칙 적용분 포함)
    row_ok = np.zeros(len(dates), dtype=bool)
    row_ok[WARMUP_MONTHS - 1:-1] = True      # 형성 가능 월: 준비기간 이후 ~ 마지막 바로 앞
    mask_f.loc[~row_ok, :] = False
    van_uni = (vanished & mask_f).sum(axis=1)     # 형성월별 유니버스 내 '다음 달 사라진' 종목 수

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
    turn = _turnover(sel, k)                                          # 형성월 기준 편도 교체율
    cost_series = _to_return_month(turn * (float(cost_pct) / 100.0), dates, 1)   # 교체분에 왕복 비용
    nw = k - 1
    st = _series_stats(port, uni_series, port_idx, nw_lags=nw, cost=cost_series)
    st["avg_monthly_turnover_pct"] = round(float(turn.dropna().mean()) * 100, 1) if turn.notna().any() else None
    st["delisted_in_portfolio_total"] = int((vanished & sel).to_numpy().sum())
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
    turn30 = _turnover(sel30, k)
    st30 = _series_stats(p30, uni_series, i30, nw_lags=nw, cost=_to_return_month(turn30 * (float(cost_pct) / 100.0), dates, 1))
    st30["avg_monthly_turnover_pct"] = round(float(turn30.dropna().mean()) * 100, 1) if turn30.notna().any() else None

    out: dict[str, Any] = {
        "ok": True,
        "method": {"방식": "매월 말 유니버스 전종목을 팩터 백분위 순위로 점수화 → 상위 포트폴리오(동일가중) → 다음 달 수익률. "
                          "유니버스 동일가중 평균·종목별 시장 지수(KOSPI/KOSDAQ)와 비교. 학계 팩터 연구와 같은 횡단면 순위·월간 리밸런싱.",
                   "팩터": {n: (" + ".join(GROUP_FACTORS[n]) + " 순위 평균" if n in GROUP_FACTORS else ATOMIC_FACTORS[n][1]) for n in names},
                   "복합점수": "각 팩터 백분위 순위의 평균(팩터 2/3 이상 있는 종목만) → 다시 백분위(100 = 최상위)",
                   "상위_규칙": top_rule, "보유_개월": k, "리밸런싱": "월말",
                   "유니버스": f"보통주(우선주·스팩·코넥스 제외){', 관리·환기종목 제외' if exclude_managed else ''}"
                               f"{', 거래정지(월 거래량 0) 제외' if exclude_suspended else ''}, "
                               f"시총 {float(min_cap_억):g}억 이상, 주가 {float(min_price):g}원 이상",
                   "상장폐지_규칙": (f"다음 달 스냅샷에서 사라진 종목은 그 달 수익률 {float(delist_return_pct):g}% 로 간주(유니버스·포트 동일)"
                                   if delist_return_pct is not None else "미적용(사라진 종목 제외)"),
                   "거래비용": f"왕복 {float(cost_pct):g}% × 월 교체율 → cost 항목(비용 후 지표). 판정 기준 ①~⑤는 비용 전",
                   "t_통계량": f"Newey-West 시차 {nw}" if nw > 0 else "단순"},
        "period": {"formation_from": dates[WARMUP_MONTHS - 1], "formation_to": dates[-2], "return_months": st.get("months"),
                   "snapshots": len(dates), "data_from": dates[0], "data_to": dates[-1],
                   "avg_universe_size": int(round(float(uni_n[row_ok].mean()))) if bool(row_ok.any()) else None,
                   "delisted_in_universe_total": int(van_uni[row_ok].sum()),
                   "delisted_in_universe_per_month": round(float(van_uni[row_ok].mean()), 2) if bool(row_ok.any()) else None},
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
        "다음 달 스냅샷에서 사라진 종목(상장폐지·합병·거래정지 후 폐지)은 그 달 수익률을 delist_return_pct 로 간주(사유 구분 불가 — 합병은 손실이 아닐 수 있고 부실 폐지는 더 클 수 있음). 민감도는 0 / -50 / -90 으로 비교할 것",
        "PER·PBR·EPS·BPS·배당은 KRX 가 해당 월말에 공표한 값(최근 사업보고서 기준) → 미래 정보 없음. 적자·미공표 EPS 는 0 으로 들어옴",
        "저변동성은 월간 수익률 12개 기준(학계는 일간 기준도 사용). 회전율은 월간 거래대금/시총",
        "월말 종가 매수·다음 월말 종가 매도, 동일가중. 비용은 '왕복 cost_pct × 월 교체율'로 근사(호가 영향·세금 변동 미반영). 수익률은 배당 제외(지수도 동일)",
        "보유기간이 1개월을 넘으면 월간 수익률이 겹쳐 자기상관이 생기므로 t 는 Newey-West 보정값",
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
        chg, _pct, _start = _fetch_change_verified(prev_ymd, ymd, prev_df, prices.index)
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

    # 요청한 팩터에 포함된 추가 신호(signals.py 등록분)의 원값도 함께 보여준다
    atoms: list[str] = []
    for n in names_f:
        for a in (GROUP_FACTORS[n] if n in GROUP_FACTORS else [n]):
            if a in EXTRA_FACTORS and a not in atoms:
                atoms.append(a)
    rows = []
    for t in row.index[: int(top_n)]:
        item = {
            "code": t, "name": names.get(t) or fallback_names.get(t, t),
            "market": "KOSDAQ" if L["kosdaq"].get(t, 0) > 0 else "KOSPI",
            "close": _v(L["close"], t, 0), "cap_억": _v(L["cap"], t, 0, 1e-8),
            "composite_pct": round(float(row[t]) * 100, 1),
            "ranks_pct": {n: _v(RL[n], t, 1, 100) for n in names_f},
            "PBR": _v(L["pbr"], t), "PER": _v(L["per"], t, 1), "DIV_pct": _v(L["div"], t),
            "ROE_pct": _v(FL["roe"], t, 1, 100), "mom_12_1_pct": _v(FL["mom"], t, 1, 100),
            "ret_1m_pct": _v(FL["rev"], t, 1, 100), "vol_12m_pct": _v(FL["lowvol"], t, 1, 100),
            "high52_pct": _v(FL["high52"], t, 1, 100), "turnover_m_pct": _v(FL["lowturn"], t, 1, 100),
        }
        if atoms:
            item["signals"] = {a: _v(FL[a], t, 4) for a in atoms if a in FL}
        rows.append(item)
    return {"ok": True, "as_of": f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}",
            "method": {"팩터": names_f, "복합점수": "팩터 백분위 순위 평균 → 백분위(100 = 최상위)",
                       "유니버스": f"보통주(우선주·스팩·코넥스 제외){', 관리·환기종목 제외' if exclude_managed else ''}, "
                                   f"시총 {float(min_cap_억):g}억 이상, 주가 {float(min_price):g}원 이상",
                       "참고": "ret_1m 은 직전 월말→최근 거래일(월중 부분) 수익률. 월말 리밸런싱·1개월 보유 전제의 점수"},
            "universe_size": uni_n, "scored_count": int(len(row)), "top": rows,
            "elapsed_sec": round(time.time() - t0, 1),
            "note": "사실 데이터 집계이며 매매 권유가 아닙니다. 백테스트(backtest_factors)에서 기준을 통과한 팩터 조합만 쓰세요.",
            "source": "KRX via pykrx"}


# ────────────────────────────────────────────────────────────
# 표본 외(paper) 검증 — 사전 등록된 규칙. 이 블록은 수정하지 않습니다.
#
#   표본 내(2016-11~2026-10) 검증을 마친 뒤 2026-11-07 에 규칙을 고정했습니다. GitHub 커밋 시각이 등록 증거입니다.
#   이후 월말마다 같은 규칙으로 30종목을 편입해 12개월 보유(코호트 겹침)하고, 2028-10 수익률월까지 24개월의
#   표본 외 성과를 모아 판정합니다. 규칙·유니버스·판정 기준을 바꾸면 표본 외 검증의 의미가 사라집니다.
# ────────────────────────────────────────────────────────────
REGISTRATION: dict[str, Any] = {
    "name": "저PBR 상위 30종목 · 12개월 보유 (표본 외 검증)",
    "registered_on": "2026-11-07",
    "rule": {
        "signal": "KRX 공표 PBR 이 낮은 순(= 1/PBR 높은 순, PBR>0) 상위 30종목. 동점은 종목코드 순",
        "universe": "보통주(우선주·스팩·코넥스 제외), 관리·환기종목 제외, 시가총액 1,000억 이상, 주가 1,000원 이상",
        "formation": "매월 마지막 거래일 종가 기준으로 편입, 동일가중",
        "holding": "12개월 보유 후 교체. 매월 새 코호트가 생겨 최대 12개 코호트를 동시에 보유(코호트 안에서는 월간 동일가중 재조정 가정)",
        "benchmark": "같은 달 유니버스 동일가중 평균(주 비교 대상), 종목별 시장 지수(KOSPI/KOSDAQ) 참고",
        "costs": "거래비용·세금 미반영(표본 내와 동일 기준으로 비교하기 위함)",
    },
    "params": {"factors": "bm", "top_n": 30, "hold_months": 12, "min_cap_eok": 1000, "min_price": 1000, "exclude_managed": True},
    "oos_first_formation": "20261030",      # 이 월말부터 형성된 코호트의 수익률이 표본 외
    "oos_first_return_month": "2026-11",
    "verdict_month": "2028-10",             # 24개월(2026-11 ~ 2028-10)
    "verdict_rule": "표본 외 24개월: 월평균 초과수익(유니버스 대비) +0.3%p 이상 그리고 월 적중률 55% 이상 → 통과 / "
                    "월평균 초과수익 0 이하 → 기각 / 그 사이 → 12개월 연장 후 재판정",
    "in_sample_reference": {"period": "2016-11 ~ 2026-10 (120개월, 표본 내)", "avg_month_pct": 0.94, "universe_avg_month_pct": 0.33,
                            "excess_pp": 0.61, "t": 1.77, "hit_pct": 53.3, "first_half_pp": 0.47, "second_half_pp": 0.76,
                            "cagr_pct": 9.0, "universe_cagr_pct": 1.4, "index_cagr_pct": 9.7, "max_drawdown_pct": -47.9,
                            "criteria_passed": "3/5 (①④⑤ 통과, ② t 1.64·③ 적중률 51.7% 미달 — 상위 10% 기준)"},
    "in_sample_window": ("20161031", "20260930"),   # 재현 검사용 형성월 범위
    "amendments": [
        "2026-11-11 (v1.4, 계산 방법만 개정 — 종목 선정 규칙은 그대로): ① 다음 달 사라진 종목(상장폐지·합병) 수익률 −50% 간주, "
        "② 월 거래량 0(거래정지) 종목 편입 제외, ③ 왕복 비용 0.5% × 월 교체율(12개월 보유 → 월 약 0.04%p)을 뺀 비용 후 지표 병기, "
        "④ 겹치는 코호트 보정 Newey-West t(시차 11). 등록 참고치(+0.61%p·t 1.77)는 구방법 값이며, 새 방법의 기준값은 "
        "in_sample_reproduction 에 매번 재계산해 표시. 판정 규칙(월평균 초과 +0.3%p·적중률 55%)은 비용 전 수치로 유지",
    ],
}


def _overlap_port(sel: pd.DataFrame, ret: pd.DataFrame, mask: pd.DataFrame, dates: list[str], k: int,
                  f_from: str, f_to: str, kflag: np.ndarray, kospi_m: pd.Series, kosdaq_m: pd.Series) -> list[list[Any]]:
    """형성월 f_from~f_to 의 코호트(sel 행)를 k 개월 보유했을 때의 월별 [수익률월, 코호트수, 포트, 유니버스, 지수, 초과](소수)."""
    acc: dict[str, list[float]] = {}
    acc_idx: dict[str, list[float]] = {}
    pos = {d: i for i, d in enumerate(dates)}
    for d in dates:
        if d < f_from or d > f_to:
            continue
        i = pos[d]
        row = sel.loc[d].to_numpy()
        if not row.any():
            continue
        for j in range(1, k + 1):
            if i + j >= len(dates):
                break
            m = dates[i + j]
            x = ret.loc[m].to_numpy()[row]
            ok = ~np.isnan(x)
            if not ok.any():
                continue
            acc.setdefault(m, []).append(float(x[ok].mean()))
            im = np.where(kflag[row], kosdaq_m.get(m, np.nan), kospi_m.get(m, np.nan))
            acc_idx.setdefault(m, []).append(float(np.nanmean(im)) if np.isfinite(im).any() else np.nan)
    rows = []
    for m in sorted(acc):
        i = pos[m]
        if i == 0:
            continue
        u = ret.loc[m].where(mask.loc[dates[i - 1]])
        uni = float(u.mean()) if u.notna().any() else np.nan
        port = float(np.mean(acc[m]))
        idx = float(np.nanmean(acc_idx[m])) if acc_idx.get(m) and np.isfinite(acc_idx[m]).any() else np.nan
        rows.append([m, len(acc[m]), port, uni, idx, port - uni])
    return rows


def _rows_summary(rows: list[list[Any]], nw_lags: int = 0, month_cost: float = 0.0) -> dict[str, Any]:
    if not rows:
        return {"months": 0}
    port = pd.Series([r[2] for r in rows], dtype=float)
    uni = pd.Series([r[3] for r in rows], dtype=float)
    idx = pd.Series([r[4] for r in rows], dtype=float)
    ex = port - uni
    out = {"months": int(len(rows)), "avg_month_pct": round(float(port.mean()) * 100, 2),
           "universe_avg_month_pct": round(float(uni.mean()) * 100, 2),
           "excess_vs_universe_pp": round(float(ex.mean()) * 100, 2), "excess_t_stat": _tstat_nw(ex, nw_lags),
           "excess_t_stat_plain": _tstat(ex),
           "hit_rate_vs_universe_pct": round(float((ex > 0).mean()) * 100, 1),
           "cum_port_pct": round((float(np.prod(1 + port)) - 1) * 100, 1),
           "cum_universe_pct": round((float(np.prod(1 + uni)) - 1) * 100, 1)}
    if idx.notna().any():
        out["cum_index_pct"] = round((float(np.prod(1 + idx.dropna())) - 1) * 100, 1)
        out["excess_vs_index_pp"] = round(float((port - idx).dropna().mean()) * 100, 2)
    if month_cost:
        pn = port - month_cost
        out["cost"] = {"month_cost_pp": round(month_cost * 100, 3), "net_excess_vs_universe_pp": round(float((pn - uni).mean()) * 100, 2),
                       "net_cum_port_pct": round((float(np.prod(1 + pn)) - 1) * 100, 1)}
    return out


def run_paper_test(show_all_cohorts: bool = False) -> dict[str, Any]:
    """사전 등록된 규칙(REGISTRATION)대로 표본 외 코호트를 계산해 보고한다. 규칙 파라미터는 받지 않는다."""
    t0 = time.time()
    reg = REGISTRATION
    prm = reg["params"]
    if _state["running"]:
        st = dict(_state)
        return {"ok": False, "status": "preparing", "message": f"월말 데이터를 받는 중입니다 ({st['done']}/{st['target']}). 몇 분 뒤 다시 실행하세요.",
                "registration": reg}
    # 직전 완료 달의 월말이 아직 없으면 받아온다(월초 자동 실행 대비, KRX 3회 + 지수 1회)
    y, m = _last_complete_month()
    me = _month_end.get((y, m), "?")
    if me == "?" or (me is not None and me not in _snaps):
        try:
            ensure_months(int(round(DEFAULT_YEARS * 12)) + WARMUP_MONTHS)
        except Exception as e:  # noqa: BLE001
            log.warning("paper_test: 최신 월말 수집 실패: %s", e)
    with _lock:
        dates = sorted(_snaps)
    if len(dates) < 2:
        return {"ok": False, "error": "월말 데이터가 없습니다. factor_cache_status 확인.", "registration": reg}
    P = _panels(dates)
    mask = _universe_mask(P, float(prm["min_cap_eok"]) * 1e8, bool(prm["exclude_managed"]), float(prm["min_price"]), True)
    ret, _van = _apply_delisting(P, DEFAULT_DELIST_PCT)
    bm = 1.0 / P["pbr"].where(P["pbr"] > 0)
    order = (-bm.where(mask)).rank(axis=1, method="first")
    sel = (order <= int(prm["top_n"])) & mask
    kospi_m, kosdaq_m = _index_monthly(dates)
    kflag = (P["kosdaq"].ffill().iloc[-1] > 0).to_numpy()      # 종목별 시장 구분(최신 행 기준; 이전·상폐 종목은 마지막 값)
    k = int(prm["hold_months"])
    month_cost = (DEFAULT_COST_PCT / 100.0) / k                  # 매달 1/k 코호트 교체 → 왕복 비용의 1/k

    oos_rows = _overlap_port(sel, ret, mask, dates, k, reg["oos_first_formation"], dates[-1], kflag, kospi_m, kosdaq_m)
    oos = _rows_summary(oos_rows, nw_lags=k - 1, month_cost=month_cost)
    f0, f1 = reg["in_sample_window"]
    ins_rows = _overlap_port(sel, ret, mask, dates, k, f0, f1, kflag, kospi_m, kosdaq_m)
    ins_rows = [r for r in ins_rows if r[0] <= "20261030"]            # 표본 내 수익률월은 2026-10 까지
    ins = _rows_summary(ins_rows, nw_lags=k - 1, month_cost=month_cost)

    # 판정 미리보기(판정월 전에는 '진행 중')
    n = oos["months"]
    latest_ret_month = oos_rows[-1][0] if oos_rows else None
    verdict = "진행 중"
    if n:
        ex, hit = oos["excess_vs_universe_pp"], oos["hit_rate_vs_universe_pct"]
        prelim = "통과" if (ex >= 0.3 and hit >= 55) else ("기각" if ex <= 0 else "연장(12개월 후 재판정)")
        final = latest_ret_month is not None and f"{latest_ret_month[:4]}-{latest_ret_month[4:6]}" >= reg["verdict_month"]
        verdict = f"판정: {prelim}" if final else f"진행 중 {n}/24개월 (지금 규칙을 적용하면 '{prelim}' — 판정은 {reg['verdict_month']} 수익률 반영 후)"

    names = _names_latest[1] if _names_latest else {}
    cohorts_out = []
    pos = {d: i for i, d in enumerate(dates)}
    for d in dates:
        if d < reg["oos_first_formation"]:
            continue
        row = sel.loc[d]
        tick = list(row.index[row.to_numpy()])
        if not tick:
            continue
        i = pos[d]
        held = min(k, len(dates) - 1 - i)
        cum = None
        if held > 0:
            r = ret.loc[dates[i + 1]:dates[i + held], tick]
            cum = round((float(np.prod(1 + r.mean(axis=1).fillna(0))) - 1) * 100, 2)
        entry = {"formation": d, "n": len(tick), "months_held": held, "cum_return_pct": cum,
                 "exit_after": dates[i + k] if i + k < len(dates) else f"{k}개월 뒤"}
        if show_all_cohorts or d == dates[-1]:
            entry["holdings"] = [{"code": t, "name": names.get(t, t), "entry_close": _num_or_none(P["close"].loc[d, t], 0),
                                  "PBR": _num_or_none(P["pbr"].loc[d, t], 2), "cap_억": _num_or_none(P["cap"].loc[d, t], 0, 1e-8),
                                  "market": "KOSDAQ" if P["kosdaq"].loc[d, t] > 0 else "KOSPI"} for t in tick]
        else:
            entry["codes"] = tick
        cohorts_out.append(entry)

    return {
        "ok": True,
        "registration": {k2: v for k2, v in reg.items() if k2 != "in_sample_window"},
        "as_of_month_end": dates[-1],
        "status": verdict,
        "oos_summary": oos,
        "oos_monthly_columns": ["수익률월", "코호트수", "포트_pct", "유니버스_pct", "지수_pct", "초과_pp"],
        "oos_monthly": [[f"{r[0][:4]}-{r[0][4:6]}", r[1], round(r[2] * 100, 2), round(r[3] * 100, 2),
                         None if (r[4] is None or np.isnan(r[4])) else round(r[4] * 100, 2), round(r[5] * 100, 2)] for r in oos_rows],
        "cohorts": cohorts_out,
        "in_sample_reproduction": {"note": "현재 캐시로 표본 내 구간을 현행 방법(v1.4: 상폐 −50%·거래정지 제외·NW t·비용 병기)으로 다시 계산한 값. "
                                           "registration.in_sample_reference(구방법 +0.61%p·t 1.77)보다 조금 낮게 나오는 것이 정상이며, "
                                           "배포 간에 이 값이 흔들리면 데이터 이상",
                                   **ins},
        "elapsed_sec": round(time.time() - t0, 1),
        "note": "사전 등록된 규칙을 그대로 적용한 기록입니다. 규칙을 바꾸지 마세요. 과거·현재 통계이며 매매 권유가 아닙니다.",
    }


def _num_or_none(x, nd: int = 2, scale: float = 1.0):
    return None if pd.isna(x) else round(float(x) * scale, nd)


try:
    load_cache()                 # 볼륨이 있으면 지난 월말 스냅샷을 먼저 복구(없는 달만 받게 됨)
except Exception as _e:  # noqa: BLE001
    log.warning("factors 캐시 복구 중 오류(무시): %s", _e)
if os.environ.get("FACTOR_PREFETCH", "1") != "0":
    prefetch()
