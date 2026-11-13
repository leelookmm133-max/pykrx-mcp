"""
signals.py — 1단계 추가 신호(시점 고정) : DART 분기 재무 · 업종 모멘텀 · 투자자 수급 · 공매도 잔고 · 외국인 보유율.

factors.py 의 팩터 레지스트리(register_factor)에 신호를 팩터로 등록하므로, backtest_factors / screen_factors / single_factors 표에서
기존 팩터와 똑같이 쓰입니다(예: factors_spec="epm,sec_mom,flow20", hold_months=3). 평가 기준·엔진은 그대로입니다.

신호 정의 (모두 월말 기준, 그 시점에 알 수 있던 값만 사용)
  [DART 분기 재무 — 공시 접수일(rcept_no 앞 8자리) 이후의 월말부터 반영]
    epm       영업이익 증가액 / 시가총액 = (당분기 3개월 영업이익 − 전년 동분기 3개월 영업이익) / 시총   (이익 모멘텀, 높을수록 좋음)
    sales_g   매출 3개월 전년 동분기 대비 증가율
    opm_chg   영업이익률(3개월) 전년 동분기 대비 변화(%p)
    agr       자산총계 전년 동기 대비 증가율 (낮을수록 좋음 — 학계 '투자' 팩터)
    roe_ttm   최근 4분기 당기순이익 합 / 자본총계
    op_yield  최근 4분기 영업이익 합 / 시가총액 (가치)
    sec_margin 업종(KRX 업종분류) 내 opm_chg 중앙값 — 산업 마진 추세
  [업종 — KRX 업종별 분류현황(날짜별)]
    sec_mom   소속 업종 동일가중 수익률의 12-1개월 모멘텀
    rs3       종목 3개월 수익률 − 업종 3개월 수익률 (업종 내 상대강도)
  [수급 — KRX 투자자별 순매수(20거래일 창)]
    flow20    (외국인 + 기관합계) 20거래일 순매수대금 / 시총
    flow60    최근 3개 월말의 20거래일 순매수 합 / 시총
    frgn_chg  외국인 지분율 3개월 변화(%p)
  [공매도 — KRX 공매도 잔고, 공표 시차 반영]
    short_bal 공매도 잔고 비중(%) — 월말 2거래일 전 잔고(월말 시점에 공표돼 있던 최신값)  (낮을수록 좋음)
    short_chg 잔고 비중 3개월 변화(%p) (낮을수록 좋음)
  [DART 장부가]
    bm_q      최근 분기 자본총계 / 시가총액 (KRX PBR 보다 최신 장부가; 진단용 단독 팩터)
  그룹: earnings=[epm, sales_g, opm_chg]  quality2=[roe_ttm, agr]  sector=[sec_mom, rs3, sec_margin]
        flow=[flow20, flow60, frgn_chg]  shortint=[short_bal, short_chg]

자료 시점(v1.7 보정 — 편입일 종가에 실제로 알 수 있던 값만 쓰도록)
  - 공매도 잔고: KRX 는 T일 잔고를 T+2 거래일에 공표한다(실측: 11/12 밤에 조회하면 잔고는 11/10 까지). 그래서 월말 신호는
    '월말 2거래일 전 잔고'를 쓴다. v1.6 까지는 월말 당일 잔고를 써서 2거래일치 미래 정보가 섞여 있었다.
  - DART: 접수일이 편입일보다 '앞선' 보고서만 반영(접수일 당일은 장 마감 뒤 제출일 수 있어 다음 날부터). v1.6 까지는 당일 포함.
  - 분기 수집: 분기말 + 20일부터 잠정 수집(조기 제출 회사 반영), 제출기한 + 10일 뒤에 받은 것은 확정. 잠정 분기는 하루(기한 3주 전까지는
    7일)에 한 번 다시 받고, 등록 전략 편입 때는 편입일 이후에 받은 자료가 있어야 한다(없으면 다시 받는 동안 status=preparing).
    과거 백테스트는 모든 보고서를 실제 접수일로 쓰므로, 실시간 수집도 같은 정보를 보도록 맞춘 것이다.

데이터 수집 (서버 시작 시 factors 의 월말 수집이 끝난 뒤 백그라운드로)
  - 월말당 KRX 9회: 업종분류 2(KOSPI·KOSDAQ) + 순매수 4(2시장×외국인·기관) + 공매도잔고(T−2) 2 + 외국인보유 1  → 10년 ≈ 1,200회(약 40분)
    (v1.7 로 올린 직후 한 번, 기존 월말들의 T−2 잔고를 추가로 받는다: 약 250회, 10~15분)
  - DART: 고유번호 목록 1회 + 다중회사 주요계정(fnlttMultiAcnt, 회사 100개씩) 분기마다 약 29회(어느 월말이든 시총 DART_MIN_CAP_EOK=300억
    이상이었던 보통주 약 2,900개). DART 가 키당 분당 4~6회 정도로 응답해 분기 하나에 약 6분. 4개 분기마다 중간 저장(GitHub Release)해
    재배포로 끊겨도 이어받음.
    DART_API_KEY 환경변수가 없으면 DART 신호는 비어 있고(NaN) 나머지는 정상 동작.
  - 메모리: 월말 신호 1개 ≈ 0.15MB, DART 분기 1개 ≈ 0.4MB → 10년 합계 약 40MB.
  - 공매도 잔고는 2016-06 이후. 금지 구간(전면 2020-03-16~2021-05-02, 2023-11-06~2025-03-30 / 부분 허용 2021-05-03~2023-11-05)에는
    잔고가 줄어 변화량 해석에 주의 — backtest_factors(split="short_ban") 로 구간별 성과를 나눠 본다.

결과는 과거 통계이며 매매 권유가 아닙니다.
"""
from __future__ import annotations

import io
import logging
import os
import re
import threading
import time
import zipfile
from datetime import datetime, timedelta
from typing import Any
from xml.etree import ElementTree

import numpy as np
import pandas as pd
import requests
from pykrx import stock

import factors
import screener
from screener import FETCH_DELAY, KST, _intern, _malloc_trim, _retry, _rss_mb

try:
    import cachestore
except Exception:  # noqa: BLE001
    cachestore = None

log = logging.getLogger("signals")
CACHE_NAME = "signals"

DART_KEY = os.environ.get("DART_API_KEY", "").strip()
DART_BASE = "https://opendart.fss.or.kr/api"
DART_DELAY = float(os.environ.get("DART_FETCH_DELAY", "0.2"))
SIGNALS_YEARS = float(os.environ.get("SIGNALS_YEARS", str(factors.DEFAULT_YEARS)))

# ────────────────────────────────────────────────────────────
# 캐시
# ────────────────────────────────────────────────────────────
_lock = threading.Lock()
SIG_COLS = ["sector", "frgn20", "inst20", "short_bal", "frgn_hold"]
_sig: dict[str, pd.DataFrame] = {}             # 월말 -> DataFrame(index=ticker, SIG_COLS float; sector 는 코드)
_sector_code: dict[str, int] = {}              # 업종명 -> 코드
_sector_name: dict[int, str] = {}
_sig_meta: dict[str, dict[str, Any]] = {}
_dart_raw: dict[str, pd.DataFrame] = {}        # "2023Q1" -> 분기 주요계정 (index=stock_code)
_dart_meta: dict[str, dict[str, Any]] = {}     # "2023Q1" -> {"n", "fetched_at"(YYYYMMDD), "fetched_ts", "provisional"}
_dart_fail: dict[str, float] = {}              # "2023Q1" -> 마지막 수집 실패 시각(time.time) — DART_RETRY_SEC 동안 다시 시도하지 않음
_corp_map: dict[str, str] = {}                 # stock_code -> corp_code
# 공매도 잔고(공표 시차 반영): 월말 -> 그 월말에 공표돼 있던 최신 잔고 비중(= 월말 SHORT_LAG_DAYS 거래일 전 잔고)
_short_l2: dict[str, pd.Series] = {}
_short_l2_meta: dict[str, dict[str, Any]] = {}  # 월말 -> {"date": 잔고 기준일, "n": 종목 수, "tried": 시도 시각}
_state: dict[str, Any] = {"running": False, "sig_done": 0, "sig_target": 0, "dart_done": 0, "dart_target": 0,
                          "last_error": None, "dart_status": "no_key" if not DART_KEY else "idle",
                          "started_at": None, "finished_at": None, "short_lag_done": 0, "short_lag_target": 0}

SHORT_LAG_DAYS = int(os.environ.get("SHORT_LAG_DAYS", "2"))     # KRX 공매도 잔고 공표 시차(거래일): T일 잔고는 T+2일에 공표
SHORT_DATA_FROM = "20160630"                                    # 공매도 잔고 공시 시작(2016-06-30) 이전 월말은 받지 않는다
REPRT = {"Q1": "11013", "Q2": "11012", "Q3": "11014", "Q4": "11011"}     # Q4 는 사업보고서(연간)에서 도출
# 보고서 법정 제출기한 다음 날: 1분기 5/15, 반기 8/14, 3분기 11/14, 사업보고서 다음해 3/31(주말이면 다음 영업일로 밀린다)
_DEADLINE = {"Q1": (5, 16), "Q2": (8, 15), "Q3": (11, 15), "Q4": (3, 31)}
_QEND = {"Q1": (3, 31), "Q2": (6, 30), "Q3": (9, 30), "Q4": (12, 31)}
DART_EARLY_DAYS = int(os.environ.get("DART_EARLY_DAYS", "20"))   # 분기말 + 20일부터 잠정 수집(조기 제출 회사 반영)
DART_FINAL_DAYS = int(os.environ.get("DART_FINAL_DAYS", "10"))   # 제출기한 + 10일 뒤에 받은 분기는 확정(다시 받지 않음)
DART_RETRY_SEC = 600                                             # 수집 실패 후 다시 시도하기까지의 최소 간격(초)
ACCOUNT_PATTERNS = {
    "sales": ("매출액", "수익(매출액)", "영업수익", "매출"),
    "op": ("영업이익", "영업이익(손실)", "영업손익"),
    "ni": ("당기순이익", "당기순이익(손실)", "당기순손익", "분기순이익", "분기순이익(손실)", "반기순이익", "반기순이익(손실)"),
    "assets": ("자산총계",),
    "equity": ("자본총계",),
}


def _today() -> datetime:
    return datetime.now(KST)


def _ymd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def _num(s: pd.Series) -> pd.Series:
    if s.dtype.kind in "fiu":
        return s.astype(float)
    t = s.astype(str).str.replace(",", "", regex=False).str.strip()
    t = t.where(~t.isin(["", "-", "nan", "None"]), other="nan")
    return pd.to_numeric(t, errors="coerce")


def _sleep() -> None:
    if FETCH_DELAY > 0:
        time.sleep(FETCH_DELAY)


# ────────────────────────────────────────────────────────────
# KRX 월말 신호
# ────────────────────────────────────────────────────────────
def _window_start(ymd: str, n_days: int) -> str:
    """ymd 를 포함해 n_days 거래일 창의 시작일(달력이 있으면 정확히, 없으면 달력일 기준 근사)."""
    days = factors._calendar.get("days") or []
    if days:
        arr = np.array(days)
        i = int(np.searchsorted(arr, ymd, side="right")) - 1
        if i >= 0:
            j = max(0, i - (n_days - 1))
            return days[j]
    return _ymd(datetime.strptime(ymd, "%Y%m%d") - timedelta(days=int(n_days * 1.45) + 2))


def _sector_frame(ymd: str) -> pd.Series:
    """티커 -> 업종코드(int). 실패 시 빈 Series."""
    out: dict[str, int] = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        try:
            df = _retry(stock.get_market_sector_classifications, ymd, mkt, tries=2)
        except Exception as e:  # noqa: BLE001
            log.warning("업종분류 실패 %s/%s: %s", ymd, mkt, e)
            df = None
        _sleep()
        if df is None or df.empty or "업종명" not in df.columns:
            continue
        for t, name in zip(df.index.astype(str), df["업종명"].astype(str)):
            key = f"{mkt}:{name.strip()}"              # KOSPI 와 KOSDAQ 업종명이 같아도 구분
            if key not in _sector_code:
                code = len(_sector_code) + 1
                _sector_code[key] = code
                _sector_name[code] = key
            out[t] = _sector_code[key]
    return pd.Series(out, dtype="float64")


def _net_frame(start: str, end: str) -> tuple[pd.Series, pd.Series]:
    """외국인·기관합계 순매수대금(원), 티커별. 실패한 시장은 빠짐."""
    fn = getattr(stock, "get_market_net_purchases_of_equities", None) or stock.get_market_net_purchases_of_equities_by_ticker
    frgn: dict[str, float] = {}
    inst: dict[str, float] = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        for inv, bucket in (("외국인", frgn), ("기관합계", inst)):
            try:
                df = _retry(fn, start, end, mkt, inv, tries=2)
            except Exception as e:  # noqa: BLE001
                log.warning("순매수 실패 %s~%s %s/%s: %s", start, end, mkt, inv, e)
                df = None
            _sleep()
            if df is None or df.empty:
                continue
            col = "순매수거래대금" if "순매수거래대금" in df.columns else df.columns[-1]
            vals = _num(df[col])
            for t, v in zip(df.index.astype(str), vals.values):
                if not np.isnan(v):
                    bucket[t] = float(v)
    return pd.Series(frgn, dtype="float64"), pd.Series(inst, dtype="float64")


def _short_frame(ymd: str) -> pd.Series:
    out: dict[str, float] = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        try:
            df = _retry(stock.get_shorting_balance_by_ticker, ymd, mkt, tries=2)
        except Exception as e:  # noqa: BLE001
            log.warning("공매도잔고 실패 %s/%s: %s", ymd, mkt, e)
            df = None
        _sleep()
        if df is None or df.empty or "비중" not in df.columns:
            continue
        vals = _num(df["비중"])
        for t, v in zip(df.index.astype(str), vals.values):
            if not np.isnan(v):
                out[t] = float(v)
    return pd.Series(out, dtype="float64")


def _frgn_hold_frame(ymd: str) -> pd.Series:
    fn = getattr(stock, "get_exhaustion_rates_of_foreign_investment", None) \
        or stock.get_exhaustion_rates_of_foreign_investment_by_ticker
    df = None
    try:
        df = _retry(fn, ymd, "ALL", tries=2)
    except TypeError:
        try:
            df = _retry(fn, ymd, market="ALL", tries=2)
        except Exception as e:  # noqa: BLE001
            log.warning("외국인보유 실패 %s: %s", ymd, e)
    except Exception as e:  # noqa: BLE001
        log.warning("외국인보유 실패 %s: %s", ymd, e)
    _sleep()
    if df is None or df.empty or "지분율" not in df.columns:
        return pd.Series(dtype="float64")
    vals = _num(df["지분율"])
    return pd.Series({str(t): float(v) for t, v in zip(df.index, vals.values) if not np.isnan(v)}, dtype="float64")


def _lag_date(ymd: str, n: int) -> str:
    """ymd(거래일)에서 n 거래일 전 날짜. 달력이 없으면 주말만 건너뛰어 근사."""
    days = factors._calendar.get("days") or []
    if days:
        arr = np.array(days)
        i = int(np.searchsorted(arr, ymd, side="right")) - 1          # ymd 이하 마지막 거래일
        if i - n >= 0:
            return days[i - n]
    d = datetime.strptime(ymd, "%Y%m%d")
    k = 0
    while k < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            k += 1
    return _ymd(d)


def fetch_short_lag(ymd: str) -> int:
    """월말 ymd 시점에 공표돼 있던 최신 공매도 잔고(= ymd 의 SHORT_LAG_DAYS 거래일 전 잔고)를 받아 _short_l2 에 넣는다.
    받은 종목 수를 돌려준다(0 이면 KRX 가 아직 공표 전이거나 조회 실패 — 나중에 다시 시도)."""
    lag = _lag_date(ymd, SHORT_LAG_DAYS)
    s = _short_frame(lag)
    n = int(s.notna().sum()) if len(s) else 0
    with _lock:
        if n > 0:
            _short_l2[ymd] = pd.Series(np.ascontiguousarray(s.to_numpy(dtype=float)),
                                       index=pd.Index([_intern(t) for t in s.index], dtype=object, name="ticker"))
        _short_l2_meta[ymd] = {"date": lag, "n": n, "tried": time.time()}
    _short_cache.clear()
    return n


def fetch_month(ymd: str) -> pd.DataFrame:
    """월말 하나의 KRX 신호 표(SIG_COLS). 공매도 잔고는 공표 시차를 반영해 따로(_short_l2) 받고, 표의 short_bal 열(월말 당일 잔고)은
    v1.7 부터 새 월말에는 채우지 않는다(월초에는 아직 공표 전이고, 팩터도 더는 쓰지 않음)."""
    sector = _sector_frame(ymd)
    start20 = _window_start(ymd, 20)
    frgn, inst = _net_frame(start20, ymd)
    short = pd.Series(dtype="float64")
    if ymd >= SHORT_DATA_FROM:
        try:
            fetch_short_lag(ymd)
        except Exception as e:  # noqa: BLE001
            log.warning("공매도 잔고(T-%d) 수집 실패 %s: %s", SHORT_LAG_DAYS, ymd, e)
    hold = _frgn_hold_frame(ymd)
    idx = sorted(set(sector.index) | set(frgn.index) | set(inst.index) | set(short.index) | set(hold.index))
    df = pd.DataFrame({"sector": sector.reindex(idx), "frgn20": frgn.reindex(idx), "inst20": inst.reindex(idx),
                       "short_bal": short.reindex(idx), "frgn_hold": hold.reindex(idx)})
    data = {c: np.ascontiguousarray(df[c].to_numpy(dtype=float)) for c in SIG_COLS}
    out = pd.DataFrame(data, index=pd.Index([_intern(t) for t in idx], dtype=object, name="ticker"))
    meta = {"n": int(len(out)), "sector_n": int(sector.notna().sum()), "flow_n": int(frgn.notna().sum()),
            "short_n": int(short.notna().sum()), "hold_n": int(hold.notna().sum()), "flow_window": f"{start20}~{ymd}"}
    with _lock:
        _sig[ymd] = out
        _sig_meta[ymd] = meta
    _malloc_trim()
    return out


# ────────────────────────────────────────────────────────────
# DART 분기 재무 (다중회사 주요계정)
# ────────────────────────────────────────────────────────────
def _redact(s: Any) -> str:
    """오류 문구에서 DART 인증키를 가린다(상태 조회 결과에 키가 노출되지 않게)."""
    t = str(s)
    return t.replace(DART_KEY, "***") if DART_KEY else t


def _dart_get(endpoint: str, params: dict[str, Any], raw: bool = False):
    """DART OpenAPI 호출. raw=True 면 bytes(zip 등), 아니면 JSON dict. 실패 시 None."""
    if not DART_KEY:
        return None
    url = f"{DART_BASE}/{endpoint}"
    q = {"crtfc_key": DART_KEY, **params}
    last = None
    for i in range(3):
        try:
            r = requests.get(url, params=q, timeout=40)
            if r.status_code != 200:
                last = f"HTTP {r.status_code}"
            elif raw:
                return r.content
            else:
                js = r.json()
                st = str(js.get("status", ""))
                if st in ("000", "013"):          # 013: 조회된 데이터 없음(정상)
                    return js
                last = f"DART status {st}: {js.get('message')}"
                if st in ("010", "011", "020", "021"):   # 키 오류·한도 초과 → 재시도 무의미
                    break
        except Exception as e:  # noqa: BLE001
            last = f"{type(e).__name__}: {e}"
        time.sleep(1.5 * (i + 1))
    last = _redact(last)                      # requests 예외 문구에는 인증키가 든 전체 URL 이 들어갈 수 있다 → 가린다
    _state["last_error"] = f"{endpoint}: {last}"
    log.warning("DART 호출 실패 %s %s: %s", endpoint, {k: v for k, v in params.items() if k != 'corp_code'}, last)
    return None


_corp_map_at: dict[str, Any] = {"ymd": None}      # 고유번호 목록을 받은 날(YYYYMMDD)
CORP_MAP_REFRESH_DAYS = 30                          # 신규 상장사를 반영하려고 30일마다 다시 받는다


def _corp_map_fresh() -> bool:
    at = _corp_map_at.get("ymd")
    if at is None:                           # 받은 날을 모르는 옛 캐시: 지금 받은 것으로 치고 30일 뒤에 갱신
        _corp_map_at["ymd"] = _ymd(_today())
        return True
    try:
        return (_naive() - datetime.strptime(str(at), "%Y%m%d")).days < CORP_MAP_REFRESH_DAYS
    except Exception:  # noqa: BLE001
        return False


def load_corp_map(force: bool = False) -> dict[str, str]:
    """상장사 종목코드 -> DART 고유번호. 캐시가 CORP_MAP_REFRESH_DAYS 일보다 오래됐으면 다시 받는다(실패하면 옛 목록 유지)."""
    global _corp_map
    if _corp_map and not force and _corp_map_fresh():
        return _corp_map
    old = _corp_map
    content = _dart_get("corpCode.xml", {}, raw=True)
    if not content:
        return old
    out: dict[str, str] = {}
    if not content[:2] == b"PK":            # zip 이 아니면 DART 가 오류 본문(XML/JSON)을 보낸 것 — 키 오류 등
        snippet = content[:300].decode("utf-8", "ignore")
        m = re.search(r"<status>(\d+)</status>.*?<message>(.*?)</message>", snippet, re.S)
        msg = f"status {m.group(1)}: {m.group(2)}" if m else snippet.strip()
        _state["last_error"] = f"corpCode.xml 응답이 zip 이 아님 → {msg}"
        log.warning("DART corpCode 오류: %s", msg)
        return old
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            name = next((n for n in zf.namelist() if n.lower().endswith(".xml")), None)
            if name is None:
                return old
            root = ElementTree.fromstring(zf.read(name))
        for el in root.iter("list"):
            sc = (el.findtext("stock_code") or "").strip()
            cc = (el.findtext("corp_code") or "").strip()
            if len(sc) == 6 and cc:
                out[sc] = cc
    except Exception as e:  # noqa: BLE001
        _state["last_error"] = f"corpCode 파싱 실패: {e}"
        log.warning("corpCode 파싱 실패: %s", e)
        return old
    if not out:
        return old
    _corp_map = out
    _corp_map_at["ymd"] = _ymd(_today())
    return out


def _match_account(name: str) -> str | None:
    n = re.sub(r"\s+", "", str(name))
    for key, pats in ACCOUNT_PATTERNS.items():
        for p in pats:
            if n == p.replace(" ", ""):
                return key
    # 느슨한 매칭(괄호 변형 등)
    if n.startswith("매출액") or n.startswith("수익(매출액)"):
        return "sales"
    if n.startswith("영업이익") or n.startswith("영업손실"):
        return "op"
    if n.startswith("당기순이익") or n.startswith("당기순손실") or n.startswith("분기순이익") or n.startswith("반기순이익"):
        return "ni"
    return None


DART_WORKERS = int(os.environ.get("DART_WORKERS", "2"))     # fnlttMultiAcnt 동시 호출 수(실측: 키당 분당 4~6회로 제한되는 듯해 2면 충분)


def _parse_dart_items(items: list[dict[str, Any]], rows: dict[str, dict[str, Any]], pref: dict[str, str]) -> None:
    """주요계정 응답 행들을 종목코드별 한 행(dict)으로 누적. 연결(CFS) 우선, 없으면 개별(OFS)."""
    for it in items:
        sc = str(it.get("stock_code", "")).strip()
        if len(sc) != 6:
            continue
        key = _match_account(it.get("account_nm", ""))
        if key is None:
            continue
        fs = str(it.get("fs_div", "")).upper()
        cur = pref.get(sc)
        if cur is None or (cur == "OFS" and fs == "CFS"):
            if cur == "OFS" and fs == "CFS":
                rows[sc] = {}                  # 개별로 채운 값을 버리고 연결로 다시
            pref[sc] = fs
        if fs != pref[sc]:
            continue
        row = rows.setdefault(sc, {})
        row["rcept"] = str(it.get("rcept_no", ""))[:8]
        row[f"{key}_cur"] = _to_f(it.get("thstrm_amount"))
        row[f"{key}_cur_add"] = _to_f(it.get("thstrm_add_amount"))
        row[f"{key}_py"] = _to_f(it.get("frmtrm_amount"))
        row[f"{key}_py_add"] = _to_f(it.get("frmtrm_add_amount"))


def fetch_dart_period(year: int, q: str, codes: list[str]) -> tuple[pd.DataFrame, int]:
    """해당 연도·분기의 주요계정을 회사 100개씩(DART_WORKERS 개 동시에) 받아 종목코드별 한 행으로.
    반환: (표, 실패한 호출 수). 실패(네트워크·키 오류)와 '아직 제출한 회사 없음'(status 013 → 빈 목록)을 구분한다."""
    reprt = REPRT[q]
    chunks = [[_corp_map[c] for c in codes[i:i + 100] if c in _corp_map] for i in range(0, len(codes), 100)]
    chunks = [c for c in chunks if c]

    def _one(corp_codes: list[str]):
        js = _dart_get("fnlttMultiAcnt.json", {"corp_code": ",".join(corp_codes), "bsns_year": str(year), "reprt_code": reprt})
        if DART_DELAY > 0:
            time.sleep(DART_DELAY)
        _state["dart_done"] = _state.get("dart_done", 0) + 1        # 빈 응답이어도 호출 1회로 센다
        if js is None:
            return None                                              # 호출 실패
        return js.get("list") or []

    rows: dict[str, dict[str, Any]] = {}
    pref: dict[str, str] = {}
    results: list[list[dict[str, Any]] | None] = []
    if DART_WORKERS > 1 and len(chunks) > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=DART_WORKERS, thread_name_prefix="dart") as ex:
            results = list(ex.map(_one, chunks))
    else:
        results = [_one(c) for c in chunks]
    failed = sum(1 for r in results if r is None)
    for items in results:                     # 응답 순서를 고정해 결과가 재현되게
        if items:
            _parse_dart_items(items, rows, pref)
    if not rows:
        return pd.DataFrame(), failed
    df = pd.DataFrame.from_dict(rows, orient="index")
    df.index = pd.Index([_intern(t) for t in df.index], dtype=object, name="ticker")
    for c in df.columns:
        if c != "rcept":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df, failed


def _to_f(x) -> float:
    if x is None:
        return np.nan
    s = str(x).replace(",", "").strip()
    if s in ("", "-"):
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


def _q_times(y: int, q: str) -> tuple[datetime, datetime, datetime]:
    """분기 (y, q) 의 (잠정 수집 시작일, 제출기한 다음 날, 확정일)."""
    qm, qd = _QEND[q]
    start = datetime(y, qm, qd) + timedelta(days=DART_EARLY_DAYS)
    m, d = _DEADLINE[q]
    due = datetime(y + 1 if q == "Q4" else y, m, d)
    return start, due, due + timedelta(days=DART_FINAL_DAYS)


def _naive(now: datetime | None = None) -> datetime:
    return (now or _today()).replace(tzinfo=None)


def _quarters_available(first_year: int, now: datetime | None = None) -> list[tuple[int, str]]:
    """수집할 수 있는 (연도, 분기) 목록, 시간순. v1.7: 제출기한이 아니라 '분기말 + DART_EARLY_DAYS 일'부터(잠정 수집)."""
    n = _naive(now)
    out = []
    for y in range(first_year, n.year + 1):
        for q in ("Q1", "Q2", "Q3", "Q4"):
            if n >= _q_times(y, q)[0]:
                out.append((y, q))
    return out


def _dart_todo(first_year: int, now: datetime | None = None, need_asof: str | None = None,
               include_failed: bool = False, age_rule: bool = True) -> list[tuple[int, str]]:
    """(다시) 받아야 할 분기.
      1) 수집 가능한데 캐시에 없는 분기
      2) 잠정 분기(확정일 전에 받은 것) 가운데
         - need_asof(편입 월말, YYYYMMDD)보다 먼저 받은 것 — 편입일 전날까지 접수된 보고서를 모두 보려면 편입일 이후에 받아야 한다
         - 받은 지 하루(제출기한 3주 전까지는 7일)가 지난 것 — 보고서가 들어오는 철에는 날마다 새로 반영
    최근 DART_RETRY_SEC 안에 실패한 분기는 건너뛴다(연속 재시도 방지) — include_failed=True 면 포함(준비 여부 판정용)."""
    n = _naive(now)
    out = []
    for (y, q) in _quarters_available(first_year, n):
        key = f"{y}{q}"
        if not include_failed and time.time() - _dart_fail.get(key, 0) < DART_RETRY_SEC:
            continue
        meta = _dart_meta.get(key) or {}
        if key not in _dart_raw:
            out.append((y, q))
            continue
        f_at = str(meta.get("fetched_at") or "")
        if len(f_at) != 8:                       # 받은 날을 모르는 옛 캐시 → 확정으로 간주
            continue
        _start, due, final = _q_times(y, q)
        if f_at >= _ymd(final):                  # 확정
            continue
        if need_asof and f_at < need_asof:
            out.append((y, q))
            continue
        if not age_rule:
            continue
        age = (n - datetime.strptime(f_at, "%Y%m%d")).days
        interval = 1 if n >= due - timedelta(days=21) else 7
        if age >= interval:
            out.append((y, q))
    return out


def dart_codes() -> list[str]:
    """DART 수집 대상: 어느 월말이든 시총이 DART_MIN_CAP_EOK(기본 300)억 이상이었던 보통주(백테스트 유니버스 하한보다 넉넉하게)."""
    min_cap = float(os.environ.get("DART_MIN_CAP_EOK", "300")) * 1e8
    with factors._lock:
        snaps = list(factors._snaps.values())
    cand: set[str] = set()
    for f in snaps:
        if "cap" in f.columns and "common" in f.columns:
            ok = (f["cap"] >= min_cap) & (f["common"] > 0)
            cand.update(str(t) for t in f.index[ok.to_numpy()])
    return sorted(t for t in cand if t in _corp_map and t.endswith("0"))


def ensure_dart(dates: list[str], need_asof: str | None = None) -> None:
    """월말 목록이 필요로 하는 분기 재무를 (없거나 다시 받아야 하면) 받아 캐시."""
    if not DART_KEY:
        _state["dart_status"] = "no_key"
        return
    if not load_corp_map():
        _state["dart_status"] = "corp_map_failed"
        return
    codes = dart_codes()
    _state["dart_companies"] = len(codes)
    if not codes:
        _state["dart_status"] = "no_tickers"
        return
    first_year = int(dates[0][:4]) if dates else _today().year - 1
    todo = _dart_todo(first_year, None, need_asof)
    per_q = max(1, (len(codes) + 99) // 100)
    _state["dart_target"] = _state.get("dart_done", 0) + len(todo) * per_q
    _state["dart_eta_note"] = f"분기 {len(todo)}개 × 호출 {per_q}회, 동시 {DART_WORKERS}개"
    if not todo:
        _state["dart_status"] = "ready"
        return
    _state["dart_status"] = "running"
    changed = 0
    for i, (y, q) in enumerate(todo, 1):
        key = f"{y}{q}"
        df, failed = fetch_dart_period(y, q, codes)
        now = _today()
        old = _dart_raw.get(key)
        if failed > max(1, per_q // 10):         # 호출의 10% 넘게 실패 → 이번 수집분은 버린다(일부 회사가 통째로 빠질 수 있음)
            _dart_fail[key] = time.time()
            _state["last_error"] = f"DART {key}: 호출 {failed}/{per_q}회 실패 → 이번 수집분은 버리고 기존 자료 유지(10분 뒤 재시도)"
            log.warning(_state["last_error"])
            continue
        if failed:
            _state["last_error"] = f"DART {key}: 호출 {failed}/{per_q}회 실패(허용 범위) — 일부 회사가 빠졌을 수 있음"
        if old is not None and len(old) >= 50 and len(df) < 0.9 * len(old):
            _dart_fail[key] = time.time()
            _state["last_error"] = f"DART {key}: 다시 받은 결과가 기존보다 적음({len(df)} < {len(old)}) → 기존 자료 유지"
            log.warning(_state["last_error"])
            continue
        with _lock:
            _dart_raw[key] = df
            _dart_meta[key] = {"n": int(len(df)), "fetched_at": _ymd(now), "fetched_ts": now.strftime("%Y-%m-%d %H:%M"),
                               "provisional": bool(_naive(now) < _q_times(y, q)[2]), "failed_calls": int(failed)}
        _dart_fail.pop(key, None)
        changed += 1
        _dart_cache.clear()
        _malloc_trim()
        if i % 4 == 0 and i < len(todo):
            save_cache()                      # 중간 저장: 재배포로 끊겨도 받은 분기는 복구됨
    _state["dart_status"] = "ready"
    if changed:
        save_cache()


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    """없는 열은 NaN float Series 로 (실데이터에서 특정 계정이 통째로 빠진 분기 대비)."""
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce").astype(float)
    return pd.Series(np.nan, index=df.index, dtype=float)


def quarter_table() -> dict[str, pd.DataFrame]:
    """캐시된 분기 주요계정 → 분기별 3개월 손익·기말 재무상태 표(index=ticker).
    열: rcept, sales3, sales3_py, op3, op3_py, ni3, ni3_py, assets, assets_py, equity, equity_py
    Q1~Q3: 3개월 금액은 thstrm_amount(누적만 있는 회사는 직전 분기 누적을 빼서). Q4: 사업보고서 연간 − 3분기 누적."""
    with _lock:
        raw = {k: v for k, v in _dart_raw.items()}
    keys = sorted(raw)
    out: dict[str, pd.DataFrame] = {}
    for k in keys:
        df = raw[k]
        if df is None or df.empty:
            continue
        y, q = int(k[:4]), k[4:]
        t = pd.DataFrame(index=df.index)
        t["rcept"] = df["rcept"].astype(str) if "rcept" in df.columns else "99999999"
        for acc in ("assets", "equity"):
            t[acc] = _col(df, f"{acc}_cur")
            t[f"{acc}_py"] = _col(df, f"{acc}_py")
        if q in ("Q1", "Q2", "Q3"):
            prev = raw.get(f"{y}{'Q1' if q == 'Q2' else 'Q2'}") if q != "Q1" else None
            for acc in ("sales", "op", "ni"):
                cur3, cur_add = _col(df, f"{acc}_cur"), _col(df, f"{acc}_cur_add")
                py3, py_add = _col(df, f"{acc}_py"), _col(df, f"{acc}_py_add")
                if q == "Q1":
                    t[f"{acc}3"], t[f"{acc}3_py"] = cur3, py3
                else:
                    has_add = cur_add.notna() & cur3.notna() & (cur_add != cur3)   # 3개월·누적이 따로 있으면 3개월 값 그대로
                    c3, p3 = cur3.copy(), py3.copy()
                    if prev is not None and not prev.empty:
                        pc = _col(prev, f"{acc}_cur_add").reindex(df.index)
                        pp = _col(prev, f"{acc}_py_add").reindex(df.index)
                        derived = cur_add - pc
                        derived_p = py_add - pp
                        c3 = c3.where(has_add, derived.where(derived.notna(), c3))
                        p3 = p3.where(has_add, derived_p.where(derived_p.notna(), p3))
                    t[f"{acc}3"], t[f"{acc}3_py"] = c3, p3
                if q == "Q3":
                    t[f"{acc}_cum"], t[f"{acc}_cum_py"] = cur_add, py_add
        else:  # Q4 = 연간 − 3분기 누적
            q3 = out.get(f"{y}Q3")
            for acc in ("sales", "op", "ni"):
                yr, yr_py = _col(df, f"{acc}_cur"), _col(df, f"{acc}_py")
                if q3 is not None and f"{acc}_cum" in q3.columns:
                    t[f"{acc}3"] = yr - q3[f"{acc}_cum"].reindex(df.index)
                    t[f"{acc}3_py"] = yr_py - q3[f"{acc}_cum_py"].reindex(df.index)
                else:
                    t[f"{acc}3"], t[f"{acc}3_py"] = np.nan, np.nan
                t[f"{acc}_y"] = yr
        out[k] = t
    return out


def dart_panels(dates: list[str], tickers: list[str]) -> dict[str, pd.DataFrame]:
    """월말(dates) × 티커 패널: 각 월말에 '그때까지 접수된 최신 분기'의 값.
    반환 키: op3, op3_py, sales3, sales3_py, ni_ttm, op_ttm, assets, assets_py, equity"""
    Q = quarter_table()
    keys = sorted(Q)
    cols = pd.Index(tickers)
    n_d, n_t = len(dates), len(tickers)
    panels = {k: np.full((n_d, n_t), np.nan) for k in ("op3", "op3_py", "sales3", "sales3_py", "ni_ttm", "op_ttm", "assets", "assets_py", "equity")}
    date_arr = np.array(dates)
    hist_ni: list[pd.Series] = []
    hist_op: list[pd.Series] = []
    for k in keys:
        t = Q[k].reindex(cols)
        rc = t["rcept"].astype(str).where(t["rcept"].notna(), "99999999").to_numpy()
        rc = np.array([s if (len(s) == 8 and s.isdigit()) else "99999999" for s in rc])
        # (dates × tickers): 이 분기가 반영 가능한 월말 — 접수일 '다음 날'부터(접수일 당일은 장 마감 뒤 제출일 수 있음; v1.6 까지는 당일 포함)
        avail = date_arr[:, None] > rc[None, :]
        hist_ni.append(t.get("ni3", pd.Series(np.nan, index=cols)))
        hist_op.append(t.get("op3", pd.Series(np.nan, index=cols)))
        ni_ttm = sum(s.reindex(cols).fillna(np.nan) for s in hist_ni[-4:]) if len(hist_ni) >= 4 else pd.Series(np.nan, index=cols)
        op_ttm = sum(s.reindex(cols).fillna(np.nan) for s in hist_op[-4:]) if len(hist_op) >= 4 else pd.Series(np.nan, index=cols)
        vals = {"op3": t.get("op3"), "op3_py": t.get("op3_py"), "sales3": t.get("sales3"), "sales3_py": t.get("sales3_py"),
                "ni_ttm": ni_ttm, "op_ttm": op_ttm, "assets": t.get("assets"), "assets_py": t.get("assets_py"), "equity": t.get("equity")}
        for name, s in vals.items():
            if s is None:
                continue
            v = s.reindex(cols).to_numpy(dtype=float)
            m = avail & ~np.isnan(v)[None, :]
            panels[name] = np.where(m, v[None, :], panels[name])
    return {k: pd.DataFrame(v, index=dates, columns=cols) for k, v in panels.items()}


# ────────────────────────────────────────────────────────────
# 패널 → 팩터 (factors.register_factor 로 등록)
# ────────────────────────────────────────────────────────────
def _sig_panels(P: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """factors 패널 P 와 같은 (행=날짜, 열=티커) 모양의 신호 패널. 신호가 없는 날짜(최근 거래일 등)는 직전 월말 값으로."""
    idx, cols = P["close"].index, P["close"].columns
    with _lock:
        frames = {d: _sig[d] for d in sorted(_sig)}
    out: dict[str, pd.DataFrame] = {}
    if not frames:
        for c in SIG_COLS:
            out[c] = pd.DataFrame(np.nan, index=idx, columns=cols)
        return out
    keys = list(frames)
    for c in SIG_COLS:
        mat = np.full((len(keys), len(cols)), np.nan)
        pos = {t: i for i, t in enumerate(cols)}
        for r, d in enumerate(keys):
            f = frames[d]
            common = [t for t in f.index if t in pos]
            if common:
                mat[r, [pos[t] for t in common]] = f.loc[common, c].to_numpy(dtype=float)
        df = pd.DataFrame(mat, index=keys, columns=cols)
        # P 의 날짜축으로 (신호 월말과 P 월말은 같은 날짜; 최근 거래일 행은 직전 월말 값 ffill)
        out[c] = df.reindex(idx.union(df.index)).sort_index().ffill(limit=1).reindex(idx)
    return out


_sig_cache: dict[int, dict[str, pd.DataFrame]] = {}
_dart_cache: dict[int, dict[str, pd.DataFrame]] = {}
_short_cache: dict[int, pd.DataFrame] = {}


def _get_short(P: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """공표 시차를 반영한 공매도 잔고 비중 패널(행=날짜, 열=티커). 월말이 아닌 최근 거래일 행은 직전 월말 값(1칸 ffill)."""
    key = id(P["close"])
    if key not in _short_cache:
        _short_cache.clear()
        idx, cols = P["close"].index, P["close"].columns
        with _lock:
            frames = {d: _short_l2[d] for d in sorted(_short_l2)}
        if not frames:
            _short_cache[key] = pd.DataFrame(np.nan, index=idx, columns=cols)
        else:
            keys = list(frames)
            pos = {t: i for i, t in enumerate(cols)}
            mat = np.full((len(keys), len(cols)), np.nan)
            for r, d in enumerate(keys):
                f = frames[d]
                common = [t for t in f.index if t in pos]
                if common:
                    mat[r, [pos[t] for t in common]] = f.loc[common].to_numpy(dtype=float)
            df = pd.DataFrame(mat, index=keys, columns=cols)
            _short_cache[key] = df.reindex(idx.union(df.index)).sort_index().ffill(limit=1).reindex(idx)
    return _short_cache[key]


def clear_panel_caches() -> None:
    """한 번의 계산이 끝나면 패널 캐시를 비운다(패널은 호출마다 새로 만들어져 재사용되지 않으므로 메모리만 차지)."""
    _sig_cache.clear()
    _dart_cache.clear()
    _short_cache.clear()


def _get_sig(P: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    key = id(P["close"])
    if key not in _sig_cache:
        _sig_cache.clear()
        _sig_cache[key] = _sig_panels(P)
    return _sig_cache[key]


_dart_compute_error: dict[str, Any] = {"error": None}


def _get_dart(P: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    key = id(P["close"])
    if key not in _dart_cache:
        _dart_cache.clear()
        try:
            _dart_cache[key] = dart_panels(list(P["close"].index), list(P["close"].columns))
            _dart_compute_error["error"] = None
        except Exception as e:  # noqa: BLE001
            _dart_compute_error["error"] = f"{type(e).__name__}: {e}"
            log.exception("DART 패널 계산 실패")
            idx, cols = P["close"].index, P["close"].columns
            _dart_cache[key] = {k: pd.DataFrame(np.nan, index=idx, columns=cols)
                                for k in ("op3", "op3_py", "sales3", "sales3_py", "ni_ttm", "op_ttm", "assets", "assets_py", "equity")}
    return _dart_cache[key]


def _sector_returns(P: dict[str, pd.DataFrame], S: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """업종 동일가중 월간 수익률(행=날짜, 열=업종코드)과 각 종목의 업종코드 패널(직전 월말 분류를 그 달 수익률에 적용)."""
    ret = P["ret"]
    sec = S["sector"].shift(1)        # 월 t 의 수익률은 월 t-1 말 분류로 묶는다(시점 일관성)
    labels = sorted({int(x) for x in pd.unique(sec.to_numpy().ravel()) if not np.isnan(x)})
    mat = np.full((len(ret.index), len(labels)), np.nan)
    sec_np, ret_np = sec.to_numpy(), ret.to_numpy()
    for j, lab in enumerate(labels):
        m = (sec_np == lab) & ~np.isnan(ret_np)
        cnt = m.sum(axis=1)
        s = np.where(m, ret_np, 0.0).sum(axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            mat[:, j] = np.where(cnt >= 3, s / cnt, np.nan)
    return pd.DataFrame(mat, index=ret.index, columns=labels), sec


def _map_sector_value(sec_panel: pd.DataFrame, sec_series: pd.DataFrame) -> pd.DataFrame:
    """업종별 시계열(행=날짜, 열=업종코드)을 종목 패널(행=날짜, 열=티커)로 펼친다."""
    out = np.full(sec_panel.shape, np.nan)
    sec_np = sec_panel.to_numpy()
    cols = {lab: j for j, lab in enumerate(sec_series.columns)}
    vals = sec_series.to_numpy()
    for lab, j in cols.items():
        m = sec_np == lab
        out = np.where(m, vals[:, j][:, None], out)
    return pd.DataFrame(out, index=sec_panel.index, columns=sec_panel.columns)


def f_sec_mom(P):
    S = _get_sig(P)
    sr, sec = _sector_returns(P, S)
    logr = np.log1p(sr.where(sr > -0.999))
    mom = np.exp(logr.shift(1).rolling(11, min_periods=10).sum()) - 1.0
    return _map_sector_value(S["sector"], mom)


def f_rs3(P):
    S = _get_sig(P)
    sr, sec = _sector_returns(P, S)
    logr = np.log1p(P["ret"].where(P["ret"] > -0.999))
    own3 = np.exp(logr.rolling(3, min_periods=3).sum()) - 1.0
    slog = np.log1p(sr.where(sr > -0.999))
    sec3 = np.exp(slog.rolling(3, min_periods=3).sum()) - 1.0
    return own3 - _map_sector_value(S["sector"], sec3)


def f_flow20(P):
    S = _get_sig(P)
    return (S["frgn20"].fillna(0) + S["inst20"].fillna(0)).where(S["frgn20"].notna() | S["inst20"].notna()) / P["cap"].where(P["cap"] > 0)


def f_flow60(P):
    S = _get_sig(P)
    f = (S["frgn20"].fillna(0) + S["inst20"].fillna(0)).where(S["frgn20"].notna() | S["inst20"].notna())
    return f.rolling(3, min_periods=2).sum() / P["cap"].where(P["cap"] > 0)


def f_frgn_chg(P):
    S = _get_sig(P)
    return S["frgn_hold"] - S["frgn_hold"].shift(3)


def f_short_bal(P):
    return _get_short(P)


def f_short_chg(P):
    sb = _get_short(P)
    return sb - sb.shift(3)


def f_epm(P):
    D = _get_dart(P)
    return (D["op3"] - D["op3_py"]) / P["cap"].where(P["cap"] > 0)


def f_sales_g(P):
    D = _get_dart(P)
    return D["sales3"] / D["sales3_py"].where(D["sales3_py"] > 0) - 1.0


def f_opm_chg(P):
    D = _get_dart(P)
    m1 = D["op3"] / D["sales3"].where(D["sales3"] > 0)
    m0 = D["op3_py"] / D["sales3_py"].where(D["sales3_py"] > 0)
    return (m1 - m0) * 100.0


def f_agr(P):
    D = _get_dart(P)
    return D["assets"] / D["assets_py"].where(D["assets_py"] > 0) - 1.0


def f_roe_ttm(P):
    D = _get_dart(P)
    return D["ni_ttm"] / D["equity"].where(D["equity"] > 0)


def f_op_yield(P):
    D = _get_dart(P)
    return D["op_ttm"] / P["cap"].where(P["cap"] > 0)


def f_bm_q(P):
    D = _get_dart(P)
    return D["equity"].where(D["equity"] > 0) / P["cap"].where(P["cap"] > 0)


def f_sec_margin(P):
    S = _get_sig(P)
    opm = f_opm_chg(P)
    sec = S["sector"]
    labels = sorted({int(x) for x in pd.unique(sec.to_numpy().ravel()) if not np.isnan(x)})
    med = np.full((len(opm.index), len(labels)), np.nan)
    sec_np, v = sec.to_numpy(), opm.to_numpy()
    for j, lab in enumerate(labels):
        m = (sec_np == lab) & ~np.isnan(v)
        for r in range(len(opm.index)):
            row = v[r][m[r]]
            if len(row) >= 3:
                med[r, j] = float(np.median(row))
    return _map_sector_value(sec, pd.DataFrame(med, index=opm.index, columns=labels))


def register_all() -> None:
    reg = factors.register_factor
    reg("epm", True, "이익 모멘텀: (당분기 3개월 영업이익 − 전년 동분기) / 시총 [DART]", f_epm)
    reg("sales_g", True, "매출 성장: 3개월 매출 전년 동분기 대비 증가율 [DART]", f_sales_g)
    reg("opm_chg", True, "마진 개선: 영업이익률(3개월) 전년 동분기 대비 변화(%p) [DART]", f_opm_chg)
    reg("agr", False, "투자 팩터: 자산총계 전년 대비 증가율 (낮을수록 좋음) [DART]", f_agr)
    reg("roe_ttm", True, "퀄리티: 최근 4분기 순이익 / 자본총계 [DART]", f_roe_ttm)
    reg("op_yield", True, "가치: 최근 4분기 영업이익 / 시총 [DART]", f_op_yield)
    reg("sec_margin", True, "산업 마진 추세: 업종 내 opm_chg 중앙값 [DART+KRX 업종]", f_sec_margin)
    reg("sec_mom", True, "업종 모멘텀: 소속 업종 동일가중 수익률 12-1개월 [KRX 업종]", f_sec_mom)
    reg("rs3", True, "업종 내 상대강도: 종목 3개월 수익률 − 업종 3개월 수익률", f_rs3)
    reg("flow20", True, "수급: (외국인+기관) 20거래일 순매수 / 시총", f_flow20)
    reg("flow60", True, "수급: 최근 3개월 (외국인+기관) 순매수 합 / 시총", f_flow60)
    reg("frgn_chg", True, "외국인 지분율 3개월 변화(%p)", f_frgn_chg)
    reg("short_bal", False, "공매도 잔고 비중(%) — 월말 2거래일 전 잔고(공표 시차 반영) (낮을수록 좋음)", f_short_bal)
    reg("short_chg", False, "공매도 잔고 비중 3개월 변화(%p) — 공표 시차 반영 (낮을수록 좋음)", f_short_chg)
    reg("bm_q", True, "가치: 최근 분기 자본총계 / 시총 [DART] (KRX PBR 보다 최신 장부가)", f_bm_q)
    factors.register_group("earnings", ["epm", "sales_g", "opm_chg"])
    factors.register_group("quality2", ["roe_ttm", "agr"])
    factors.register_group("sector", ["sec_mom", "rs3", "sec_margin"])
    factors.register_group("flow", ["flow20", "flow60", "frgn_chg"])
    factors.register_group("shortint", ["short_bal", "short_chg"])


# ────────────────────────────────────────────────────────────
# 수집 오케스트레이션
# ────────────────────────────────────────────────────────────
def save_cache() -> bool:
    if cachestore is None:
        return False
    with _lock:
        payload = {"version": 2, "sig": dict(_sig), "sig_meta": dict(_sig_meta), "sector_code": dict(_sector_code),
                   "sector_name": dict(_sector_name), "dart_raw": dict(_dart_raw), "dart_meta": dict(_dart_meta),
                   "corp_map": dict(_corp_map), "corp_map_at": _corp_map_at.get("ymd"),
                   "short_l2": dict(_short_l2), "short_l2_meta": dict(_short_l2_meta),
                   "saved_at": _today().strftime("%Y-%m-%d %H:%M:%S")}
    return cachestore.save(CACHE_NAME, payload)


def load_cache() -> int:
    global _corp_map
    if cachestore is None:
        return 0
    obj = cachestore.load(CACHE_NAME)
    if not obj or not isinstance(obj, dict) or "sig" not in obj:
        return 0
    with _lock:
        for d, df in obj["sig"].items():
            if isinstance(df, pd.DataFrame) and list(df.columns) == SIG_COLS:
                _sig[d] = df
                _sig_meta[d] = obj.get("sig_meta", {}).get(d, {})
        _sector_code.update(obj.get("sector_code", {}))
        _sector_name.update({int(k): v for k, v in obj.get("sector_name", {}).items()})
        for k, df in obj.get("dart_raw", {}).items():
            if isinstance(df, pd.DataFrame):
                _dart_raw[k] = df
                _dart_meta[k] = obj.get("dart_meta", {}).get(k, {})
        if obj.get("corp_map"):
            _corp_map = dict(obj["corp_map"])
            _corp_map_at["ymd"] = obj.get("corp_map_at")
        for d, ser in (obj.get("short_l2") or {}).items():          # v1.7 부터(없으면 서버가 백그라운드로 채움)
            if isinstance(ser, pd.Series):
                _short_l2[d] = ser
        _short_l2_meta.update(obj.get("short_l2_meta") or {})
    if _dart_raw and DART_KEY:
        _state["dart_status"] = "ready"
    log.info("signals 캐시 복구: 월말 %d개, DART 분기 %d개, 공매도(T-%d) %d개 (저장 %s)", len(_sig), len(_dart_raw),
             SHORT_LAG_DAYS, len(_short_l2), obj.get("saved_at"))
    return len(_sig)


def ensure_signals(dates: list[str]) -> None:
    todo = [d for d in dates if d not in _sig]
    _state["sig_target"] = len(dates)
    _state["sig_done"] = len(dates) - len(todo)
    for d in todo:
        try:
            fetch_month(d)
        except Exception as e:  # noqa: BLE001
            _state["last_error"] = f"{d}: {e}"
            log.warning("신호 수집 실패 %s: %s", d, e)
        _state["sig_done"] += 1
        _sig_cache.clear()
    if todo:
        save_cache()


def _short_lag_needed(dates: list[str]) -> list[str]:
    """공표 시차 반영 공매도 잔고가 있어야 하는 월말(공시 시작 이후)."""
    return [d for d in dates if d >= SHORT_DATA_FROM and _lag_date(d, SHORT_LAG_DAYS) >= SHORT_DATA_FROM]


def ensure_short_lag(dates: list[str], retry_empty_sec: int = 6 * 3600) -> int:
    """월말들의 '2거래일 전 공매도 잔고'를 (없는 달만) 받는다. v1.7 로 올린 뒤 처음 한 번은 기존 월말 전부(약 250회 호출)."""
    need = _short_lag_needed(dates)
    now = time.time()
    todo = [d for d in need if d not in _short_l2
            and now - float((_short_l2_meta.get(d) or {}).get("tried") or 0) > retry_empty_sec]
    _state["short_lag_target"] = len(need)
    _state["short_lag_done"] = len(need) - len([d for d in need if d not in _short_l2])
    got = 0
    for i, d in enumerate(todo, 1):
        try:
            if fetch_short_lag(d) > 0:
                got += 1
                _state["short_lag_done"] += 1
        except Exception as e:  # noqa: BLE001
            _state["last_error"] = f"공매도(T-{SHORT_LAG_DAYS}) {d}: {e}"
            log.warning("공매도 잔고(T-%d) 수집 실패 %s: %s", SHORT_LAG_DAYS, d, e)
        if i % 24 == 0 and got:
            save_cache()                      # 중간 저장
    if got:
        save_cache()
    return got


def _wait_for_factors(max_wait_sec: int = 7200) -> list[str]:
    t0 = time.time()
    time.sleep(15)
    while time.time() - t0 < max_wait_sec:
        st = factors._state
        with factors._lock:
            n = len(factors._snaps)
        if not st.get("running") and n >= factors.WARMUP_MONTHS:
            break
        time.sleep(10)
    with factors._lock:
        return sorted(factors._snaps)


def prefetch(years: float | None = None) -> dict[str, Any]:
    years = float(years or SIGNALS_YEARS)
    if _state["running"]:
        return cache_status()

    def _job():
        _state.update(running=True, last_error=None, started_at=_today().strftime("%Y-%m-%d %H:%M"))
        try:
            dates = _wait_for_factors()
            n = int(round(years * 12)) + factors.WARMUP_MONTHS
            dates = dates[-n:]
            # DART 먼저(빠르고 다른 호스트) → KRX 신호
            def _dart_job():
                try:
                    ensure_dart(dates)
                except Exception as e:  # noqa: BLE001
                    _state["dart_status"] = f"failed: {type(e).__name__}: {e}"
                    _state["last_error"] = f"DART: {type(e).__name__}: {e}"
                    log.exception("DART 수집 실패")
            if DART_KEY:
                t = threading.Thread(target=_dart_job, name="signals-dart", daemon=True)
                t.start()
            else:
                t = None
            ensure_signals(dates)
            ensure_short_lag(dates)
            if t is not None:
                t.join()
            clear_panel_caches()
        except Exception as e:  # noqa: BLE001
            _state["last_error"] = str(e)
            log.exception("signals prefetch 실패")
        finally:
            _state["running"] = False
            _state["finished_at"] = _today().strftime("%Y-%m-%d %H:%M")

    threading.Thread(target=_job, name="signals-prefetch", daemon=True).start()
    return cache_status()


def _refresh_needs(need_asof: str | None = None) -> dict[str, Any]:
    """지금 보충해야 할 것(빠른 점검, 네트워크 호출 없음)."""
    with factors._lock:
        dates = sorted(factors._snaps)
    if not dates:
        return {"dates": dates, "sig": [], "short": [], "dart": []}
    sig = [d for d in dates[-3:] if d not in _sig]
    now = time.time()
    short = [d for d in _short_lag_needed(dates) if d not in _short_l2
             and now - float((_short_l2_meta.get(d) or {}).get("tried") or 0) > 6 * 3600]
    dart = _dart_todo(int(dates[0][:4]), None, need_asof) if DART_KEY else []
    return {"dates": dates, "sig": sig, "short": short, "dart": dart}


def refresh_recent(need_asof: str | None = None) -> None:
    """새 월말의 신호, 빠진 공매도(T−2) 잔고, (다시) 받아야 할 DART 분기를 보충한다(월초 자동 실행·실적 철 대비)."""
    need = _refresh_needs(need_asof)
    dates = need["dates"]
    if not dates:
        return
    if need["sig"]:
        ensure_signals(dates)
    if need["short"]:
        ensure_short_lag(dates)
    if need["dart"]:
        ensure_dart(dates, need_asof)
    clear_panel_caches()


def refresh_recent_async(need_asof: str | None = None) -> bool:
    """도구 호출 시 백그라운드로 최근 월말 신호·분기 재무를 보충(응답을 막지 않음). 돌고 있으면 True."""
    if _state["running"]:
        return True
    need = _refresh_needs(need_asof)
    if not (need["sig"] or need["short"] or need["dart"]):
        return False

    def _job():
        _state["running"] = True
        _state["started_at"] = _today().strftime("%Y-%m-%d %H:%M")
        try:
            refresh_recent(need_asof)
        except Exception as e:  # noqa: BLE001
            _state["last_error"] = f"refresh: {type(e).__name__}: {e}"
            log.exception("signals refresh 실패")
        finally:
            _state["running"] = False
            _state["finished_at"] = _today().strftime("%Y-%m-%d %H:%M")

    threading.Thread(target=_job, name="signals-refresh", daemon=True).start()
    return True


def paper_ready(asof: str, need_dart: bool = True, need_short: bool = True) -> dict[str, Any]:
    """등록 전략이 월말 asof 에 편입할 종목을 정하는 데 필요한 자료가 준비됐는지. 빠진 것이 있으면 백그라운드 수집을 시작한다.
    DART: 잠정 분기는 asof 이후에 받은 것이어야 한다(asof 전날까지 접수된 보고서를 모두 반영하기 위해)."""
    missing: list[str] = []
    with factors._lock:
        fdates = [d for d in sorted(factors._snaps) if d <= asof]
    if need_short:
        # short_chg 는 3개월 전 잔고와의 차이 → 편입 월말과 그 3개월 전까지(최근 4개 월말)가 모두 있어야 한다
        lack = [d for d in _short_lag_needed(fdates[-4:]) if d not in _short_l2]
        if lack:
            missing.append(f"공매도 잔고(T−{SHORT_LAG_DAYS}) {', '.join(lack)}")
    if need_dart:
        if not DART_KEY:
            return {"ok": False, "fatal": True, "message": "DART_API_KEY 가 없어 이익 신호를 계산할 수 없습니다(Railway 변수 확인)."}
        with factors._lock:
            dates = sorted(factors._snaps)
        todo = _dart_todo(int(dates[0][:4]), None, need_asof=asof, include_failed=True, age_rule=False) if dates else []
        if todo:
            missing.append("DART 분기 " + ", ".join(f"{y}{q}" for y, q in todo) + f" (편입일 {asof} 이후에 받은 자료 필요)")
    if not missing:
        return {"ok": True}
    running = refresh_recent_async(need_asof=asof)
    return {"ok": False, "missing": missing, "refresh_running": bool(running), "last_error": _state.get("last_error"),
            "message": "편입에 필요한 자료를 받는 중입니다(DART 분기 하나에 약 6분, KRX 신호 약 30초). 몇 분 뒤 다시 실행하세요."}


def readiness_warnings() -> list[str]:
    """백테스트 결과에 붙일 경고(자료가 덜 모였을 때)."""
    out: list[str] = []
    with factors._lock:
        dates = sorted(factors._snaps)
    if not dates:
        return out
    miss_sig = [d for d in dates if d not in _sig]
    if miss_sig:
        out.append(f"업종·수급 신호가 없는 월말 {len(miss_sig)}개 — sector·flow 계열 결과는 수집이 끝난 뒤 다시 실행하세요")
    need = _short_lag_needed(dates)
    have = sum(1 for d in need if d in _short_l2)
    if need and have < len(need):
        out.append(f"공매도 잔고(T−{SHORT_LAG_DAYS}) 자료가 {have}/{len(need)}개 월말만 있음 — short_bal·short_chg·shortint 가 들어간 결과는 "
                   "수집이 끝난 뒤(signals_cache_status 의 short_lag) 다시 실행하세요")
    if DART_KEY:
        missing_q = [f"{y}{q}" for (y, q) in _quarters_available(int(dates[0][:4])) if f"{y}{q}" not in _dart_raw]
        if missing_q:
            out.append(f"DART 분기 {len(missing_q)}개 미수집({missing_q[0]}…{missing_q[-1]}) — 이익 신호 결과는 수집이 끝난 뒤 다시 실행하세요")
    return out


def cache_status() -> dict[str, Any]:
    with _lock:
        sig_dates = sorted(_sig)
        metas = [_sig_meta[d] for d in sig_dates]
        dart_keys = sorted(_dart_raw)
        dart_n = {k: _dart_meta.get(k, {}).get("n") for k in dart_keys}
        dart_meta = {k: dict(_dart_meta.get(k) or {}) for k in dart_keys}
        short_dates = sorted(_short_l2)
    st = dict(_state)
    with factors._lock:
        f_dates = sorted(factors._snaps)
    need_short = _short_lag_needed(f_dates)
    provisional = [k for k in dart_keys if dart_meta[k].get("provisional")]
    recent = {k: {"n": dart_meta[k].get("n"), "fetched": dart_meta[k].get("fetched_ts") or dart_meta[k].get("fetched_at"),
                  "provisional": bool(dart_meta[k].get("provisional"))} for k in dart_keys[-3:]}
    todo_now = [f"{y}{q}" for (y, q) in (_dart_todo(int(f_dates[0][:4])) if (DART_KEY and f_dates) else [])]
    return {"signal_month_ends": len(sig_dates), "earliest": sig_dates[0] if sig_dates else None, "latest": sig_dates[-1] if sig_dates else None,
            "coverage_avg": {"sector": int(np.mean([m["sector_n"] for m in metas])) if metas else 0,
                             "flow": int(np.mean([m["flow_n"] for m in metas])) if metas else 0,
                             "short": int(np.mean([m["short_n"] for m in metas])) if metas else 0,
                             "frgn_hold": int(np.mean([m["hold_n"] for m in metas])) if metas else 0},
            "short_lag": {"lag_trading_days": SHORT_LAG_DAYS, "month_ends": len(short_dates), "needed": len(need_short),
                          "complete": bool(need_short) and all(d in _short_l2 for d in need_short),
                          "first": short_dates[0] if short_dates else None, "last": short_dates[-1] if short_dates else None,
                          "last_balance_date": (_short_l2_meta.get(short_dates[-1]) or {}).get("date") if short_dates else None,
                          "note": "KRX 공매도 잔고는 T+2 거래일에 공표 → 월말 신호는 2거래일 전 잔고 사용(short_bal·short_chg)"},
            "sectors_known": len(_sector_code),
            "dart": {"status": st.get("dart_status"), "key_set": bool(DART_KEY), "key_len": len(DART_KEY), "quarters": len(dart_keys),
                     "first": dart_keys[0] if dart_keys else None, "last": dart_keys[-1] if dart_keys else None,
                     "companies_per_quarter_avg": int(np.mean([v for v in dart_n.values() if v])) if any(dart_n.values()) else 0,
                     "corp_map": len(_corp_map), "companies_fetched": st.get("dart_companies"),
                     "calls_done": st.get("dart_done"), "calls_target": st.get("dart_target"),
                     "provisional_quarters": provisional, "recent_quarters": recent, "to_fetch_now": todo_now,
                     "panel_compute_error": _dart_compute_error["error"], "last_error": st.get("last_error")},
            "prefetch": st, "process_rss_mb": _rss_mb(),
            "persist": cachestore.status(CACHE_NAME) if cachestore is not None else {"enabled": False},
            "factors_registered": [n for n in factors.ATOMIC_FACTORS if n in factors.EXTRA_FACTORS],
            "groups": {k: v for k, v in factors.GROUP_FACTORS.items() if k in ("earnings", "quality2", "sector", "flow", "shortint")},
            "note": "factors 월말 수집이 끝난 뒤 시작. 월말당 KRX 9회(약 20~30초), DART 분기당 약 29회(약 6분). "
                    "신호가 없는 달은 해당 팩터가 NaN 으로 처리돼 그 달 유니버스에서 빠집니다. "
                    "DART 는 분기말+20일부터 잠정 수집하고 제출기한+10일까지 하루 한 번 다시 받습니다."}


register_all()
factors.READINESS_HOOKS.append(readiness_warnings)
factors.CLEANUP_HOOKS.append(clear_panel_caches)
factors.PAPER_READY["fn"] = paper_ready
try:
    load_cache()
except Exception as _e:  # noqa: BLE001
    log.warning("signals 캐시 복구 중 오류(무시): %s", _e)
if os.environ.get("SIGNALS_PREFETCH", "1") != "0":
    prefetch()
