#!/usr/bin/env python3
"""
pykrx MCP — 한국거래소(KRX) 시세를 MCP 도구로 노출하는 원격 서버.

왜 만들었나
-----------
- KIS Open API는 `0219E0` 같은 **영문 섞인 종목코드**를 읽지 못합니다.
- KIS 일봉은 1회 100건 제한이라 12개월 차트를 못 그립니다(약 5개월에서 끊김).
- 국내 수급(투자자별 순매매)·공매도 데이터를 주는 커넥터가 없습니다.

pykrx는 KRX 공식 데이터를 그대로 읽으므로 위 3가지가 한 번에 해결됩니다.

전송 방식
---------
Streamable HTTP. Claude 커스텀 커넥터가 바로 붙습니다.
인증은 `Authorization: Bearer <MCP_TOKEN>` 헤더 하나로 끝냅니다.

⚠️ 이 서버는 **읽기 전용**입니다. 주문·이체 기능은 없고 앞으로도 넣지 마세요.
⚠️ 매매 의견을 생성하지 않습니다. 숫자와 기준일만 돌려줍니다.
"""

from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import pandas as pd
from pykrx import stock

import indicators          # 같은 폴더. 지표 계산을 서버에서 수행
import screener            # 같은 폴더. 전종목 조건 스크리너
import factors             # 같은 폴더. 학계식 팩터(횡단면 순위) 백테스트·스크리너 (월말 스냅샷 캐시)

# MCP SDK 2.x 에서 FastMCP 가 MCPServer 로 이름이 바뀌었습니다.
# 어느 버전이 깔려도 돌아가게 둘 다 시도합니다.
try:
    from mcp.server.mcpserver import MCPServer as _Server  # mcp >= 2.0
except ImportError:  # pragma: no cover
    from mcp.server.fastmcp import FastMCP as _Server  # mcp < 2.0

from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Mount, Route

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pykrx-mcp")

KST = timezone(timedelta(hours=9))

# 지수 코드 (KRX 기준)
INDEX_CODES: dict[str, str] = {
    "KOSPI": "1001",
    "KOSDAQ": "2001",
    "KOSPI200": "1028",
}

MAX_ROWS = 400  # 응답 폭주 방지. 12개월 영업일 ≈ 245.


# ─────────────────────────────────────────────────────────────
# 날짜 유틸
# ─────────────────────────────────────────────────────────────

def today_kst() -> datetime:
    """Railway는 UTC로 돌기 때문에 한국 날짜를 직접 계산해야 합니다."""
    return datetime.now(KST)


def ymd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def months_ago(n: int) -> str:
    return ymd(today_kst() - timedelta(days=int(n * 30.5) + 5))


def days_ago(n: int) -> str:
    return ymd(today_kst() - timedelta(days=n + 10))  # 주말·공휴일 여유


# ─────────────────────────────────────────────────────────────
# pykrx 호출 래퍼
#
# pykrx는 버전에 따라 함수명이 `get_x` / `get_x_by_date` 로 갈립니다.
# 배포 환경에서 조용히 AttributeError 가 나지 않도록 둘 다 시도합니다.
# ─────────────────────────────────────────────────────────────

def _retry(fn, *args, tries: int = 3, **kwargs):
    """
    KRX 호출 재시도.

    왜: 브리핑이 여러 종목을 연달아 조회하면 KRX가 일부 요청을 순간적으로 거부합니다
    (실측 2026-10-27 16:05: 같은 엔드포인트가 단독 호출은 성공, 연속 호출 중에는
    SERVER_ERROR). 호출자가 매번 대응하게 두지 말고 서버에서 흡수합니다.

    빈 DataFrame 도 재시도 대상입니다 — KRX 는 실패를 빈 응답으로 돌려주기도 합니다.
    단 휴장일처럼 **정상적으로 비어 있는** 경우가 있으므로 마지막 결과를 그대로 반환합니다.
    """
    last_err: Exception | None = None
    out = None
    for i in range(tries):
        try:
            out = fn(*args, **kwargs)
            if out is not None and not (isinstance(out, pd.DataFrame) and out.empty):
                return out
        except Exception as e:
            last_err = e
            log.warning("KRX 호출 실패 (%d/%d): %s", i + 1, tries, e)
        if i < tries - 1:
            time.sleep(1.5 * (i + 1))      # 1.5s → 3.0s
    if out is None and last_err is not None:
        raise last_err
    return out if out is not None else pd.DataFrame()


def _call(primary: str, fallback: str, *args) -> pd.DataFrame:
    fn = getattr(stock, primary, None) or getattr(stock, fallback, None)
    if fn is None:
        raise RuntimeError(f"pykrx에 {primary}/{fallback} 가 없습니다. 버전을 확인하세요.")
    return _retry(fn, *args)


def _ohlcv(code: str, start: str, end: str) -> pd.DataFrame:
    return _call("get_market_ohlcv", "get_market_ohlcv_by_date", start, end, code)


def _etf_ohlcv(code: str, start: str, end: str) -> pd.DataFrame:
    return _call("get_etf_ohlcv_by_date", "get_etf_ohlcv", start, end, code)


def _index_ohlcv(idx: str, start: str, end: str) -> pd.DataFrame:
    """
    ⚠️ `stock.get_index_ohlcv` 는 **인자 개수로 동작이 갈리는 오버로드 함수**입니다.
       (3인자 = 기간별 지수 시세 / 1인자 = 특정일 전종목 시세)
       분기를 잘못 타면 '지수명' 을 인덱스로 쓰는 전종목 DataFrame 이 돌아와
       `KeyError: '지수명'` 이 납니다.
       모호함을 없애기 위해 **`get_index_ohlcv_by_date` 를 직접** 부릅니다.

    ⚠️ `name_display=False` 가 필수입니다. 기본값(True)이면 pykrx가 OHLCV를
       정상적으로 받아온 **뒤에** 지수 이름을 따로 조회하는데
       (`df.columns.name = get_index_ticker_name(ticker)`),
       그 조회가 실패하면 데이터를 다 받아놓고도 `KeyError: '지수명'` 으로
       통째로 날아갑니다. 이름은 우리가 INDEX_CODES 로 이미 알고 있으니 끕니다.
    """
    fn = getattr(stock, "get_index_ohlcv_by_date", None)
    if fn is not None:
        return _retry(fn, start, end, idx, "d", False)
    return _retry(stock.get_index_ohlcv, start, end, idx, "d", False)


def _is_etf(code: str) -> bool:
    """영문이 섞인 코드는 거의 전부 신규 ETF/ETN 입니다."""
    return not code.isdigit()


def _frame(code: str, start: str, end: str) -> tuple[pd.DataFrame, str]:
    """
    일반 종목이면 주식 OHLCV, ETF면 ETF OHLCV.
    둘 다 비면 반대쪽도 시도합니다 (코드 형태만으로는 확실치 않기 때문).
    """
    order = ["etf", "stock"] if _is_etf(code) else ["stock", "etf"]
    last_err: Exception | None = None
    for kind in order:
        try:
            df = _etf_ohlcv(code, start, end) if kind == "etf" else _ohlcv(code, start, end)
            if df is not None and not df.empty:
                return df, kind
        except Exception as e:  # pykrx는 없는 코드에 예외를 던지기도 합니다
            last_err = e
    if last_err:
        log.warning("조회 실패 %s: %s", code, last_err)
    return pd.DataFrame(), "unknown"


COMPACT_COLS = ["시가", "고가", "저가", "종가", "거래량"]


def _compact(df: pd.DataFrame, limit: int = MAX_ROWS) -> dict[str, Any]:
    """
    차트·지표 계산용 압축 포맷.

    왜 필요한가: 12개월 = 약 248봉인데, 봉마다 한글 키가 붙은 dict 로 돌려주면
    한 종목에 1.5만 토큰 가까이 됩니다. 지수 5종이면 그것만으로 세션 예산이
    말라붙습니다. 열 이름을 한 번만 쓰고 값은 배열로 보내 3~4배 줄입니다.

    반환: {"columns": ["date","시가",...], "rows": [["2026-01-02", 1,2,3,4,5], ...]}
    """
    if df is None or df.empty:
        return {"columns": [], "rows": []}
    df = df.tail(limit)
    cols = [c for c in COMPACT_COLS if c in df.columns]
    rows: list[list[Any]] = []
    for idx, row in df.iterrows():
        rec: list[Any] = [pd.Timestamp(idx).strftime("%Y-%m-%d")]
        for c in cols:
            v = row[c]
            if pd.isna(v):
                rec.append(None)
            elif isinstance(v, float):
                rec.append(round(v, 2))
            elif hasattr(v, "item"):
                rec.append(v.item())
            else:
                rec.append(v)
        rows.append(rec)
    return {"columns": ["date"] + cols, "rows": rows}


def _rows(df: pd.DataFrame, limit: int = MAX_ROWS) -> list[dict[str, Any]]:
    """DataFrame → JSON 직렬화 가능한 행 목록. 날짜는 YYYY-MM-DD."""
    if df is None or df.empty:
        return []
    df = df.tail(limit)
    out: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        rec: dict[str, Any] = {"date": pd.Timestamp(idx).strftime("%Y-%m-%d")}
        for col, val in row.items():
            if pd.isna(val):
                rec[str(col)] = None
            elif hasattr(val, "item"):
                rec[str(col)] = val.item()
            else:
                rec[str(col)] = val
        out.append(rec)
    return out


def _name(code: str) -> str | None:
    for fn in ("get_market_ticker_name", "get_etf_ticker_name"):
        f = getattr(stock, fn, None)
        if f is None:
            continue
        try:
            n = f(code)
            if n:
                return str(n)
        except Exception:
            continue
    return None


# ─────────────────────────────────────────────────────────────
# MCP 도구
# ─────────────────────────────────────────────────────────────

mcp = _Server("pykrx-korea-market")


@mcp.tool()
def health() -> dict[str, Any]:
    """서버 상태와 pykrx 응답 가능 여부를 확인합니다."""
    ok, detail = True, "ok"
    try:
        df = _index_ohlcv(INDEX_CODES["KOSPI"], days_ago(10), ymd(today_kst()))
        if df.empty:
            ok, detail = False, "pykrx가 빈 응답을 돌려줬습니다 (KRX 점검/휴장 가능)"
    except Exception as e:
        # 예외 메시지만으로는 원인을 못 찾는 경우가 많아 타입과 pykrx 버전을 같이 남깁니다.
        ok, detail = False, f"pykrx 호출 실패: {type(e).__name__}: {e}"
    return {
        "ok": ok,
        "detail": detail,
        "pykrx_version": getattr(__import__("pykrx"), "__version__", "unknown"),
        "server_time_kst": today_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "KRX via pykrx",
    }


@mcp.tool()
def get_quote(code: str) -> dict[str, Any]:
    """
    종목(주식·ETF·ETN)의 최근 거래일 시세를 조회합니다.

    `0219E0` 처럼 영문이 섞인 코드도 조회됩니다.
    반환되는 `as_of` 는 **실제 체결이 있었던 거래일**이며, 장중 실시간가가 아니라
    해당 거래일의 종가입니다. 브리핑에 쓸 때 반드시 기준일을 함께 표기하세요.

    Args:
        code: 6자리 종목코드 (예: "005930", "0219E0")
    """
    code = code.strip().upper()
    end = ymd(today_kst())
    df, kind = _frame(code, months_ago(13), end)

    if df.empty:
        return {
            "code": code,
            "found": False,
            "error": "해당 코드의 시세를 찾지 못했습니다. search_ticker 로 코드를 확인하세요.",
            "source": "KRX via pykrx",
        }

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else None
    close = float(last.get("종가", float("nan")))
    prev_close = float(prev.get("종가")) if prev is not None else None

    change = None
    change_pct = None
    if prev_close:
        change = round(close - prev_close, 2)
        change_pct = round((close - prev_close) / prev_close * 100, 2)

    # 52주 고저는 최근 1년 구간에서 직접 계산합니다 (pykrx가 직접 주지 않음).
    yr = df.tail(250)
    return {
        "code": code,
        "name": _name(code),
        "found": True,
        "kind": kind,
        "as_of": pd.Timestamp(df.index[-1]).strftime("%Y-%m-%d"),
        "as_of_note": "해당 거래일 종가. 장중 실시간가 아님.",
        "close": close,
        "open": float(last.get("시가")) if "시가" in last else None,
        "high": float(last.get("고가")) if "고가" in last else None,
        "low": float(last.get("저가")) if "저가" in last else None,
        "volume": int(last.get("거래량")) if "거래량" in last and not pd.isna(last.get("거래량")) else None,
        "change": change,
        "change_pct": change_pct,
        "high_52w": float(yr["고가"].max()) if "고가" in yr else None,
        "low_52w": float(yr["저가"].min()) if "저가" in yr else None,
        "nav": float(last["NAV"]) if "NAV" in last and not pd.isna(last["NAV"]) else None,
        "source": "KRX via pykrx",
    }


@mcp.tool()
def get_ohlcv(code: str, months: int = 12, compact: bool = True) -> dict[str, Any]:
    """
    종목의 일봉 OHLCV 이력을 조회합니다. **KIS의 100건 제한이 없습니다.**

    12개월 차트(일목균형표 78봉 포함)를 그리는 데 쓰세요.

    Args:
        code: 6자리 종목코드
        months: 조회 개월 수 (기본 12, 최대 24)
        compact: True(기본)면 columns/rows 배열로 압축해 반환합니다.
                 12개월이면 응답이 3~4배 작아집니다. **그대로 두세요.**
                 False 는 봉마다 한글 키가 붙은 dict 라 토큰을 크게 씁니다.
    """
    code = code.strip().upper()
    months = max(1, min(int(months), 24))
    df, kind = _frame(code, months_ago(months), ymd(today_kst()))
    out: dict[str, Any] = {
        "code": code,
        "name": _name(code),
        "kind": kind,
        "months": months,
        "source": "KRX via pykrx",
    }
    if compact:
        c = _compact(df)
        out["count"] = len(c["rows"])
        out["first_date"] = c["rows"][0][0] if c["rows"] else None
        out["last_date"] = c["rows"][-1][0] if c["rows"] else None
        out["columns"] = c["columns"]
        out["rows"] = c["rows"]
    else:
        bars = _rows(df)
        out["count"] = len(bars)
        out["first_date"] = bars[0]["date"] if bars else None
        out["last_date"] = bars[-1]["date"] if bars else None
        out["bars"] = bars
    return out


@mcp.tool()
def get_index_ohlcv(
    index: Literal["KOSPI", "KOSDAQ", "KOSPI200"] = "KOSPI",
    months: int = 12,
    compact: bool = True,
) -> dict[str, Any]:
    """
    국내 지수의 일봉 이력을 조회합니다. 12개월 지수 차트용.

    ⚠️ 빈 응답(count=0)이 오면 KRX가 비로그인 접근을 막은 것일 수 있습니다.
       Render 환경변수 `KRX_ID` / `KRX_PW` 를 설정하면 해결됩니다.

    Args:
        index: KOSPI | KOSDAQ | KOSPI200
        months: 조회 개월 수 (기본 12, 최대 24)
        compact: True(기본)면 columns/rows 배열로 압축. 그대로 두세요.
    """
    months = max(1, min(int(months), 24))
    idx = INDEX_CODES[index]
    out: dict[str, Any] = {
        "index": index, "krx_code": idx, "months": months,
        "krx_login": bool(os.environ.get("KRX_ID") and os.environ.get("KRX_PW")),
        "source": "KRX via pykrx",
    }
    try:
        df = _index_ohlcv(idx, months_ago(months), ymd(today_kst()))
    except Exception as e:
        out.update({"count": 0, "rows": [], "error": f"{type(e).__name__}: {e}"})
        return out

    if compact:
        c = _compact(df)
        out["count"] = len(c["rows"])
        out["first_date"] = c["rows"][0][0] if c["rows"] else None
        out["last_date"] = c["rows"][-1][0] if c["rows"] else None
        out["columns"] = c["columns"]
        out["rows"] = c["rows"]
    else:
        bars = _rows(df)
        out["count"] = len(bars)
        out["bars"] = bars
    if not out["count"]:
        out["hint"] = ("빈 응답입니다. KRX 비로그인 제한일 가능성이 큽니다 — "
                       "Render 에 KRX_ID / KRX_PW 를 설정해 보세요.")
    return out


@mcp.tool()
def get_investor_trading(code: str, days: int = 20) -> dict[str, Any]:
    """
    투자자별 순매매(개인·외국인·기관)를 조회합니다. 국내 시장 고유 수급 데이터입니다.

    ⚠️ 수급은 **사실 기록**입니다. 이걸 근거로 매수/매도 의견을 만들지 마세요.

    Args:
        code: 6자리 종목코드
        days: 조회 일수 (기본 20, 최대 120)
    """
    code = code.strip().upper()
    days = max(1, min(int(days), 120))
    try:
        df = _call(
            "get_market_trading_value_by_date",
            "get_market_trading_value_by_investor",
            days_ago(days), ymd(today_kst()), code,
        )
    except Exception as e:
        return {"code": code, "rows": [], "error": f"조회 실패: {e}",
                "source": "KRX via pykrx"}
    rows = _rows(df, limit=days)
    return {
        "code": code,
        "name": _name(code),
        "unit": "원 (순매수 금액)",
        "count": len(rows),
        "rows": rows,
        "note": "양수=순매수, 음수=순매도. 사실 기록이며 매매 권유가 아닙니다.",
        "source": "KRX via pykrx",
    }


@mcp.tool()
def get_short_selling(code: str, days: int = 20) -> dict[str, Any]:
    """
    공매도 거래량·잔고를 조회합니다.

    Args:
        code: 6자리 종목코드
        days: 조회 일수 (기본 20, 최대 120)
    """
    code = code.strip().upper()
    days = max(1, min(int(days), 120))
    out: dict[str, Any] = {"code": code, "name": _name(code), "source": "KRX via pykrx"}

    for key, primary, fallback in (
        ("volume", "get_shorting_volume_by_date", "get_shorting_status_by_date"),
        ("balance", "get_shorting_balance_by_date", "get_shorting_investor_volume_by_date"),
    ):
        try:
            df = _call(primary, fallback, days_ago(days), ymd(today_kst()), code)
            out[key] = _rows(df, limit=days)
        except Exception as e:
            out[key] = []
            out[f"{key}_error"] = str(e)

    return out


@mcp.tool()
def get_etf_detail(code: str, days: int = 20) -> dict[str, Any]:
    """
    ETF의 NAV·괴리율·추적오차를 조회합니다. ETF가 아니면 비어서 돌아옵니다.

    Args:
        code: 6자리 ETF 코드 (예: "0219E0", "069500")
        days: 조회 일수 (기본 20, 최대 120)
    """
    code = code.strip().upper()
    days = max(1, min(int(days), 120))
    start, end = days_ago(days), ymd(today_kst())
    out: dict[str, Any] = {"code": code, "name": _name(code), "source": "KRX via pykrx"}

    try:
        out["ohlcv_nav"] = _rows(_etf_ohlcv(code, start, end), limit=days)
    except Exception as e:
        out["ohlcv_nav"], out["ohlcv_nav_error"] = [], str(e)

    for key, fn_name in (("price_deviation", "get_etf_price_deviation"),
                         ("tracking_error", "get_etf_tracking_error")):
        fn = getattr(stock, fn_name, None)
        if fn is None:
            out[key] = []
            out[f"{key}_error"] = f"pykrx에 {fn_name} 없음"
            continue
        try:
            out[key] = _rows(fn(start, end, code), limit=days)
        except Exception as e:
            out[key], out[f"{key}_error"] = [], str(e)

    return out



@mcp.tool()
def screen_volume_breakout(
    min_cap_eok: float = 1000,
    vol_mult: float = 2.0,
    min_days: int = 3,
    ret_lo_pct: float = -3.0,
    ret_hi_pct: float = 5.0,
    dd_max_pct: float = -10.0,
    require_flow: bool = True,
    max_results: int = 30,
) -> dict[str, Any]:
    """
    전종목 스크리너: 거래량은 늘었지만 주가는 아직 안 움직인 종목을 찾습니다.

    조건(기본값):
      - 시가총액 min_cap_eok(1000억) 이상  ※ 단위: 억원
      - 20일 평균(최근 5일 제외) 대비 vol_mult(2배) 이상 거래량이 최근 5거래일 중 min_days(3)일 이상
      - 20일 등락률 ret_lo_pct(-3%) ~ ret_hi_pct(+5%)
      - 60일 종가 고점 대비 dd_max_pct(-10%) 이내
      - 외국인 또는 기관 20일 누적 순매수 > 0 (require_flow)
    결과에는 통과 종목(passed)과 조건 1개만 못 미친 종목(near_miss)이 함께 옵니다.
    첫 실행은 KRX 60거래일치를 받느라 1~3분 걸릴 수 있습니다. 결과는 사실 집계이며 매매 권유가 아닙니다.
    """
    return screener.run_screen(
        min_cap_억=min_cap_eok, vol_mult=vol_mult, min_days=int(min_days),
        ret_lo_pct=ret_lo_pct, ret_hi_pct=ret_hi_pct, dd_max_pct=dd_max_pct,
        require_flow=require_flow, max_results=int(max_results),
    )


@mcp.tool()
def screen_momentum(min_cap_eok: float = 1000, min_score: int = 4, max_results: int = 20,
                    exclude_overheated_pct: float = 40.0) -> dict[str, Any]:
    """
    규칙 기반 모멘텀 스크리너 (예측 아님). 6개 조건 충족 수(0~6)로 점수화해 상위 종목을 보여줍니다:
    60일 수익률>10%, 20일 수익률>0, 정배열(종가>20일선>60일선), 52주 고점 -5% 이내,
    20일 평균 거래대금 > 직전 60일 평균, 외국인 또는 기관 20일 순매수.
    20일 수익률이 exclude_overheated_pct(40%)를 넘는 과열 종목은 제외합니다.
    결과는 현재 상태의 분류이며 매매 권유가 아닙니다.
    """
    return screener.run_momentum(min_cap_억=min_cap_eok, min_score=int(min_score),
                                 max_results=int(max_results), exclude_overheated_pct=exclude_overheated_pct)


@mcp.tool()
def backtest_volume_breakout(min_cap_eok: float = 1000, vol_mult: float = 2.0, min_days: int = 3,
                             ret_lo_pct: float = -3.0, ret_hi_pct: float = 5.0, dd_max_pct: float = -10.0,
                             hold_days: int = 20, lookback_days: int = 250, horizons: str = "5,10,20,40",
                             step: int = 1, require_flow: bool = False, cap_mode: str = "scaled",
                             cooldown: int = 20, max_signals_listed: int = 40) -> dict[str, Any]:
    """
    거래량 급증·주가 미반영 조건(screen_volume_breakout 과 같은 정의)의 백테스트.

    최근 lookback_days(기본 250거래일 ≈ 1년) 중 60일째부터 매 거래일(step) 조건을 적용해, 통과 종목의
    horizons(기본 5,10,20,40일) 뒤 수익률·승률·중앙값, 시장지수(종목별 KOSPI/KOSDAQ) 대비 초과수익,
    같은 날 시총 조건을 만족한 전종목 평균(universe) 대비 성과, 보유 중 최고/최저를 집계합니다.
    hold_days 는 월별·지속일별·개별 신호 상세에 쓰는 기준 보유기간입니다.
    같은 종목의 연속 신호(중복 표본)를 뺀 첫 신호만의 집계는 entries_by_horizon 에 있습니다(cooldown 거래일 기준).
    cap_mode="scaled"(기본) 는 최신 시총을 신호일 가격으로 환산해 미래참조 편향을 줄입니다. "latest" 는 최신 시총 고정.

    캐시가 lookback_days 보다 적으면 백그라운드로 받기 시작하고 status="preparing" 을 바로 돌려주므로,
    screener_cache_status 로 진행률을 본 뒤 다시 호출하세요(하루당 1~2초, 1년치 약 5~8분).
    require_flow=True 면 신호일마다 수급(외국인·기관 20일 순매수)을 추가로 받아야 해서 status="preparing_flows" 가
    먼저 올 수 있습니다. 결과는 과거 통계이며 매매 권유가 아닙니다.
    """
    return screener.run_backtest(min_cap_억=min_cap_eok, vol_mult=vol_mult, min_days=int(min_days),
                                 ret_lo_pct=ret_lo_pct, ret_hi_pct=ret_hi_pct, dd_max_pct=dd_max_pct,
                                 hold_days=int(hold_days), step=int(step), require_flow=require_flow,
                                 max_signals_listed=int(max_signals_listed), lookback_days=int(lookback_days),
                                 horizons=horizons, cap_mode=cap_mode, cooldown=int(cooldown))


@mcp.tool()
def backtest_exit_rules(min_cap_eok: float = 1000, vol_mult: float = 3.0, min_days: int = 3,
                        ret_lo_pct: float = -3.0, ret_hi_pct: float = 5.0, dd_max_pct: float = -10.0,
                        tp_pct: str = "10", sl_pct: str = "7", max_hold: str = "10", exit_mode: str = "intraday",
                        cost_pct: float = 0.3, lookback_days: int = 250, step: int = 1, require_flow: bool = False,
                        cap_mode: str = "scaled", max_trades_listed: int = 30) -> dict[str, Any]:
    """
    거래량 급증·주가 미반영 신호(backtest_volume_breakout 과 같은 조건)에 익절·손절·최대보유 청산 규칙을 적용한
    거래 단위 백테스트. 신호일 종가 진입 → 다음 날부터 익절선(tp_pct) 또는 손절선(sl_pct)에 닿으면 청산,
    max_hold 거래일째면 종가 청산. 왕복 거래비용 cost_pct(기본 0.3%)를 뺀 순수익률로 집계합니다.

    tp_pct / sl_pct / max_hold 는 "10,15" 처럼 쉼표로 여러 값을 주면 조합(최대 12개)을 한 번에 비교합니다. 0 은 해당 규칙 미사용.
    exit_mode="intraday"(기본) 는 장중 고가·저가 터치로 판정(갭은 시가 체결, 같은 날 양쪽 터치는 손절 우선), "close" 는 종가로만 판정.
    결과: trades(같은 종목은 청산 후에만 재진입하는 비중복 거래), signals_all(신호마다 독립 진입),
    universe_same_rule(시총 조건 통과 전종목에 같은 규칙 적용 = 아무 종목이나 골랐을 때), plain_hold_same_entries(규칙 없이 max_hold 보유).
    캐시가 부족하면 status="preparing" 이 먼저 올 수 있습니다(screener_cache_status 로 확인 후 재호출).
    결과는 과거 통계이며 매매 권유가 아닙니다.
    """
    return screener.run_exit_backtest(min_cap_억=min_cap_eok, vol_mult=vol_mult, min_days=int(min_days),
                                      ret_lo_pct=ret_lo_pct, ret_hi_pct=ret_hi_pct, dd_max_pct=dd_max_pct,
                                      tp_pct=tp_pct, sl_pct=sl_pct, max_hold=max_hold, exit_mode=exit_mode,
                                      cost_pct=cost_pct, step=int(step), require_flow=require_flow,
                                      lookback_days=int(lookback_days), cap_mode=cap_mode,
                                      max_trades_listed=int(max_trades_listed))


@mcp.tool()
def screen_accumulation(min_cap_eok: float = 1000, accum_mult: float = 1.5, max_spike: float = 5.0,
                        ret_lo_pct: float = -3.0, ret_hi_pct: float = 5.0, dd_max_pct: float = -10.0,
                        per_max: float = 25.0, pbr_max: float = 1.5, require_flow: bool = True,
                        flow_mode: str = "any", max_results: int = 30) -> dict[str, Any]:
    """
    누적매집형 + 저평가 스크리너(최근 거래일 기준). 하루 폭발이 아니라 한 달에 걸쳐 조용히 쌓인 거래량을 잡습니다.
      - 최근 20일 평균 거래량이 이전 60일 평균의 accum_mult(1.5)배 이상
      - 그 20일 안에 하루 max_spike(5)배 이상 폭발한 날 없음(뉴스·테마 반응 제외)
      - 20일 등락률 ret_lo_pct~ret_hi_pct, 60일 고점 대비 dd_max_pct 이내
      - 저평가: 0 < PER <= per_max, 0 < PBR <= pbr_max (적자·자본잠식 제외). 0 이하를 주면 미적용
      - 수급: 외국인 또는 기관(flow_mode="any") / 둘 다(flow_mode="both") 20일 순매수 > 0
      - 시총 min_cap_eok 억 이상. 상장 80거래일 미만 종목은 자동 제외.
    결과는 사실 집계이며 매매 권유가 아닙니다.
    """
    return screener.run_accum_screen(min_cap_억=min_cap_eok, accum_mult=accum_mult, max_spike=max_spike,
                                     ret_lo_pct=ret_lo_pct, ret_hi_pct=ret_hi_pct, dd_max_pct=dd_max_pct,
                                     per_max=(per_max if per_max and per_max > 0 else None),
                                     pbr_max=(pbr_max if pbr_max and pbr_max > 0 else None),
                                     require_flow=require_flow, flow_mode=flow_mode, max_results=int(max_results))


@mcp.tool()
def backtest_accumulation(min_cap_eok: float = 1000, accum_mult: float = 1.5, max_spike: float = 5.0,
                          ret_lo_pct: float = -3.0, ret_hi_pct: float = 5.0, dd_max_pct: float = -10.0,
                          per_max: float = 25.0, pbr_max: float = 1.5, require_flow: bool = False,
                          flow_mode: str = "any", hold_days: int = 20, horizons: str = "5,10,20,40",
                          lookback_days: int = 250, step: int = 1, cap_mode: str = "scaled",
                          cooldown: int = 20, max_signals_listed: int = 40) -> dict[str, Any]:
    """
    screen_accumulation 과 같은 조건(누적매집형 + 저평가)의 백테스트. 집계 방식은 backtest_volume_breakout 과 동일
    (horizons 별 수익률·승률·중앙값, 종목별 시장지수 대비, 유니버스 대비, 첫 신호만 집계 등).
    per_max/pbr_max 에 0 이하를 주면 저평가 조건 미적용. require_flow=True 면 신호일 수급을 받느라
    status="preparing_flows" 가 먼저 올 수 있습니다. 결과는 과거 통계이며 매매 권유가 아닙니다.
    """
    return screener.run_backtest(min_cap_억=min_cap_eok, vol_mult=2.0, min_days=3,
                                 ret_lo_pct=ret_lo_pct, ret_hi_pct=ret_hi_pct, dd_max_pct=dd_max_pct,
                                 hold_days=int(hold_days), step=int(step), require_flow=require_flow,
                                 max_signals_listed=int(max_signals_listed), lookback_days=int(lookback_days),
                                 horizons=horizons, cap_mode=cap_mode, cooldown=int(cooldown),
                                 mode="accum", accum_mult=accum_mult, max_spike=max_spike,
                                 per_max=(per_max if per_max and per_max > 0 else None),
                                 pbr_max=(pbr_max if pbr_max and pbr_max > 0 else None), flow_mode=flow_mode)


@mcp.tool()
def backtest_accumulation_exit(min_cap_eok: float = 1000, accum_mult: float = 1.5, max_spike: float = 5.0,
                               ret_lo_pct: float = -3.0, ret_hi_pct: float = 5.0, dd_max_pct: float = -10.0,
                               per_max: float = 25.0, pbr_max: float = 1.5, require_flow: bool = False,
                               flow_mode: str = "any", tp_pct: str = "10", sl_pct: str = "7", max_hold: str = "10",
                               exit_mode: str = "intraday", cost_pct: float = 0.3, lookback_days: int = 250,
                               step: int = 1, cap_mode: str = "scaled", max_trades_listed: int = 30) -> dict[str, Any]:
    """
    누적매집형 + 저평가 신호에 익절·손절·최대보유 규칙을 적용한 거래 단위 백테스트(backtest_exit_rules 와 같은 집계).
    tp_pct/sl_pct/max_hold 는 "10,15" 처럼 여러 값(조합 최대 12개). 결과는 과거 통계이며 매매 권유가 아닙니다.
    """
    return screener.run_exit_backtest(min_cap_억=min_cap_eok, vol_mult=2.0, min_days=3,
                                      ret_lo_pct=ret_lo_pct, ret_hi_pct=ret_hi_pct, dd_max_pct=dd_max_pct,
                                      tp_pct=tp_pct, sl_pct=sl_pct, max_hold=max_hold, exit_mode=exit_mode,
                                      cost_pct=cost_pct, step=int(step), require_flow=require_flow,
                                      lookback_days=int(lookback_days), cap_mode=cap_mode,
                                      max_trades_listed=int(max_trades_listed),
                                      mode="accum", accum_mult=accum_mult, max_spike=max_spike,
                                      per_max=(per_max if per_max and per_max > 0 else None),
                                      pbr_max=(pbr_max if pbr_max and pbr_max > 0 else None), flow_mode=flow_mode)


@mcp.tool()
def backtest_momentum(min_cap_eok: float = 1000, min_score: int = 4, r60_min_pct: float = 10.0,
                      high_pct: float = -5.0, overheat_pct: float = 40.0, require_flow: bool = False,
                      flow_mode: str = "any", per_max: float = 0, pbr_max: float = 0,
                      hold_days: int = 20, horizons: str = "5,10,20,40", lookback_days: int = 500,
                      step: int = 1, cap_mode: str = "scaled", cooldown: int = 20,
                      max_signals_listed: int = 40) -> dict[str, Any]:
    """
    모멘텀형 신호의 백테스트(집계 방식은 backtest_volume_breakout 과 동일).
    가격 조건 5개(60일 수익률>r60_min_pct, 20일 수익률>0, 종가>20일선>60일선, 52주 고점 high_pct 이내,
    20일 평균 거래대금>직전 60일) 중 min_score 개 이상이면 신호. 20일 수익률이 overheat_pct 를 넘으면 과열로 제외.
    require_flow=True 면 외국인/기관 20일 순매수(flow_mode any/both)도 요구(수급 준비에 시간이 걸릴 수 있음).
    per_max/pbr_max 는 0 이면 미적용. by_spike_days 항목은 이 도구에서는 "점수별" 집계입니다.
    결과는 과거 통계이며 매매 권유가 아닙니다.
    """
    return screener.run_backtest(min_cap_억=min_cap_eok, vol_mult=2.0, min_days=3,
                                 ret_lo_pct=-100.0, ret_hi_pct=1000.0, dd_max_pct=-100.0,
                                 hold_days=int(hold_days), step=int(step), require_flow=require_flow,
                                 max_signals_listed=int(max_signals_listed), lookback_days=int(lookback_days),
                                 horizons=horizons, cap_mode=cap_mode, cooldown=int(cooldown),
                                 mode="momentum", per_max=(per_max if per_max and per_max > 0 else None),
                                 pbr_max=(pbr_max if pbr_max and pbr_max > 0 else None), flow_mode=flow_mode,
                                 mom_min_score=int(min_score), mom_r60_min=r60_min_pct / 100.0,
                                 mom_high_pct=high_pct / 100.0, mom_overheat=overheat_pct / 100.0)


@mcp.tool()
def backtest_momentum_exit(min_cap_eok: float = 1000, min_score: int = 4, r60_min_pct: float = 10.0,
                           high_pct: float = -5.0, overheat_pct: float = 40.0, require_flow: bool = False,
                           flow_mode: str = "any", tp_pct: str = "15", sl_pct: str = "7", max_hold: str = "20",
                           exit_mode: str = "intraday", cost_pct: float = 0.3, lookback_days: int = 500,
                           step: int = 1, cap_mode: str = "scaled", max_trades_listed: int = 30) -> dict[str, Any]:
    """모멘텀형 신호(backtest_momentum 과 같은 조건)에 익절·손절·최대보유 규칙을 적용한 거래 단위 백테스트.
    tp_pct/sl_pct/max_hold 는 "10,15" 처럼 여러 값(조합 최대 12개). 결과는 과거 통계이며 매매 권유가 아닙니다."""
    return screener.run_exit_backtest(min_cap_억=min_cap_eok, vol_mult=2.0, min_days=3,
                                      ret_lo_pct=-100.0, ret_hi_pct=1000.0, dd_max_pct=-100.0,
                                      tp_pct=tp_pct, sl_pct=sl_pct, max_hold=max_hold, exit_mode=exit_mode,
                                      cost_pct=cost_pct, step=int(step), require_flow=require_flow,
                                      lookback_days=int(lookback_days), cap_mode=cap_mode,
                                      max_trades_listed=int(max_trades_listed), mode="momentum", flow_mode=flow_mode,
                                      mom_min_score=int(min_score), mom_r60_min=r60_min_pct / 100.0,
                                      mom_high_pct=high_pct / 100.0, mom_overheat=overheat_pct / 100.0)


@mcp.tool()
def screener_prefetch(days: int = 250) -> dict[str, Any]:
    """전종목 일별 데이터를 days 거래일만큼 백그라운드로 미리 받아 둡니다(백테스트 기간 확장용, 최대 500).
    바로 돌아오며, 진행률은 screener_cache_status 로 확인합니다."""
    days = max(60, min(int(days), 500))
    screener.prefetch(days)
    return {"ok": True, "requested_days": days, **screener.cache_status()}


@mcp.tool()
def screener_cache_status() -> dict[str, Any]:
    """스크리너 캐시 상태(받아둔 거래일 수·기간, 백그라운드 준비 진행률, 수급 캐시)를 확인합니다."""
    return screener.cache_status()


# ─────────────────────────────────────────────────────────────
# 팩터(횡단면 순위) 백테스트·스크리너 — 학계·실전 퀀트 방식
#   매월 말 유니버스 전종목을 팩터 백분위 순위로 점수화 → 상위 포트폴리오 → 다음 달 수익률.
#   10년 월간 리밸런싱, 10분위, IC, t-통계량. 자세한 설명은 factors.py 머리말.
# ─────────────────────────────────────────────────────────────
@mcp.tool()
def factor_prefetch(years: float = 10) -> dict[str, Any]:
    """팩터 백테스트용 월말 스냅샷(전종목 시세·수정주가 월간수익률·PER/PBR)을 years 년 + 13개월만큼 백그라운드로 받아 둡니다.
    월말당 KRX 3회(약 5~7초) → 10년이면 약 15분. 바로 돌아오며 진행률은 factor_cache_status 로 확인합니다.
    서버 시작 시 자동으로 10년치를 받기 시작하므로 보통은 호출할 필요가 없습니다(기간을 늘릴 때만)."""
    years = max(2.0, min(float(years), 20.0))
    return factors.prefetch(years)


@mcp.tool()
def factor_cache_status() -> dict[str, Any]:
    """팩터 캐시 상태: 받아둔 월말 수·기간, 진행률, 월간 수익률 출처(KRX 수정주가 / 하이브리드 / 종가 폴백)와 기준가 일치율, 메모리."""
    return factors.cache_status()


@mcp.tool()
def factor_month_debug(ymd: str = "", n: int = 20) -> dict[str, Any]:
    """팩터 캐시 월별 데이터 품질 진단. ymd 를 비우면 모든 월말의 [수익률 출처, 기준가 일치율, KRX 사용비율, 등락률 시작일, 상장수] 표.
    ymd(예: 20240531)를 주면 그 달의 전종목등락률을 다시 받아 기준가가 직전 월말 종가와 다른 종목 n 개(이름·기준가·주식수 변동·수익률)를 보여줍니다."""
    return factors.month_debug(ymd=str(ymd or ""), n=int(n))


@mcp.tool()
def backtest_factors(factors_spec: str = "value,quality,lowvol,mom", min_cap_eok: float = 1000, years: float = 10,
                     top_pct: float = 10, top_n: int = 0, hold_months: int = 1, exclude_managed: bool = True,
                     min_price: float = 1000, include_single_factor_table: bool = True,
                     max_months_listed: int = 130) -> dict[str, Any]:
    """학계식 팩터 백테스트: 매월 말 유니버스(보통주, 시총 min_cap_eok억↑) 전종목을 팩터 백분위 순위로 점수화해
    상위 top_pct%(또는 top_n 종목)를 동일가중 매수, hold_months 개월 보유(월말 리밸런싱), 다음 달 수익률을
    유니버스 동일가중 평균·종목별 시장 지수와 비교합니다. years 년(기본 10년 = 120개월).

    factors_spec: 쉼표로 묶은 팩터 이름. 그룹 value(=bm+ep+dy 순위 평균), quality(=roe) 또는 개별
      bm(1/PBR) ep(EPS/P) dy(배당수익률) roe(EPS/BPS) mom(12-1개월 모멘텀) rev(1개월 반전: 낮을수록 좋음)
      lowvol(12개월 저변동성) high52(52주고가 근접) size(소형주) lowturn(저회전율). 복합점수 = 팩터 순위 평균.

    결과: criteria_academic(사전 기준 5개 통과/미달: 월평균 초과 +0.3%p↑, t≥2, 월 적중률 55%↑, 전·후반 모두 플러스,
    10분위 단조성), portfolio(월평균·초과·t·적중률·CAGR·MDD, 지수 대비), ic(순위상관), deciles(10분위),
    top30_fixed(상위 30종목 고정 버전), by_year, monthly, single_factors(팩터별 단독 성과표 — 어떤 팩터가 한국에서
    먹히는지), data_quality. 캐시가 아직 없으면 status="preparing" 과 진행률을 돌려줍니다.
    결과는 과거 통계이며 매매 권유가 아닙니다."""
    return factors.run_backtest(factors=factors_spec, min_cap_억=min_cap_eok, years=float(years), top_pct=float(top_pct),
                                top_n=int(top_n), hold_months=int(hold_months), exclude_managed=bool(exclude_managed),
                                min_price=float(min_price), include_single_factor_table=bool(include_single_factor_table),
                                max_months_listed=int(max_months_listed))


@mcp.tool()
def paper_test(show_all_cohorts: bool = False) -> dict[str, Any]:
    """표본 외(paper) 검증 보고 — 2026-11-07 에 사전 등록한 규칙 '저PBR 상위 30종목·12개월 보유(시총 1,000억↑ 보통주)'를
    그대로 적용합니다. 규칙 파라미터는 받지 않습니다(바꾸면 표본 외 검증이 아님).
    돌려주는 것: registration(등록된 규칙·판정 규칙·표본 내 참고치), status(진행 n/24개월 또는 판정), oos_summary(표본 외 월평균
    초과수익·적중률·t·누적 수익률 vs 유니버스·지수), oos_monthly, cohorts(이번 달 편입 30종목 포함; show_all_cohorts=True 면 모든
    코호트의 종목), in_sample_reproduction(표본 내 구간 재계산 — 등록 참고치와 거의 같아야 함).
    직전 완료 달의 월말 데이터가 없으면 먼저 받아옵니다(약 10초). 과거·현재 통계이며 매매 권유가 아닙니다."""
    return factors.run_paper_test(show_all_cohorts=bool(show_all_cohorts))


@mcp.tool()
def screen_factors(factors_spec: str = "value,quality,lowvol,mom", min_cap_eok: float = 1000, top_n: int = 30,
                   exclude_managed: bool = True, min_price: float = 1000) -> dict[str, Any]:
    """최근 거래일 기준 팩터 복합점수 상위 종목(backtest_factors 와 같은 정의·유니버스). 종목별로 복합점수 백분위,
    팩터별 순위, PBR·PER·배당·ROE·12-1모멘텀·1개월수익률·12개월변동성·52주고가·회전율을 함께 돌려줍니다.
    최근 거래일 시세를 KRX 에서 3회 받아 하루 캐시합니다(약 5~10초). 월말 캐시 13개가 없으면 status="preparing".
    사실 데이터 집계이며 매매 권유가 아닙니다 — 백테스트에서 기준을 통과한 조합만 쓰세요."""
    return factors.run_screen(factors=factors_spec, min_cap_억=min_cap_eok, top_n=int(top_n),
                              exclude_managed=bool(exclude_managed), min_price=float(min_price))


@mcp.tool()
def search_ticker(query: str, limit: int = 20) -> dict[str, Any]:
    """
    종목명 일부로 코드를 찾습니다. 주식과 ETF를 함께 검색합니다.

    Args:
        query: 종목명 일부 (예: "커버드콜", "삼성전자")
        limit: 최대 결과 수 (기본 20, 최대 50)
    """
    query = query.strip()
    limit = max(1, min(int(limit), 50))
    # 휴장일에는 당일 목록이 비므로 최근 영업일로 되돌아갑니다.
    base = None
    for back in range(0, 10):
        d = ymd(today_kst() - timedelta(days=back))
        try:
            if stock.get_market_ticker_list(d, market="ALL"):
                base = d
                break
        except Exception:
            continue
    if base is None:
        return {"query": query, "results": [], "error": "종목 목록을 가져오지 못했습니다."}

    results: list[dict[str, str]] = []
    try:
        codes = list(stock.get_market_ticker_list(base, market="ALL"))
    except Exception:
        codes = []
    try:
        codes += list(stock.get_etf_ticker_list(base))
    except Exception:
        pass

    for c in codes:
        if len(results) >= limit:
            break
        n = _name(c)
        if n and query in n:
            results.append({"code": c, "name": n})

    return {
        "query": query,
        "base_date": base,
        "count": len(results),
        "results": results,
        "source": "KRX via pykrx",
    }


# ─────────────────────────────────────────────────────────────
# HTTP 앱 — Bearer 토큰 인증
# ─────────────────────────────────────────────────────────────

TOKEN = os.environ.get("MCP_TOKEN", "").strip()

# ⚠️ /robots.txt 를 인증 뒤에 두면 안 됩니다.
#    401 이 돌아오면 Claude 의 web_fetch 가 "robots_disallowed" 로 간주해
#    이 도메인 전체를 거부합니다 (실측 2026-10-27 08:15 브리핑 실패 원인).
#    크롤링 걱정은 불필요합니다 — 실제 데이터는 전부 토큰 뒤에 있습니다.
PUBLIC_PATHS = ("/", "/healthz", "/robots.txt", "/kakao/callback")


class TokenAuth:
    """
    토큰을 두 가지 방법으로 받습니다.

    1) 헤더:  Authorization: Bearer <MCP_TOKEN>
    2) 경로:  /t/<MCP_TOKEN>/mcp      ← 접두사를 떼고 /mcp 로 넘깁니다

    (2)가 필요한 이유: Claude 커스텀 커넥터의 "요청 헤더" 기능은 커스텀 헤더
    이름마다 Anthropic 승인이 필요하고, `Authorization` 은 Claude가 OAuth용으로
    이미 쓰고 있어 커스텀 헤더로 지정할 수 없습니다. 경로에 토큰을 넣으면
    헤더 설정 없이 "No sign in" 만으로 붙습니다.

    ⚠️ 경로 방식은 토큰이 URL에 노출됩니다. 접근 로그에 남을 수 있으니
       이 URL을 공유하지 마세요. 유출이 의심되면 Render에서 MCP_TOKEN 을
       새 값으로 바꾸면 기존 URL은 즉시 무효가 됩니다.

    BaseHTTPMiddleware 가 아니라 순수 ASGI 미들웨어인 이유는 경로를 고쳐 써야
    하기 때문입니다.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        path: str = scope.get("path", "")

        if path in PUBLIC_PATHS or not TOKEN:
            return await self.app(scope, receive, send)

        # (2) 경로 방식
        prefix = f"/t/{TOKEN}"
        if path == prefix or path.startswith(prefix + "/"):
            rest = path[len(prefix):] or "/"
            scope = dict(scope)
            scope["path"] = rest
            scope["raw_path"] = rest.encode()
            return await self.app(scope, receive, send)

        # (1) 헤더 방식
        for k, v in scope.get("headers", []):
            if k == b"authorization" and v.decode(errors="ignore") == f"Bearer {TOKEN}":
                return await self.app(scope, receive, send)

        await JSONResponse({"error": "unauthorized"}, status_code=401)(scope, receive, send)


# ─────────────────────────────────────────────────────────────
# REST (GET) 엔드포인트
#
# 왜 필요한가: 예약 작업 세션에 커스텀 커넥터가 노출되지 않는 경우가 있습니다
# (2026-10-27 실측: 07:22 실행에서 "pykrx 커넥터 세션에 미등록").
# web_fetch 는 GET 전용·헤더 지정 불가지만 **어떤 세션에서도 동작**하므로,
# 토큰이 경로에 들어간 GET 엔드포인트를 열어 커넥터 없이도 쓸 수 있게 합니다.
#
#   GET /t/<MCP_TOKEN>/api/health
#   GET /t/<MCP_TOKEN>/api/quote/0219E0
#   GET /t/<MCP_TOKEN>/api/ohlcv/069500?months=12
#   GET /t/<MCP_TOKEN>/api/index/KOSPI?months=12
#   GET /t/<MCP_TOKEN>/api/etf/0219E0?days=20
#   GET /t/<MCP_TOKEN>/api/investor/005930?days=20
#   GET /t/<MCP_TOKEN>/api/short/005930?days=20
#   GET /t/<MCP_TOKEN>/api/search?q=커버드콜
#
# 응답 JSON은 같은 이름의 MCP 도구와 **완전히 동일**합니다.
# 인증은 TokenAuth 미들웨어가 경로 접두사로 이미 처리합니다.
# ─────────────────────────────────────────────────────────────

def _int_param(request, name, default):
    try:
        return int(request.query_params.get(name, default))
    except (TypeError, ValueError):
        return default


def _json(obj):
    return JSONResponse(obj, media_type="application/json; charset=utf-8")


async def api_health(_request):
    return _json(health())


async def api_quote(request):
    return _json(get_quote(request.path_params["code"]))


async def api_ohlcv(request):
    return _json(get_ohlcv(request.path_params["code"],
                           months=_int_param(request, "months", 12)))


async def api_index(request):
    idx = request.path_params["index"].upper()
    if idx not in INDEX_CODES:
        return JSONResponse(
            {"error": f"알 수 없는 지수: {idx}",
             "allowed": sorted(INDEX_CODES)}, status_code=400)
    return _json(get_index_ohlcv(idx, months=_int_param(request, "months", 12)))


async def api_etf(request):
    return _json(get_etf_detail(request.path_params["code"],
                                days=_int_param(request, "days", 20)))


async def api_investor(request):
    return _json(get_investor_trading(request.path_params["code"],
                                      days=_int_param(request, "days", 20)))


async def api_short(request):
    return _json(get_short_selling(request.path_params["code"],
                                   days=_int_param(request, "days", 20)))


def _bars_for_indicators(df: pd.DataFrame) -> list[dict[str, Any]]:
    """indicators.analyze 가 먹는 dict 배열로 변환 (한글 키는 normalize 가 인식)."""
    out = []
    for idx, row in df.iterrows():
        rec: dict[str, Any] = {"date": pd.Timestamp(idx).strftime("%Y-%m-%d")}
        for k in ("시가", "고가", "저가", "종가", "거래량"):
            if k in row and not pd.isna(row[k]):
                v = row[k]
                rec[k] = v.item() if hasattr(v, "item") else v
        out.append(rec)
    return out


async def _indicators_payload(df, label, months, kind=None):
    """
    일봉을 서버에서 계산해 **결과만** 돌려줍니다.

    왜: 12개월 일봉(약 248봉)을 그대로 보내면 종목당 5~6천 토큰이라
    보유 종목 + 지수를 합치면 세션 전송량 한도를 넘습니다
    (실측 2026-10-28 07:19: 지표 5종목·차트 전부 생략됨).
    계산 결과만 보내면 **약 450 토큰** 으로 13배 줄어듭니다.
    """
    bars = _bars_for_indicators(df)
    if len(bars) < 15:
        return {"name": label, "months": months, "ok": False,
                "reason": f"봉 부족 ({len(bars)}개)", "source": "KRX via pykrx"}
    res = indicators.analyze(bars)
    res["name"] = label
    res["months"] = months
    if kind:
        res["kind"] = kind
    res["source"] = "KRX via pykrx"
    return res


async def api_indicators(request):
    code = request.path_params["code"].strip().upper()
    months = max(3, min(_int_param(request, "months", 12), 24))
    df, kind = _frame(code, months_ago(months), ymd(today_kst()))
    return _json(await _indicators_payload(df, _name(code) or code, months, kind))


async def api_idx_indicators(request):
    idx = request.path_params["index"].upper()
    if idx not in INDEX_CODES:
        return JSONResponse({"error": f"알 수 없는 지수: {idx}",
                             "allowed": sorted(INDEX_CODES)}, status_code=400)
    months = max(3, min(_int_param(request, "months", 12), 24))
    try:
        df = _index_ohlcv(INDEX_CODES[idx], months_ago(months), ymd(today_kst()))
    except Exception as e:
        return _json({"name": idx, "months": months, "ok": False,
                      "error": f"{type(e).__name__}: {e}",
                      "hint": "KRX 접근 실패. KRX_ID/KRX_PW 설정 여부를 확인하세요.",
                      "source": "KRX via pykrx"})
    return _json(await _indicators_payload(df, idx, months, "index"))


CHART_COLS = ["고가", "저가", "종가", "거래량"]      # 시가는 차트에 쓰지 않음


def _chartdata(df: pd.DataFrame, label: str, months: int) -> dict[str, Any]:
    """
    차트 전용 최소 필드. make_charts.py 는 종가선·일목·MACD·RSI·OBV 만 그리므로
    **시가는 필요 없습니다.** 소수점도 버려 전송량을 더 줄입니다.
    pykrx_adapter.py 가 그대로 받습니다.
    """
    cols = [c for c in CHART_COLS if c in df.columns]
    rows = []
    for idx, row in df.iterrows():
        rec: list[Any] = [pd.Timestamp(idx).strftime("%Y-%m-%d")]
        for c in cols:
            v = row[c]
            rec.append(None if pd.isna(v) else (round(float(v), 2) if c != "거래량" else int(v)))
        rows.append(rec)
    return {"name": label, "months": months, "count": len(rows),
            "columns": ["date"] + cols, "rows": rows, "source": "KRX via pykrx"}


async def api_chartdata(request):
    code = request.path_params["code"].strip().upper()
    months = max(3, min(_int_param(request, "months", 12), 24))
    df, _k = _frame(code, months_ago(months), ymd(today_kst()))
    return _json(_chartdata(df, _name(code) or code, months))


async def api_idx_chartdata(request):
    idx = request.path_params["index"].upper()
    if idx not in INDEX_CODES:
        return JSONResponse({"error": f"알 수 없는 지수: {idx}",
                             "allowed": sorted(INDEX_CODES)}, status_code=400)
    months = max(3, min(_int_param(request, "months", 12), 24))
    try:
        df = _index_ohlcv(INDEX_CODES[idx], months_ago(months), ymd(today_kst()))
    except Exception as e:
        return _json({"name": idx, "months": months, "count": 0,
                      "columns": [], "rows": [],
                      "error": f"{type(e).__name__}: {e}",
                      "hint": "KRX 접근 실패. KRX_ID/KRX_PW 설정 여부를 확인하세요.",
                      "source": "KRX via pykrx"})
    return _json(_chartdata(df, idx, months))


# ─────────────────────────────────────────────────────────────
# 카카오톡 "나에게 보내기"
#
# 왜 서버에 넣나: PlayMCP 의 KakaotalkChat-MemoChat 은 예약 세션에서 인증이
# 반나절이면 풀립니다 (실측 2026-10-29~30: 카톡 발송 7회 중 6회 실패).
# 리프레시 토큰을 서버가 들고 자동 갱신하면 수동 재연결이 필요 없습니다.
#
# ⚠️ web_fetch 는 GET 전용이라 POST 를 쓸 수 없습니다.
#    그래서 메시지를 쿼리스트링으로 받습니다 (부작용 있는 GET — 비표준이지만 불가피).
#    메시지 내용이 서버 접근 로그에 남을 수 있습니다. 비밀값을 보내지 마세요.
#
# 환경변수 (Render):
#   KAKAO_REST_API_KEY   카카오 디벨로퍼스 앱의 REST API 키
#   KAKAO_REFRESH_TOKEN  최초 1회 OAuth 동의로 발급 (아래 /kakao/setup 참고)
# ─────────────────────────────────────────────────────────────

KAKAO_KEY = os.environ.get("KAKAO_REST_API_KEY", "").strip()
KAKAO_REFRESH = os.environ.get("KAKAO_REFRESH_TOKEN", "").strip()
# 카카오는 REST API 키 생성 시 클라이언트 시크릿을 기본 활성화합니다.
# 활성화 상태면 토큰 요청에 client_secret 을 반드시 포함해야 합니다.
KAKAO_SECRET = os.environ.get("KAKAO_CLIENT_SECRET", "").strip()
KAKAO_REDIRECT = os.environ.get(
    "KAKAO_REDIRECT_URI", "https://pykrx-mcp.onrender.com/kakao/callback").strip()

_kakao_tok: dict[str, Any] = {"access": None, "exp": 0.0}


def _kakao_access_token() -> str:
    """리프레시 토큰으로 액세스 토큰을 받아옵니다. 만료 5분 전까지 재사용."""
    import requests
    if _kakao_tok["access"] and time.time() < _kakao_tok["exp"] - 300:
        return _kakao_tok["access"]
    if not (KAKAO_KEY and KAKAO_REFRESH):
        raise RuntimeError("KAKAO_REST_API_KEY / KAKAO_REFRESH_TOKEN 미설정")
    payload = {
        "grant_type": "refresh_token",
        "client_id": KAKAO_KEY,
        "refresh_token": KAKAO_REFRESH,
    }
    if KAKAO_SECRET:
        payload["client_secret"] = KAKAO_SECRET
    r = requests.post("https://kauth.kakao.com/oauth/token", timeout=15, data=payload)
    d = r.json()
    if "access_token" not in d:
        raise RuntimeError(f"토큰 갱신 실패: {d}")
    _kakao_tok["access"] = d["access_token"]
    _kakao_tok["exp"] = time.time() + int(d.get("expires_in", 3600))
    # 카카오가 리프레시 토큰을 갱신해 주는 경우가 있습니다(만료 1개월 이내).
    if d.get("refresh_token"):
        log.warning("카카오가 새 refresh_token 을 발급했습니다. "
                    "Render 의 KAKAO_REFRESH_TOKEN 을 갱신하세요.")
    return _kakao_tok["access"]


async def api_kakao_send(request):
    """
    GET /t/<TOKEN>/api/kakao?msg=<본문>

    `msg` 는 URL 인코딩된 평문. 카카오 제한에 맞춰 200자를 넘으면 자릅니다.
    """
    import json as _jsonlib
    import requests
    msg = (request.query_params.get("msg") or "").strip()
    if not msg:
        return JSONResponse({"ok": False, "error": "msg 파라미터가 필요합니다"},
                            status_code=400)
    truncated = False
    if len(msg) > 200:
        msg, truncated = msg[:197] + "...", True
    try:
        tok = _kakao_access_token()
        r = requests.post(
            "https://kapi.kakao.com/v2/api/talk/memo/default/send",
            headers={"Authorization": f"Bearer {tok}"}, timeout=15,
            data={"template_object": _jsonlib.dumps({
                "object_type": "text", "text": msg,
                "link": {"web_url": "https://markets.hankyung.com",
                         "mobile_web_url": "https://markets.hankyung.com"},
            }, ensure_ascii=False)})
        ok = r.status_code == 200 and r.json().get("result_code") == 0
        return _json({"ok": ok, "chars": len(msg), "truncated": truncated,
                      "status": r.status_code,
                      "detail": None if ok else r.text[:300]})
    except Exception as e:
        return _json({"ok": False, "error": f"{type(e).__name__}: {e}"})


async def kakao_setup(request):
    """최초 설정 안내 — 동의 URL 을 보여줍니다."""
    if not KAKAO_KEY:
        return PlainTextResponse(
            "KAKAO_REST_API_KEY 가 설정되지 않았습니다.\n"
            "Render → Environment 에 추가한 뒤 다시 여세요.\n", status_code=400)
    from urllib.parse import urlencode
    url = "https://kauth.kakao.com/oauth/authorize?" + urlencode({
        "client_id": KAKAO_KEY, "redirect_uri": KAKAO_REDIRECT,
        "response_type": "code", "scope": "talk_message"})
    return PlainTextResponse(
        "① 아래 주소를 브라우저에서 열어 동의하세요:\n\n" + url +
        "\n\n② 동의하면 이 서버의 /kakao/callback 으로 돌아오고,\n"
        "   화면에 refresh_token 이 표시됩니다.\n"
        "③ 그 값을 Render 환경변수 KAKAO_REFRESH_TOKEN 에 넣고 저장하세요.\n"
        "④ 재배포 후 /t/<토큰>/api/kakao?msg=테스트 로 확인하세요.\n\n"
        f"현재 클라이언트 시크릿: {'설정됨' if KAKAO_SECRET else '미설정'}\n"
        "  (카카오 콘솔에서 활성화 ON 이면 반드시 설정해야 합니다)\n")


async def kakao_callback(request):
    """카카오 동의 후 code → refresh_token 교환. 값은 화면에만 표시합니다."""
    import requests
    code = request.query_params.get("code")
    if not code:
        return PlainTextResponse("code 파라미터가 없습니다.", status_code=400)
    if not KAKAO_KEY:
        return PlainTextResponse("KAKAO_REST_API_KEY 미설정", status_code=400)
    payload = {"grant_type": "authorization_code", "client_id": KAKAO_KEY,
               "redirect_uri": KAKAO_REDIRECT, "code": code}
    if KAKAO_SECRET:
        payload["client_secret"] = KAKAO_SECRET
    r = requests.post("https://kauth.kakao.com/oauth/token", timeout=15, data=payload)
    d = r.json()
    if "refresh_token" not in d:
        return PlainTextResponse(f"교환 실패:\n{r.text[:500]}", status_code=400)
    return PlainTextResponse(
        "발급되었습니다. 아래 값을 Render 환경변수 KAKAO_REFRESH_TOKEN 에 넣으세요.\n"
        "이 화면을 닫으면 다시 볼 수 없습니다. 아무에게도 공유하지 마세요.\n\n"
        f"KAKAO_REFRESH_TOKEN =\n{d['refresh_token']}\n\n"
        f"(유효기간 약 {int(d.get('refresh_token_expires_in', 0))//86400}일. "
        "사용할 때마다 카카오가 자동 연장합니다.)\n")


async def api_search(request):
    q = request.query_params.get("q", "").strip()
    if not q:
        return JSONResponse({"error": "q 파라미터가 필요합니다"}, status_code=400)
    return _json(search_ticker(q, limit=_int_param(request, "limit", 20)))


API_ROUTES = [
    Route("/api/health", api_health),
    Route("/api/quote/{code}", api_quote),
    Route("/api/ohlcv/{code}", api_ohlcv),
    Route("/api/index/{index}", api_index),
    Route("/api/etf/{code}", api_etf),
    Route("/api/investor/{code}", api_investor),
    Route("/api/short/{code}", api_short),
    Route("/api/indicators/{code}", api_indicators),
    Route("/api/idx-indicators/{index}", api_idx_indicators),
    Route("/api/chartdata/{code}", api_chartdata),
    Route("/api/idx-chartdata/{index}", api_idx_chartdata),
    Route("/api/search", api_search),
    Route("/api/kakao", api_kakao_send),
    Route("/kakao/setup", kakao_setup),
]


async def root(_request):
    return PlainTextResponse(
        "pykrx MCP server (읽기 전용 · KRX via pykrx)\n"
        "MCP endpoint: /mcp  (Authorization: Bearer <MCP_TOKEN>)\n"
        "          또는 /t/<MCP_TOKEN>/mcp  (헤더 불필요)\n"
    )


async def healthz(_request):
    return JSONResponse({"ok": True, "kst": today_kst().strftime("%Y-%m-%d %H:%M:%S")})


async def robots(_request):
    """
    모든 경로 허용. 이 파일이 401·404 면 web_fetch 가 도메인 전체를 거부합니다.
    데이터는 토큰 경로 뒤에 있으므로 크롤러에게 노출되지 않습니다.
    """
    return PlainTextResponse("User-agent: *\nAllow: /\n")


# ⚠️ MCP SDK는 DNS 리바인딩 보호가 기본 ON 이라, Railway 도메인으로 들어오는 요청을
#    Host 헤더 불일치로 거부합니다. 이게 "커넥터가 안 붙는" 가장 흔한 원인입니다.
#    ALLOWED_HOSTS 를 지정하면 그 목록만 허용하고, 비워두면 보호를 끕니다.
#    (끄더라도 위 Bearer 토큰 검사는 그대로 적용됩니다.)
_hosts = [h.strip() for h in os.environ.get("ALLOWED_HOSTS", "").split(",") if h.strip()]
_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=bool(_hosts),
    allowed_hosts=_hosts,
    allowed_origins=_hosts,
)

_mcp_app = mcp.streamable_http_app(
    streamable_http_path="/mcp",
    # 호스팅 환경에서 인스턴스가 재시작돼도 세션이 깨지지 않도록 무상태로.
    stateless_http=True,
    transport_security=_security,
)

# ⚠️ MCP 앱을 바깥 Starlette 에 Mount 하지 마세요.
#    하위 앱의 lifespan 이 실행되지 않아 모든 MCP 요청이
#    "Task group is not initialized" 500 으로 떨어집니다.
#    대신 MCP 앱을 그대로 쓰고 라우트와 미들웨어만 얹습니다.
app = _mcp_app
# MCP 앱보다 앞에 꽂아야 /api/* 가 MCP 라우트에 먹히지 않습니다.
for _i, _r in enumerate([Route("/", root), Route("/healthz", healthz),
                         Route("/robots.txt", robots),
                         Route("/kakao/callback", kakao_callback), *API_ROUTES]):
    app.router.routes.insert(_i, _r)
app.add_middleware(TokenAuth)


if __name__ == "__main__":
    import uvicorn

    if not TOKEN:
        log.warning("MCP_TOKEN 이 비어 있습니다 — 서버가 인증 없이 공개됩니다!")
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
