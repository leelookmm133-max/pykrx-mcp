# pykrx MCP — 배포 가이드

한국거래소(KRX) 시세를 Claude 커스텀 커넥터로 노출하는 **읽기 전용** 원격 MCP 서버.

## 왜 필요한가

| 지금 문제 | 이 서버가 해결 |
|---|---|
| `0219E0` 등 영문 섞인 코드를 KIS가 못 읽음 | ✅ 네이티브 조회 |
| KIS 일봉 100건 제한 → 지수 차트가 약 5개월에서 끊김 | ✅ 12개월 전체 OHLCV |
| 국내 수급(개인·외국인·기관) 데이터 없음 | ✅ 투자자별 순매매 |
| 공매도 데이터 없음 | ✅ 거래량·잔고 |
| 한경 web_fetch 단일 장애점 | ✅ 한경은 백업으로 강등 |

## 제공 도구 (8개)

| 도구 | 설명 |
|---|---|
| `health` | 서버·pykrx 상태 확인 |
| `get_quote(code)` | 최근 거래일 시세 + 52주 고저 + NAV |
| `get_ohlcv(code, months=12)` | 일봉 이력 (최대 24개월) |
| `get_index_ohlcv(index, months=12)` | KOSPI / KOSDAQ / KOSPI200 지수 일봉 |
| `get_investor_trading(code, days=20)` | 투자자별 순매매 |
| `get_short_selling(code, days=20)` | 공매도 거래량·잔고 |
| `get_etf_detail(code, days=20)` | ETF NAV·괴리율·추적오차 |
| `search_ticker(query)` | 종목명으로 코드 검색 (주식+ETF) |

모든 응답에 `as_of` 또는 `date` 가 들어갑니다. **브리핑에서 기준일 없이 숫자만 쓰지 마세요.**

---

## 배포 (Railway)

### 1. GitHub 저장소 만들기

이 폴더(`_mcp/pykrx-mcp`) 전체를 새 GitHub 저장소에 올리세요. **private 로 두세요** —
토큰은 코드에 없지만 굳이 공개할 이유가 없습니다.

```
server.py
requirements.txt
Procfile
railway.json
README.md
```

### 2. Railway 프로젝트 생성

1. [railway.app](https://railway.app) 로그인 → **New Project** → **Deploy from GitHub repo**
2. 방금 만든 저장소 선택
3. Nixpacks가 `requirements.txt` 를 보고 Python 환경을 자동 구성합니다

### 3. 환경 변수 설정

Railway 프로젝트 → **Variables** 탭:

| 변수 | 필수 | 값 |
|---|---|---|
| `MCP_TOKEN` | ✅ **필수** | 길고 무작위한 문자열. 아래 생성법 참고 |
| `ALLOWED_HOSTS` | 선택 | 비워두면 DNS 리바인딩 보호 off (Bearer 인증은 그대로 적용). 지정 시 `xxx.up.railway.app` |
| `KRX_ID` / `KRX_PW` | 선택 | KRX 계정. 없으면 익명 요청으로 동작 (아래 참고) |

토큰 생성 (PowerShell):

```powershell
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### 4. 공개 도메인 발급

Railway → **Settings** → **Networking** → **Generate Domain**.
`https://<이름>.up.railway.app` 형태의 URL이 나옵니다.

### 5. 동작 확인

```powershell
curl https://<이름>.up.railway.app/healthz
# → {"ok":true,"kst":"..."}
```

### 6. Claude 커스텀 커넥터 등록

1. Claude → **Customize > Connectors** → **+ Add** → **Add custom connector**
2. 이름: `pykrx 한국시장`
3. URL: `https://<이름>.up.railway.app/mcp`   ← **끝에 `/mcp` 를 반드시 붙이세요**
4. 로그인 방식: **No sign in**
5. **Request headers** 에 추가:
   - Key: `Authorization`
   - Value: `Bearer <MCP_TOKEN 값>`
6. **Add**

### 7. 검증

새 대화에서 `health` 를 호출해 `ok: true` 가 나오는지, 그다음
`get_quote("0219E0")` 로 실제 가격이 오는지 확인하세요.

---

---

## 팩터(횡단면 순위) 백테스트·스크리너 — `factors.py` (2026-11-07 추가)

학계·실전 퀀트가 쓰는 방식으로 종목 선별 기준을 검증합니다. 기존 `screener.py`(일별 캐시·절대 조건 신호)와 별도 캐시를 씁니다.

| 도구 | 설명 |
|---|---|
| `factor_prefetch(years=10)` | 월말 스냅샷(전종목 시세·수정주가 월간수익률·PER/PBR)을 years년+13개월 백그라운드 수집. 서버 시작 시 자동 실행 |
| `factor_cache_status()` | 수집 진행률, 월간 수익률 출처(KRX 수정주가 / 하이브리드 / 종가 폴백), 기준가 일치율, 메모리 |
| `factor_month_debug(ymd="", n=20)` | 월별 데이터 품질 표; ymd 를 주면 그 달 기준가가 직전 월말 종가와 다른 종목 샘플 |
| `backtest_factors(factors_spec, min_cap_eok, years, top_pct, top_n, hold_months, …)` | 매월 말 유니버스 전종목을 팩터 백분위 순위로 점수화 → 상위 포트폴리오(동일가중) → 다음 달 수익률을 유니버스·지수와 비교. 10분위·IC·t-통계량·연도별·팩터별 단독 성과표 |
| `screen_factors(factors_spec, min_cap_eok, top_n)` | 최근 거래일 기준 복합점수 상위 종목과 팩터 값 |
| `paper_test(show_all_cohorts=False)` | **표본 외 검증 보고.** 2026-11-07 사전 등록 규칙(저PBR 상위 30종목·12개월 보유·시총 1,000억↑)을 그대로 적용해 이번 달 편입 종목, 표본 외 월별·누적 성과, 판정 상태를 돌려줌. 규칙 파라미터 없음(`factors.REGISTRATION` 수정 금지) |

팩터: `value`(=`bm` 1/PBR + `ep` EPS/P + `dy` 배당수익률 순위 평균), `quality`(=`roe` EPS/BPS), `mom`(12-1개월 모멘텀),
`rev`(1개월 반전·낮을수록 좋음), `lowvol`(12개월 저변동성), `high52`(52주고가 근접), `size`(소형주), `lowturn`(저회전율).
기본 복합: `value,quality,lowvol,mom`.

평가 기준(결과 `criteria_academic` 에 통과/미달): ① 상위 포트 월평균 초과수익(유니버스 대비) +0.3%p↑ ② t≥2
③ 월 적중률 55%↑ ④ 전·후반 모두 플러스 ⑤ 10분위 단조성(ρ≥0.7, D10>D1). 참고: IC 평균 ≥ 0.02.

데이터: 월말마다 KRX 3회 호출(전종목시세 → 종목명·시장·시총·주식수·소속부 / 전종목등락률(수정주가) → 월간 수익률·거래대금 /
PER·PBR·EPS·BPS·DIV) + KOSPI 지수 일별(거래일 달력, 연 1회). 10년 ≈ 133개 월말 ≈ 410회 ≈ 8~15분, 메모리 약 50MB. 일별 prefetch 가 끝난 뒤 시작합니다.

환경 변수: `FACTOR_YEARS`(기본 10), `FACTOR_PREFETCH=0` 이면 자동 수집 끔.

### 1단계 추가 신호 — `signals.py` (2026-11-08 추가)

`factors` 레지스트리에 팩터로 등록되므로 `backtest_factors`/`screen_factors` 의 `factors_spec` 에 이름을 그대로 씁니다(예: `"earnings,flow,value"`, `hold_months=3`).

| 신호 | 정의 | 출처 |
|---|---|---|
| `epm` | (당분기 3개월 영업이익 − 전년 동분기) / 시총 — 이익 모멘텀 | DART 분기 재무(공시 접수일 이후 반영) |
| `sales_g`, `opm_chg` | 매출 3개월 YoY, 영업이익률 YoY 변화(%p) | DART |
| `agr`(낮을수록), `roe_ttm`, `op_yield` | 자산증가율, 4분기 순이익/자본, 4분기 영업이익/시총 | DART |
| `sec_margin` | 업종 내 opm_chg 중앙값(산업 마진 추세) | DART + KRX 업종분류 |
| `sec_mom`, `rs3` | 업종 12-1 모멘텀, 종목−업종 3개월 상대강도 | KRX 업종별 분류현황(날짜별) |
| `flow20`, `flow60`, `frgn_chg` | (외국인+기관) 20거래일 순매수/시총, 3개월 합, 외국인 지분율 3개월 변화 | KRX 투자자별 순매수·외국인 보유 |
| `short_bal`, `short_chg`(둘 다 낮을수록) | 공매도 잔고 비중, 3개월 변화 | KRX 공매도 잔고(2016-06~) |
| 그룹 | `earnings`=[epm,sales_g,opm_chg] `quality2`=[roe_ttm,agr] `sector`=[sec_mom,rs3,sec_margin] `flow`=[flow20,flow60,frgn_chg] `shortint`=[short_bal,short_chg] | |

도구: `signals_prefetch(years)`, `signals_cache_status()`. 수집은 factors 월말 수집이 끝난 뒤 자동 시작(월말당 KRX 9회 ≈ 10년 40분, DART 분기당 약 30회 ≈ 15분).

환경 변수: `DART_API_KEY`(OpenDART 인증키 — 없으면 DART 신호는 NaN, 나머지는 정상), `SIGNALS_PREFETCH=0` 자동 수집 끔, `SIGNALS_YEARS`(기본 10).

### v1.4 (2026-11-11) — 계산 규칙 보강 · 캐시 영속화

- **겹침 보정 t**: `hold_months>1` 이면 월간 수익률이 겹쳐 자기상관이 생기므로 `excess_t_stat` 은 Newey-West(시차 k−1) 보정값, `excess_t_stat_plain` 이 단순 t.
- **거래비용·회전율**: `cost_pct`(왕복, 기본 0.5%) × 월 교체율로 비용을 근사해 `portfolio.cost`(비용 후 초과수익·t·CAGR)와 `avg_monthly_turnover_pct` 를 보고. 판정 기준 ①~⑤는 비용 전, ⑥(비용 후 > 0)은 참고.
- **상장폐지**: 다음 달 스냅샷에서 사라진 종목은 그 달 수익률을 `delist_return_pct`(기본 −50%)로 간주(유니버스·포트 동일). 민감도는 0/−50/−90 비교. `period.delisted_in_universe_*` 에 건수.
- **거래정지**: 월 거래량 0 인 종목은 편입 후보에서 제외(`exclude_suspended`, 기본 true).
- **paper_test**: 같은 규칙을 적용하고 `registration.amendments` 에 개정 내용을 기록. 등록 참고치(+0.61%p·t 1.77)는 구방법 값이며 새 기준값은 `in_sample_reproduction`.
- **캐시 영속화**(`cachestore.py`): (1) Railway 볼륨(`RAILWAY_VOLUME_MOUNT_PATH` 자동 또는 `CACHE_DIR`) 또는 (2) GitHub Release 자산 — 저장소(`GITHUB_CACHE_REPO`, 기본 이 저장소)의 `cache` 태그 릴리스에 pickle 첨부. 업로드에는 `GITHUB_TOKEN`(fine-grained PAT, Contents: Read and write) 필요, 공개 저장소면 다운로드(복구)는 토큰 없이. 둘 다 없으면 기존처럼 매번 수집. 상태는 `cache_persist_status`.
- **DART 병렬 수집**(v1.5): `fnlttMultiAcnt` 를 `DART_WORKERS`(기본 4)개 동시 호출 — 직렬 4시간 → 약 1시간. 수집 시작 연도는 첫 월말의 연도.

## 알아둘 것

### KRX 계정 (`KRX_ID` / `KRX_PW`)

pykrx 1.2.9 부터 KRX 로그인 세션을 지원합니다. 설정하지 않으면 시작 시
`KRX 로그인 실패: KRX_ID 또는 KRX_PW 환경 변수가 설정되지 않았습니다.` 가
로그에 찍히지만 **서버는 정상 동작합니다** — 익명 요청으로 폴백합니다.

다만 KRX가 비로그인 접근을 조이는 추세라, 안정적으로 쓰려면
[krx.co.kr](https://www.krx.co.kr) 무료 계정을 만들어 넣어두는 쪽을 권합니다.
익명 요청이 갑자기 빈 응답을 주기 시작하면 이게 원인입니다.

### 콜드 스타트

Railway는 유휴 시 인스턴스를 재우지 않으므로 브리핑이 타임아웃될 걱정은 적습니다.
다만 무료 크레딧(월 $5)을 넘으면 중단되니 사용량을 확인하세요.
Render 무료 티어로 옮기면 15분 유휴 후 잠들어 **첫 요청에 50초 이상** 걸리고,
하루 5회·몇 시간 간격으로 도는 브리핑은 항상 콜드 상태가 됩니다.

### 절대 하지 말 것

- 이 서버에 **주문·이체 기능을 넣지 마세요.** 읽기 전용으로 유지합니다.
- `MCP_TOKEN` 없이 배포하지 마세요. 공개 URL이라 누구나 호출할 수 있습니다.
- 토큰을 저장소에 커밋하지 마세요. Railway Variables 에만 두세요.

---

## 로컬 테스트

```powershell
pip install -r requirements.txt
$env:MCP_TOKEN="testtoken"
python server.py
```

다른 터미널에서:

```powershell
curl http://127.0.0.1:8000/healthz
```

### 검증 이력 (2026-10-27)

샌드박스에서 실제로 돌려 확인한 것:

- pykrx 1.2.9 에서 사용한 17개 함수 **전부 존재** 확인
- 8개 도구 모두 등록됨
- `initialize` / `tools/list` / `tools/call` **200 OK**
- 토큰 없는 요청 → **401**
- 없는 코드 조회 → 크래시 없이 `found: false` 구조화 응답

> **배포 중 가장 흔한 함정 (이미 코드에 반영됨)**
> MCP 앱을 바깥 Starlette 에 `Mount` 하면 하위 앱의 lifespan 이 실행되지 않아
> 모든 MCP 요청이 `Task group is not initialized` 500 으로 떨어집니다.
> `server.py` 는 MCP 앱을 그대로 쓰고 라우트·미들웨어만 얹는 방식으로 피했습니다.
> 구조를 고칠 때 이 부분을 되돌리지 마세요.
