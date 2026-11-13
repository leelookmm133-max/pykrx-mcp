"""
cachestore.py — 서버 재시작 후에도 캐시를 되살리기 위한 저장소(pickle). 두 가지 백엔드를 지원합니다.

1) 로컬 디스크: Railway 볼륨을 서비스에 붙이면 RAILWAY_VOLUME_MOUNT_PATH 가 자동 설정되며(또는 CACHE_DIR 지정),
   <dir>/pykrx_cache/<name>.pkl 로 저장·복구합니다. 무료 플랜은 볼륨이 안 되므로 보통 2) 를 씁니다.
2) GitHub Release 자산: 저장소(GITHUB_CACHE_REPO, 기본 leelookmm133-max/pykrx-mcp)의 태그 'cache' 릴리스에
   <name>.pkl 을 첨부합니다. 복구(다운로드)는 공개 저장소면 토큰 없이 되고, 저장(업로드)에는 GITHUB_TOKEN
   (fine-grained PAT, 해당 저장소 Contents: Read and write 권한) 이 필요합니다. 자산 하나 최대 2GB.

둘 다 없으면 아무 일도 하지 않습니다 — 기존처럼 매번 KRX/DART 에서 받습니다.
복구 순서: 로컬 파일 → GitHub 자산. 저장: 로컬(가능하면) + GitHub(토큰 있으면).

v1.7
- 안전한 교체: 새 파일을 임시 이름(<파일>.new)으로 먼저 올린 뒤 옛 자산을 지우고 이름을 바꿉니다. 중간에 실패해도 옛 자산이나
  .new 중 하나는 남고, 복구할 때 둘 중 최신 것을 씁니다(이전에는 '삭제 → 업로드' 순서라 업로드가 실패하면 캐시가 사라졌음).
- save_text / load_text: 사람이 읽을 수 있는 텍스트(JSON) 자산 — 표본 외 편입 종목 장부(paper_ledger.json) 등.
- 저장·복구 시각을 한국 시간(KST)으로 기록.
"""
from __future__ import annotations

import logging
import os
import pickle
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

log = logging.getLogger("cachestore")
_lock = threading.Lock()
_meta: dict[str, dict[str, Any]] = {}

KST = timezone(timedelta(hours=9))
GITHUB_REPO = os.environ.get("GITHUB_CACHE_REPO", "leelookmm133-max/pykrx-mcp").strip()
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_TAG = os.environ.get("GITHUB_CACHE_TAG", "cache").strip()
GITHUB_API = "https://api.github.com"
_gh_state: dict[str, Any] = {"last_error": None, "release_id": None}


def _now() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


# ────────────────────────────────────────────────────────────
# 로컬 디스크
# ────────────────────────────────────────────────────────────
def cache_dir() -> str | None:
    d = os.environ.get("CACHE_DIR") or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
    if not d:
        return None
    d = os.path.join(d, "pykrx_cache")
    try:
        os.makedirs(d, exist_ok=True)
        probe = os.path.join(d, ".probe")
        with open(probe, "w") as f:
            f.write("ok")
        os.remove(probe)
        return d
    except Exception as e:  # noqa: BLE001
        log.warning("캐시 디렉터리 사용 불가 %s: %s", d, e)
        return None


def _file_path(fname: str) -> str | None:
    d = cache_dir()
    return os.path.join(d, fname) if d else None


def path_for(name: str) -> str | None:
    return _file_path(f"{name}.pkl")


def _local_save_file(fname: str, data: bytes) -> bool:
    p = _file_path(fname)
    if not p:
        return False
    tmp = p + ".tmp"
    try:
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, p)
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("로컬 캐시 저장 실패 %s: %s", fname, e)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:  # noqa: BLE001
            pass
        return False


def _local_load_file(fname: str) -> bytes | None:
    p = _file_path(fname)
    if not p or not os.path.exists(p):
        return None
    try:
        with open(p, "rb") as f:
            return f.read()
    except Exception as e:  # noqa: BLE001
        log.warning("로컬 캐시 읽기 실패 %s: %s", fname, e)
        return None


def _local_save(name: str, data: bytes) -> bool:
    return _local_save_file(f"{name}.pkl", data)


def _local_load(name: str) -> bytes | None:
    return _local_load_file(f"{name}.pkl")


# ────────────────────────────────────────────────────────────
# GitHub Release 자산
# ────────────────────────────────────────────────────────────
def _gh_headers(auth: bool) -> dict[str, str]:
    h = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "pykrx-mcp-cache"}
    if auth and GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


def _gh_release(create: bool = False) -> dict[str, Any] | None:
    """태그 GITHUB_TAG 의 릴리스(dict). 없고 create=True·토큰 있으면 만든다."""
    if not GITHUB_REPO:
        _gh_state["last_status"] = "absent"
        return None
    _gh_state["last_status"] = "error"       # 아래에서 성공하면 ok / 릴리스가 없으면 absent 로 바뀐다(exists() 가 '없음'과 '확인 불가'를 구분)
    try:
        r = requests.get(f"{GITHUB_API}/repos/{GITHUB_REPO}/releases/tags/{GITHUB_TAG}", headers=_gh_headers(bool(GITHUB_TOKEN)), timeout=30)
        if r.status_code == 200:
            _gh_state["last_status"] = "ok"
            return r.json()
        if r.status_code != 404:
            _gh_state["last_error"] = f"release GET {r.status_code}: {r.text[:200]}"
            return None
        _gh_state["last_status"] = "absent"
        if not create or not GITHUB_TOKEN:
            return None
        body = {"tag_name": GITHUB_TAG, "name": "pykrx-mcp cache snapshots", "prerelease": True,
                "body": "서버 캐시 스냅샷(pickle). 코드 릴리스가 아니라 재시작 복구용 데이터입니다. 자동 갱신."}
        r = requests.post(f"{GITHUB_API}/repos/{GITHUB_REPO}/releases", headers=_gh_headers(True), json=body, timeout=30)
        if r.status_code in (200, 201):
            _gh_state["last_status"] = "ok"
            return r.json()
        _gh_state["last_error"] = f"release POST {r.status_code}: {r.text[:200]}"
    except Exception as e:  # noqa: BLE001
        _gh_state["last_error"] = f"release: {type(e).__name__}: {e}"
    return None


def _asset_ok(a: dict[str, Any]) -> bool:
    """끝까지 올라간 자산만(중단된 업로드는 state 가 'uploaded' 가 아니거나 크기 0)."""
    return a.get("state", "uploaded") == "uploaded" and int(a.get("size", 1) or 0) > 0


def _gh_upload_file(fname: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
    """릴리스 자산 fname 을 새 내용으로 교체. 임시 이름으로 올린 뒤 → 옛 자산 삭제 → 이름 변경.
    업로드만 성공하면 True(뒤처리가 실패해도 .new 가 남아 복구할 때 그것을 쓴다)."""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False
    rel = _gh_release(create=True)
    if not rel:
        return False
    tmp = fname + ".new"
    base = f"{GITHUB_API}/repos/{GITHUB_REPO}/releases/assets"
    try:
        assets = {a.get("name"): a for a in rel.get("assets", [])}
        if tmp in assets:                      # 지난번에 중단돼 남은 임시 자산
            requests.delete(f"{base}/{assets[tmp]['id']}", headers=_gh_headers(True), timeout=30)
        up = str(rel.get("upload_url", "")).split("{")[0]
        if not up:
            _gh_state["last_error"] = "upload_url 없음"
            return False
        h = _gh_headers(True)
        h["Content-Type"] = content_type
        r = requests.post(f"{up}?name={tmp}", headers=h, data=data, timeout=600)
        if r.status_code not in (200, 201):
            _gh_state["last_error"] = f"asset POST {r.status_code}: {r.text[:200]}"
            return False
        new_id = (r.json() or {}).get("id")
    except Exception as e:  # noqa: BLE001
        _gh_state["last_error"] = f"upload: {type(e).__name__}: {e}"
        return False
    # 여기부터는 새 내용이 안전하게 올라간 상태 — 뒤처리 실패는 경고만
    try:
        if fname in assets:
            d = requests.delete(f"{base}/{assets[fname]['id']}", headers=_gh_headers(True), timeout=30)
            if d.status_code not in (204, 404):
                _gh_state["last_error"] = f"옛 자산 삭제 {d.status_code} (새 자산은 {tmp} 로 저장됨)"
                return True
        p = requests.patch(f"{base}/{new_id}", headers=_gh_headers(True), json={"name": fname}, timeout=30)
        if p.status_code not in (200, 201):
            _gh_state["last_error"] = f"자산 이름 변경 {p.status_code} (새 자산은 {tmp} 로 저장됨)"
            return True
        _gh_state["last_error"] = None
    except Exception as e:  # noqa: BLE001
        _gh_state["last_error"] = f"upload 뒤처리: {type(e).__name__}: {e} (새 자산은 {tmp} 로 저장됨)"
    return True


def _gh_download_file(fname: str) -> bytes | None:
    """자산 fname(또는 교체 도중 남은 fname.new) 중 최신 것을 받는다."""
    rel = _gh_release(create=False)
    if not rel:
        return None
    cands = [a for a in rel.get("assets", []) if a.get("name") in (fname, fname + ".new") and _asset_ok(a)]
    cands.sort(key=lambda a: str(a.get("updated_at") or a.get("created_at") or ""), reverse=True)
    for asset in cands:
        try:
            # 공개 저장소: browser_download_url 로 토큰 없이. 비공개: API url + octet-stream + 토큰
            url = asset.get("browser_download_url")
            h = _gh_headers(bool(GITHUB_TOKEN))
            if GITHUB_TOKEN:
                url = asset.get("url", url)
                h["Accept"] = "application/octet-stream"
            r = requests.get(url, headers=h, timeout=600, allow_redirects=True)
            if r.status_code == 200:
                return r.content
            _gh_state["last_error"] = f"asset GET {r.status_code} ({asset.get('name')})"
        except Exception as e:  # noqa: BLE001
            _gh_state["last_error"] = f"download: {type(e).__name__}: {e}"
    return None


def _gh_upload(name: str, data: bytes) -> bool:
    return _gh_upload_file(f"{name}.pkl", data)


def _gh_download(name: str) -> bytes | None:
    return _gh_download_file(f"{name}.pkl")


# ────────────────────────────────────────────────────────────
# 공개 API
# ────────────────────────────────────────────────────────────
def enabled() -> bool:
    return bool(cache_dir() or GITHUB_REPO)


def exists(name: str) -> bool | None:
    """캐시 name 이 저장소에 있는지: True / False(확실히 없음) / None(네트워크·API 오류로 확인 불가).
    '없어서 못 읽음'과 '있는데 못 읽음'을 구분해야 하는 자료(표본 외 편입 장부)가 덮어쓰기 사고를 피하는 데 쓴다."""
    p = path_for(name)
    if p and os.path.exists(p):
        return True
    if not GITHUB_REPO:
        return False
    rel = _gh_release(create=False)
    if rel is None:
        return False if _gh_state.get("last_status") == "absent" else None
    fname = f"{name}.pkl"
    return any(a.get("name") in (fname, fname + ".new") and _asset_ok(a) for a in rel.get("assets", []))


def save(name: str, obj: Any) -> bool:
    """obj 를 직렬화해 로컬(가능하면)과 GitHub(토큰 있으면)에 저장. 어느 하나라도 성공하면 True."""
    t0 = time.time()
    try:
        data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:  # noqa: BLE001
        log.warning("캐시 직렬화 실패 %s: %s", name, e)
        return False
    with _lock:
        ok_local = _local_save(name, data)
        ok_gh = _gh_upload(name, data) if GITHUB_TOKEN else False
    _meta[name] = {"saved_at": _now(), "size_mb": round(len(data) / 1e6, 1),
                   "elapsed_sec": round(time.time() - t0, 1), "local": ok_local, "github": ok_gh}
    if ok_local or ok_gh:
        log.info("캐시 저장 %s (%.1f MB, %.1fs, local=%s github=%s)", name, len(data) / 1e6, time.time() - t0, ok_local, ok_gh)
    return ok_local or ok_gh


def load(name: str) -> Any | None:
    """로컬 → GitHub 순으로 읽어 역직렬화. 없거나 실패하면 None."""
    t0 = time.time()
    src = None
    data = _local_load(name)
    if data is not None:
        src = "local"
    elif GITHUB_REPO:
        data = _gh_download(name)
        if data is not None:
            src = "github"
    if data is None:
        return None
    try:
        obj = pickle.loads(data)
    except Exception as e:  # noqa: BLE001
        log.warning("캐시 복구 실패 %s(%s): %s (무시하고 새로 받습니다)", name, src, e)
        return None
    _meta[name] = {"loaded_at": _now(), "size_mb": round(len(data) / 1e6, 1),
                   "elapsed_sec": round(time.time() - t0, 1), "source": src}
    log.info("캐시 복구 %s from %s (%.1f MB, %.1fs)", name, src, len(data) / 1e6, time.time() - t0)
    return obj


def save_text(filename: str, text: str) -> bool:
    """텍스트(JSON 등)를 파일 이름 그대로 저장 — 릴리스 페이지에서 사람이 내려받아 읽을 수 있다."""
    data = text.encode("utf-8")
    with _lock:
        ok_local = _local_save_file(filename, data)
        ok_gh = _gh_upload_file(filename, data, "application/json; charset=utf-8") if GITHUB_TOKEN else False
    _meta[filename] = {"saved_at": _now(), "size_kb": round(len(data) / 1e3, 1), "local": ok_local, "github": ok_gh}
    return ok_local or ok_gh


def load_text(filename: str) -> str | None:
    data = _local_load_file(filename)
    src = "local" if data is not None else None
    if data is None and GITHUB_REPO:
        data = _gh_download_file(filename)
        src = "github" if data is not None else None
    if data is None:
        return None
    _meta[filename] = {"loaded_at": _now(), "size_kb": round(len(data) / 1e3, 1), "source": src}
    try:
        return data.decode("utf-8")
    except Exception:  # noqa: BLE001
        return None


def status(name: str | None = None) -> dict[str, Any]:
    d = cache_dir()
    out: dict[str, Any] = {"enabled": bool(d) or bool(GITHUB_REPO), "local_dir": d,
                           "github": {"repo": GITHUB_REPO or None, "tag": GITHUB_TAG, "token_set": bool(GITHUB_TOKEN),
                                      "can_upload": bool(GITHUB_TOKEN and GITHUB_REPO), "last_error": _gh_state.get("last_error")}}
    if name:
        out["file"] = dict(_meta.get(name, {}))
        p = path_for(name)
        out["local_file_exists"] = bool(p and os.path.exists(p))
    else:
        out["files"] = {k: dict(v) for k, v in _meta.items()}
    return out
