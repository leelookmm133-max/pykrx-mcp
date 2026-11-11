"""
cachestore.py — 서버 재시작 후에도 캐시를 되살리기 위한 저장소(pickle). 두 가지 백엔드를 지원합니다.

1) 로컬 디스크: Railway 볼륨을 서비스에 붙이면 RAILWAY_VOLUME_MOUNT_PATH 가 자동 설정되며(또는 CACHE_DIR 지정),
   <dir>/pykrx_cache/<name>.pkl 로 저장·복구합니다. 무료 플랜은 볼륨이 안 되므로 보통 2) 를 씁니다.
2) GitHub Release 자산: 저장소(GITHUB_CACHE_REPO, 기본 leelookmm133-max/pykrx-mcp)의 태그 'cache' 릴리스에
   <name>.pkl 을 첨부합니다. 복구(다운로드)는 공개 저장소면 토큰 없이 되고, 저장(업로드)에는 GITHUB_TOKEN
   (fine-grained PAT, 해당 저장소 Contents: Read and write 권한) 이 필요합니다. 자산 하나 최대 2GB.

둘 다 없으면 아무 일도 하지 않습니다 — 기존처럼 매번 KRX/DART 에서 받습니다.
복구 순서: 로컬 파일 → GitHub 자산. 저장: 로컬(가능하면) + GitHub(토큰 있으면).
"""
from __future__ import annotations

import io
import logging
import os
import pickle
import threading
import time
from datetime import datetime
from typing import Any

import requests

log = logging.getLogger("cachestore")
_lock = threading.Lock()
_meta: dict[str, dict[str, Any]] = {}

GITHUB_REPO = os.environ.get("GITHUB_CACHE_REPO", "leelookmm133-max/pykrx-mcp").strip()
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_TAG = os.environ.get("GITHUB_CACHE_TAG", "cache").strip()
GITHUB_API = "https://api.github.com"
_gh_state: dict[str, Any] = {"last_error": None, "release_id": None}


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


def path_for(name: str) -> str | None:
    d = cache_dir()
    return os.path.join(d, f"{name}.pkl") if d else None


def _local_save(name: str, data: bytes) -> bool:
    p = path_for(name)
    if not p:
        return False
    tmp = p + ".tmp"
    try:
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, p)
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("로컬 캐시 저장 실패 %s: %s", name, e)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:  # noqa: BLE001
            pass
        return False


def _local_load(name: str) -> bytes | None:
    p = path_for(name)
    if not p or not os.path.exists(p):
        return None
    try:
        with open(p, "rb") as f:
            return f.read()
    except Exception as e:  # noqa: BLE001
        log.warning("로컬 캐시 읽기 실패 %s: %s", name, e)
        return None


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
        return None
    try:
        r = requests.get(f"{GITHUB_API}/repos/{GITHUB_REPO}/releases/tags/{GITHUB_TAG}", headers=_gh_headers(bool(GITHUB_TOKEN)), timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code != 404:
            _gh_state["last_error"] = f"release GET {r.status_code}: {r.text[:200]}"
            return None
        if not create or not GITHUB_TOKEN:
            return None
        body = {"tag_name": GITHUB_TAG, "name": "pykrx-mcp cache snapshots", "prerelease": True,
                "body": "서버 캐시 스냅샷(pickle). 코드 릴리스가 아니라 재시작 복구용 데이터입니다. 자동 갱신."}
        r = requests.post(f"{GITHUB_API}/repos/{GITHUB_REPO}/releases", headers=_gh_headers(True), json=body, timeout=30)
        if r.status_code in (200, 201):
            return r.json()
        _gh_state["last_error"] = f"release POST {r.status_code}: {r.text[:200]}"
    except Exception as e:  # noqa: BLE001
        _gh_state["last_error"] = f"release: {type(e).__name__}: {e}"
    return None


def _gh_upload(name: str, data: bytes) -> bool:
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False
    rel = _gh_release(create=True)
    if not rel:
        return False
    fname = f"{name}.pkl"
    try:
        for a in rel.get("assets", []):
            if a.get("name") == fname:
                requests.delete(f"{GITHUB_API}/repos/{GITHUB_REPO}/releases/assets/{a['id']}", headers=_gh_headers(True), timeout=30)
        up = str(rel.get("upload_url", "")).split("{")[0]
        if not up:
            _gh_state["last_error"] = "upload_url 없음"
            return False
        h = _gh_headers(True)
        h["Content-Type"] = "application/octet-stream"
        r = requests.post(f"{up}?name={fname}", headers=h, data=data, timeout=600)
        if r.status_code in (200, 201):
            _gh_state["last_error"] = None
            return True
        _gh_state["last_error"] = f"asset POST {r.status_code}: {r.text[:200]}"
    except Exception as e:  # noqa: BLE001
        _gh_state["last_error"] = f"upload: {type(e).__name__}: {e}"
    return False


def _gh_download(name: str) -> bytes | None:
    rel = _gh_release(create=False)
    if not rel:
        return None
    fname = f"{name}.pkl"
    asset = next((a for a in rel.get("assets", []) if a.get("name") == fname), None)
    if not asset:
        return None
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
        _gh_state["last_error"] = f"asset GET {r.status_code}"
    except Exception as e:  # noqa: BLE001
        _gh_state["last_error"] = f"download: {type(e).__name__}: {e}"
    return None


# ────────────────────────────────────────────────────────────
# 공개 API
# ────────────────────────────────────────────────────────────
def enabled() -> bool:
    return bool(cache_dir() or GITHUB_REPO)


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
    _meta[name] = {"saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "size_mb": round(len(data) / 1e6, 1),
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
    _meta[name] = {"loaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "size_mb": round(len(data) / 1e6, 1),
                   "elapsed_sec": round(time.time() - t0, 1), "source": src}
    log.info("캐시 복구 %s from %s (%.1f MB, %.1fs)", name, src, len(data) / 1e6, time.time() - t0)
    return obj


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
