"""
cachestore.py — 서버 재시작 후에도 캐시를 되살리기 위한 디스크 저장소(pickle).

Railway 볼륨을 서비스에 붙이면 RAILWAY_VOLUME_MOUNT_PATH 가 자동으로 설정되며, 그 경로(또는 CACHE_DIR 환경변수)에
screener(일별 OHLCV)·factors(월말 스냅샷)·signals(신호·DART) 캐시를 저장하고 시작 시 불러옵니다.
저장소가 없으면(환경변수 없음, 경로 쓰기 불가) 아무 일도 하지 않습니다 — 기존처럼 매번 KRX 에서 받습니다.

파일: <dir>/pykrx_cache/<name>.pkl  (임시 파일에 쓴 뒤 rename 하므로 중간에 죽어도 깨진 파일이 남지 않음)
"""
from __future__ import annotations

import logging
import os
import pickle
import threading
import time
from datetime import datetime
from typing import Any

log = logging.getLogger("cachestore")
_lock = threading.Lock()
_meta: dict[str, dict[str, Any]] = {}


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


def save(name: str, obj: Any) -> bool:
    """obj 를 <name>.pkl 로 저장. 저장소가 없으면 False."""
    p = path_for(name)
    if not p:
        return False
    tmp = p + ".tmp"
    t0 = time.time()
    try:
        with _lock:
            with open(tmp, "wb") as f:
                pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, p)
        size = os.path.getsize(p)
        _meta[name] = {"saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "size_mb": round(size / 1e6, 1),
                       "elapsed_sec": round(time.time() - t0, 1), "path": p}
        log.info("캐시 저장 %s (%.1f MB, %.1fs)", name, size / 1e6, time.time() - t0)
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("캐시 저장 실패 %s: %s", name, e)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:  # noqa: BLE001
            pass
        return False


def load(name: str) -> Any | None:
    """<name>.pkl 을 읽어 돌려준다. 없거나 실패하면 None."""
    p = path_for(name)
    if not p or not os.path.exists(p):
        return None
    t0 = time.time()
    try:
        with open(p, "rb") as f:
            obj = pickle.load(f)
        _meta[name] = {"loaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "size_mb": round(os.path.getsize(p) / 1e6, 1),
                       "elapsed_sec": round(time.time() - t0, 1), "path": p,
                       "file_mtime": datetime.fromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S")}
        log.info("캐시 복구 %s (%.1f MB, %.1fs)", name, os.path.getsize(p) / 1e6, time.time() - t0)
        return obj
    except Exception as e:  # noqa: BLE001
        log.warning("캐시 복구 실패 %s: %s (파일을 무시하고 새로 받습니다)", name, e)
        return None


def status(name: str | None = None) -> dict[str, Any]:
    d = cache_dir()
    out: dict[str, Any] = {"enabled": bool(d), "dir": d}
    if name:
        out["file"] = dict(_meta.get(name, {}))
        p = path_for(name)
        out["file_exists"] = bool(p and os.path.exists(p))
    else:
        out["files"] = {k: dict(v) for k, v in _meta.items()}
    return out
