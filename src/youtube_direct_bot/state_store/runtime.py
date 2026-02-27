from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Set

from .core import load_index, load_known_chats


@dataclass
class ArchiveItem:
    video_id: str
    url: str
    title: str
    channel: str
    started_utc: str
    started_local: str
    date_key: str
    service_key: str
    service_label: str
    filename: str
    public_url: str
    status: str
    created_at_local: str


@dataclass
class ActiveLive:
    video_id: str
    url: str
    title: str
    started_local: datetime
    service_key: str
    service_label: str
    date_key: str
    status_message_chat_id: int
    status_message_id: int
    started_by_chat_id: int
    started_at: float
    last_progress_edit: float = 0.0


class RuntimeState:
    def __init__(self):
        self.known_chats: Set[int] = load_known_chats()
        self.archive_index: Dict[str, dict] = load_index()
        self.active_lives: Dict[str, ActiveLive] = {}
        self.stop_live_requests: Set[str] = set()
        self.stop_live_lock = threading.Lock()
        self.state_lock = asyncio.Lock()
        self.replay_tasks: Set[str] = set()
        self.replay_tasks_lock = asyncio.Lock()


STATE = RuntimeState()


def request_live_stop(video_id: str) -> bool:
    vid = str(video_id or "").strip()
    if not vid:
        return False
    with STATE.stop_live_lock:
        STATE.stop_live_requests.add(vid)
    return True


def clear_live_stop_request(video_id: str) -> None:
    vid = str(video_id or "").strip()
    if not vid:
        return
    with STATE.stop_live_lock:
        STATE.stop_live_requests.discard(vid)


def is_live_stop_requested(video_id: str) -> bool:
    vid = str(video_id or "").strip()
    if not vid:
        return False
    with STATE.stop_live_lock:
        return vid in STATE.stop_live_requests
