
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Sequence, Tuple

class EventRepo(ABC):
    @abstractmethod
    async def create(self, name: str, user_id: int, channel_id: int, start_iso: str, end_iso: str, message_id: int) -> int: ...
    @abstractmethod
    async def get_message_ref(self, name: str, channel_id: int) -> Tuple[int, int] | None: ...

class SignupRepo(ABC):
    @abstractmethod
    async def add(self, event_name: str, user_id: int) -> None: ...
    @abstractmethod
    async def remove(self, event_name: str, user_id: int) -> None: ...
    @abstractmethod
    async def list_names(self, event_name: str) -> list[int]: ...

class UploadRepo(ABC):
    @abstractmethod
    async def add_upload(self, event_name: str, file_path: str, permalink: str, boss_id: int, boss_name: str, success: int, time_iso: str) -> int | None: ...
    @abstractmethod
    async def list_for_event(self, event_name: str) -> list[dict]: ...