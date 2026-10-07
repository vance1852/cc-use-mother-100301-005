"""提供可替换的 UTC 时钟。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    """定义服务所需的最小时钟接口。"""

    def now(self) -> datetime:
        """返回带时区的当前时间。"""


class SystemClock:
    """使用系统 UTC 时间。"""

    def now(self) -> datetime:
        """返回当前 UTC 时间。"""

        return datetime.now(timezone.utc)


class FixedClock:
    """为测试与离线验收提供固定时间。"""

    def __init__(self, value: datetime) -> None:
        if value.tzinfo is None:
            raise ValueError("固定时间必须包含时区")
        self._value = value.astimezone(timezone.utc)

    def now(self) -> datetime:
        """返回固定的 UTC 时间。"""

        return self._value


class ManualClock:
    """允许测试与验收按秒手动推进的时钟。"""

    def __init__(self, value: datetime) -> None:
        if value.tzinfo is None:
            raise ValueError("初始时间必须包含时区")
        self._value = value.astimezone(timezone.utc)

    def now(self) -> datetime:
        """返回当前的 UTC 时间。"""

        return self._value

    def advance(self, seconds: float) -> None:
        """把时钟向前推进指定的秒数。"""

        self._value = self._value + timedelta(seconds=seconds)
