"""可注入时间源。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


@dataclass
class FrozenClock:
    current: datetime

    def now(self) -> datetime:
        if self.current.tzinfo is None:
            raise ValueError("冻结时钟必须带时区")
        return self.current

    def advance(self, **kwargs: float) -> None:
        self.current += timedelta(**kwargs)


def isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("时间必须带时区")
    # 固定微秒宽度，保证存储后的字符串字典序与时间序一致，
    # 租约与可用时间的比较不会出现 "…:00Z" 与 "…:00.5Z" 混排误判。
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
