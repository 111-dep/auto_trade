from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, Mapping


@dataclass(frozen=True)
class SignalSnapshot(Mapping[str, Any]):
    """Typed wrapper around strategy signal payload."""

    payload: Dict[str, Any]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SignalSnapshot":
        return cls(payload=dict(data))

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.payload)

    def __getitem__(self, key: str) -> Any:
        return self.payload[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.payload)

    def __len__(self) -> int:
        return len(self.payload)

    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)

    @property
    def signal_ts_ms(self) -> int:
        return int(self.payload.get("signal_ts_ms", 0) or 0)

    @property
    def close(self) -> float:
        return float(self.payload.get("close", 0.0) or 0.0)

    @property
    def long_level(self) -> int:
        return int(self.payload.get("long_level", 0) or 0)

    @property
    def short_level(self) -> int:
        return int(self.payload.get("short_level", 0) or 0)
