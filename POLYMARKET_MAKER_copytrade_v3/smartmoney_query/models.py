from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class Trade:
    side: Optional[str]
    size: float
    price: float
    timestamp: datetime
    raw: Dict[str, Any]
    market_id: Optional[str] = None

