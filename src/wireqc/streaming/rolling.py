from __future__ import annotations

from collections import deque
import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class RollingPearson:
    maxlen: int
    q: deque = None
    n: int = 0
    sx: float = 0.0
    sy: float = 0.0
    sxx: float = 0.0
    syy: float = 0.0
    sxy: float = 0.0

    def __post_init__(self):
        self.q = deque(maxlen=self.maxlen)

    def add(self, x: Optional[float], y: Optional[float]) -> None:
        if x is None or y is None:
            return

        if len(self.q) == self.q.maxlen:
            ox, oy = self.q.popleft()
            self._remove_values(ox, oy)

        self.q.append((x, y))
        self._add_values(x, y)

    def _add_values(self, x: float, y: float) -> None:
        self.n += 1
        self.sx += x
        self.sy += y
        self.sxx += x * x
        self.syy += y * y
        self.sxy += x * y

    def _remove_values(self, x: float, y: float) -> None:
        self.n -= 1
        self.sx -= x
        self.sy -= y
        self.sxx -= x * x
        self.syy -= y * y
        self.sxy -= x * y

    def value(self) -> Optional[float]:
        if self.n < 3:
            return None

        num = self.n * self.sxy - self.sx * self.sy
        den_x = self.n * self.sxx - self.sx * self.sx
        den_y = self.n * self.syy - self.sy * self.sy

        if den_x <= 0 or den_y <= 0:
            return None

        return num / math.sqrt(den_x * den_y)


@dataclass
class RollingMeanStd:
    maxlen: int
    q: deque = None
    n: int = 0
    s: float = 0.0
    ss: float = 0.0

    def __post_init__(self):
        self.q = deque(maxlen=self.maxlen)

    def add(self, x: Optional[float]) -> None:
        if x is None:
            return

        if len(self.q) == self.q.maxlen:
            old = self.q.popleft()
            self.n -= 1
            self.s -= old
            self.ss -= old * old

        self.q.append(x)
        self.n += 1
        self.s += x
        self.ss += x * x

    def mean(self) -> Optional[float]:
        if self.n == 0:
            return None
        return self.s / self.n

    def std(self) -> Optional[float]:
        if self.n < 2:
            return None
        mu = self.mean()
        var = (self.ss - self.n * mu * mu) / (self.n - 1)
        if var < 0:
            var = 0.0
        return math.sqrt(var)