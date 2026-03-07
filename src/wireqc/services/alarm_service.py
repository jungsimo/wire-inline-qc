from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from math import sqrt
from typing import Iterable, Optional, Tuple, Dict

from wireqc.services.base_service import BaseStreamService
from wireqc.io.processdata_parser import parse_raw_message


def _parse_ts_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        dt = datetime.fromisoformat(ts)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


class RollingMeanStdPast:
    """
    Gleitende Statistik über das bisherige Fenster.
    Der aktuelle Wert wird erst NACH der Prüfung in das Fenster aufgenommen.
    """
    def __init__(self, maxlen: int):
        self.maxlen = maxlen
        self.q = deque()
        self.n = 0
        self.s = 0.0
        self.ss = 0.0

    def stats(self) -> Tuple[Optional[float], Optional[float], int]:
        if self.n == 0:
            return None, None, self.n

        mean = self.s / self.n

        if self.n < 2:
            return mean, None, self.n

        var = (self.ss - self.n * mean * mean) / (self.n - 1)
        if var < 0:
            var = 0.0

        return mean, sqrt(var), self.n

    def add(self, x: Optional[float]) -> None:
        if x is None:
            return

        x = float(x)

        if len(self.q) == self.maxlen:
            old = self.q.popleft()
            self.n -= 1
            self.s -= old
            self.ss -= old * old

        self.q.append(x)
        self.n += 1
        self.s += x
        self.ss += x * x


@dataclass
class MetricAlarmState:
    stats: RollingMeanStdPast
    in_alarm: bool = False
    violation_run: int = 0


@dataclass
class AlarmState:
    hard: MetricAlarmState
    temp: MetricAlarmState


class AlarmService(BaseStreamService):
    """
    Six-Sigma-Schwellwerterkennung mit:
    - 5-Minuten-Fenster (standardmäßig)
    - Warm-up
    - Prüfung gegen das vergangene Fenster
    - Persistenzbedingung
    """
    def __init__(
        self,
        cfg: dict,
        window_s: int = 300,
        sample_hz: int = 10,
        min_n: int = 3000,
        persist_n: int = 15,
        group_id: str = "wireqc-alarms",
        auto_offset_reset: str = "latest",
    ):
        super().__init__(
            cfg=cfg,
            group_id=group_id,
            in_topics=[cfg["topics"]["raw"]],
            parse_fn=parse_raw_message,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
            commit_every=50,
        )

        self.out_topic = cfg["topics"]["alarms"]
        self.maxlen = window_s * sample_hz
        self.min_n = min_n
        self.persist_n = persist_n
        self.state = {}

    def _ensure_state(self, mid: str) -> AlarmState:
        st = self.state.get(mid)
        if st is None:
            st = AlarmState(
                hard=MetricAlarmState(stats=RollingMeanStdPast(self.maxlen)),
                temp=MetricAlarmState(stats=RollingMeanStdPast(self.maxlen)),
            )
            self.state[mid] = st
        return st

    def _check_metric(
        self,
        ts: datetime,
        machine_id: int,
        metric: str,
        value: Optional[float],
        ms: MetricAlarmState,
    ):
        outs = []

        mean, sd, n = ms.stats.stats()

        violated = False
        lcl = None
        ucl = None

        if n >= self.min_n and mean is not None and sd is not None and value is not None:
            lcl = mean - 3.0 * sd
            ucl = mean + 3.0 * sd
            violated = (value < lcl) or (value > ucl)

        if violated:
            ms.violation_run += 1
        else:
            ms.violation_run = 0

        if (not ms.in_alarm) and violated and ms.violation_run >= self.persist_n:
            ms.in_alarm = True
            event = {
                "ts": ts.isoformat(),
                "machine_id": machine_id,
                "metric": metric,
                "value": value,
                "mean": mean,
                "std": sd,
                "lcl": lcl,
                "ucl": ucl,
                "persist_n": self.persist_n,
                "violation_run": ms.violation_run,
                "type": "ALARM_START",
            }
            print(
                "[alarms] START metric={0} value={1:.3f} lcl={2:.3f} ucl={3:.3f}".format(
                    metric,
                    float(value) if value is not None else float("nan"),
                    float(lcl) if lcl is not None else float("nan"),
                    float(ucl) if ucl is not None else float("nan"),
                )
            )
            outs.append(event)

        elif ms.in_alarm and (not violated):
            ms.in_alarm = False
            event = {
                "ts": ts.isoformat(),
                "machine_id": machine_id,
                "metric": metric,
                "value": value,
                "mean": mean,
                "std": sd,
                "lcl": lcl,
                "ucl": ucl,
                "persist_n": self.persist_n,
                "violation_run": 0,
                "type": "ALARM_END",
            }
            print(
                "[alarms] END   metric={0} value={1:.3f}".format(
                    metric,
                    float(value) if value is not None else float("nan"),
                )
            )
            outs.append(event)

        # erst nach der Prüfung den Wert ins Fenster aufnehmen
        ms.stats.add(value)

        return outs

    def handle(self, rec: dict) -> Iterable[Tuple[str, dict, Optional[str]]]:
        mid = str(rec.get("machine_id") or "unknown")
        machine_id = rec.get("machine_id")
        ts = _parse_ts_iso(rec.get("ts"))

        if ts is None or machine_id is None:
            return []

        st = self._ensure_state(mid)

        out_events = []
        out_events += self._check_metric(ts, machine_id, "hard_temp", rec.get("hard_temp"), st.hard)
        out_events += self._check_metric(ts, machine_id, "temp_temp", rec.get("temp_temp"), st.temp)

        return [(self.out_topic, e, mid) for e in out_events]


def main():
    from wireqc.common.config import load_config

    ap = argparse.ArgumentParser()
    ap.add_argument("--offset", choices=["earliest", "latest"], default="latest")
    ap.add_argument("--group", default="wireqc-alarms")
    ap.add_argument("--window-s", type=int, default=300)
    ap.add_argument("--sample-hz", type=int, default=10)
    ap.add_argument("--min-n", type=int, default=3000)
    ap.add_argument("--persist-n", type=int, default=20)
    args = ap.parse_args()

    cfg = load_config()

    AlarmService(
        cfg,
        window_s=args.window_s,
        sample_hz=args.sample_hz,
        min_n=args.min_n,
        persist_n=args.persist_n,
        group_id=args.group,
        auto_offset_reset=args.offset,
    ).run()


if __name__ == "__main__":
    main()