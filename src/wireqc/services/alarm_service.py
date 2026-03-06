from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional, Tuple

from wireqc.services.base_service import BaseStreamService
from wireqc.io.processdata_parser import parse_raw_message
from wireqc.streaming.rolling import RollingMeanStd


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


@dataclass
class MetricAlarmState:
    stats: RollingMeanStd
    in_alarm: bool = False


@dataclass
class AlarmState:
    hard: MetricAlarmState
    temp: MetricAlarmState


class AlarmService(BaseStreamService):
    def __init__(
        self,
        cfg: dict,
        window_s: int = 120,
        sample_hz: int = 10,
        min_n: int = 50,
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
        self.state = {}

    def _ensure_state(self, mid: str) -> AlarmState:
        st = self.state.get(mid)
        if st is None:
            st = AlarmState(
                hard=MetricAlarmState(stats=RollingMeanStd(self.maxlen)),
                temp=MetricAlarmState(stats=RollingMeanStd(self.maxlen)),
            )
            self.state[mid] = st
        return st

    def _check_metric(self, ts: datetime, machine_id: int, metric: str, value: Optional[float], ms: MetricAlarmState):
        outs = []
        ms.stats.add(value)

        if ms.stats.n < self.min_n:
            return outs

        mu = ms.stats.mean()
        sd = ms.stats.std()
        if mu is None or sd is None:
            return outs

        lcl = mu - 3.0 * sd
        ucl = mu + 3.0 * sd
        violated = (value is not None) and (value < lcl or value > ucl)

        if violated and not ms.in_alarm:
            ms.in_alarm = True
            outs.append({
                "ts": ts.isoformat(),
                "machine_id": machine_id,
                "metric": metric,
                "value": value,
                "mean": mu,
                "std": sd,
                "lcl": lcl,
                "ucl": ucl,
                "type": "ALARM_START",
            })
        elif (not violated) and ms.in_alarm:
            ms.in_alarm = False
            outs.append({
                "ts": ts.isoformat(),
                "machine_id": machine_id,
                "metric": metric,
                "value": value,
                "mean": mu,
                "std": sd,
                "lcl": lcl,
                "ucl": ucl,
                "type": "ALARM_END",
            })

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
    ap.add_argument("--window-s", type=int, default=120)
    ap.add_argument("--sample-hz", type=int, default=10)
    ap.add_argument("--min-n", type=int, default=50)
    args = ap.parse_args()

    cfg = load_config()
    AlarmService(
        cfg,
        window_s=args.window_s,
        sample_hz=args.sample_hz,
        min_n=args.min_n,
        group_id=args.group,
        auto_offset_reset=args.offset,
    ).run()


if __name__ == "__main__":
    main()