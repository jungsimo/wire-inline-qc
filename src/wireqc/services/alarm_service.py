from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from wireqc.services.base_service import BaseStreamService
from wireqc.io.processdata_parser import parse_raw_message
from wireqc.streaming.rolling import RollingMeanStd

def _parse_ts_iso(ts: str | None) -> datetime | None:
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
    """
    Six-Sigma Eingriffsgrenzen: mu ± 3*sigma im gleitenden Fenster.
    Ereignis: ALARM_START bei erstmaliger Verletzung, ALARM_END bei Rückkehr.
    """
    def __init__(self, cfg: dict, window_s: int = 120, sample_hz: int = 10, min_n: int = 50):
        super().__init__(
            cfg=cfg,
            group_id="wireqc-alarms",
            in_topics=[cfg["topics"]["raw"]],
            parse_fn=parse_raw_message,
        )
        self.out_topic = cfg["topics"]["alarms"]
        self.maxlen = window_s * sample_hz
        self.min_n = min_n
        self.state: dict[str, AlarmState] = {}

    def _ensure_state(self, mid: str) -> AlarmState:
        st = self.state.get(mid)
        if st is None:
            st = AlarmState(
                hard=MetricAlarmState(stats=RollingMeanStd(self.maxlen)),
                temp=MetricAlarmState(stats=RollingMeanStd(self.maxlen)),
            )
            self.state[mid] = st
        return st

    def _check_metric(self, ts: datetime, machine_id: int, mid: str, metric: str, value: float | None, ms: MetricAlarmState):
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

    def handle(self, rec: dict) -> Iterable[tuple[str, dict, str | None]]:
        mid = str(rec.get("machine_id") or "unknown")
        machine_id = rec.get("machine_id")
        ts = _parse_ts_iso(rec.get("ts"))
        if ts is None or machine_id is None:
            return []

        st = self._ensure_state(mid)

        out_events = []
        out_events += self._check_metric(ts, machine_id, mid, "hard_temp", rec.get("hard_temp"), st.hard)
        out_events += self._check_metric(ts, machine_id, mid, "temp_temp", rec.get("temp_temp"), st.temp)

        return [(self.out_topic, e, mid) for e in out_events]

def main():
    from wireqc.common.config import load_config
    cfg = load_config()
    AlarmService(cfg).run()

if __name__ == "__main__":
    main()