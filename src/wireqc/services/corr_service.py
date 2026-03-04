# src/wireqc/services/corr_service.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from wireqc.services.base_service import BaseStreamService
from wireqc.io.processdata_parser import parse_raw_message
from wireqc.streaming.rolling import RollingPearson

def _parse_ts_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    # robuste ISO-Variante (Z / ohne Z)
    try:
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        dt = datetime.fromisoformat(ts)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

@dataclass
class CorrState:
    rp: RollingPearson
    last_emit: datetime | None = None

class CorrelationService(BaseStreamService):
    def __init__(self, cfg: dict, window_s: int = 120, emit_every_s: int = 5, sample_hz: int = 10):
        super().__init__(
            cfg=cfg,
            group_id="wireqc-corr",
            in_topics=[cfg["topics"]["raw"]],
            parse_fn=parse_raw_message,
        )
        self.out_topic = cfg["topics"]["corr"]
        self.emit_every_s = emit_every_s
        self.maxlen = window_s * sample_hz
        self.state: dict[str, CorrState] = {}

    def handle(self, rec: dict) -> Iterable[tuple[str, dict, str | None]]:
        mid = str(rec.get("machine_id") or "unknown")
        st = self.state.get(mid)
        if st is None:
            st = CorrState(rp=RollingPearson(maxlen=self.maxlen))
            self.state[mid] = st

        ts = _parse_ts_iso(rec.get("ts"))
        # Fallback: wenn keine ts kommt, skip (oder msg timestamp nutzen – optional)
        if ts is None:
            return []

        st.rp.add(rec.get("speed"), rec.get("hard_temp"))

        # emit cadence
        if st.last_emit is None or (ts - st.last_emit).total_seconds() >= self.emit_every_s:
            corr = st.rp.value()
            st.last_emit = ts
            if corr is None:
                return []
            event = {
                "ts": ts.isoformat(),
                "machine_id": rec.get("machine_id"),
                "window_samples": st.rp.n,
                "pearson_corr_speed_hardtemp": corr,
            }
            return [(self.out_topic, event, mid)]

        return []

def main():
    from wireqc.common.config import load_config
    cfg = load_config()
    CorrelationService(cfg).run()

if __name__ == "__main__":
    main()