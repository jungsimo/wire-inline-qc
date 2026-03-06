from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional, Tuple

from wireqc.services.base_service import BaseStreamService
from wireqc.io.processdata_parser import parse_raw_message
from wireqc.streaming.rolling import RollingPearson


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
class CorrState:
    rp: RollingPearson
    last_emit: Optional[datetime] = None


class CorrelationService(BaseStreamService):
    def __init__(
        self,
        cfg: dict,
        window_s: int = 120,
        emit_every_s: int = 5,
        sample_hz: int = 10,
        group_id: str = "wireqc-corr",
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
        self.out_topic = cfg["topics"]["corr"]
        self.emit_every_s = emit_every_s
        self.maxlen = window_s * sample_hz
        self.state = {}

    def handle(self, rec: dict) -> Iterable[Tuple[str, dict, Optional[str]]]:
        mid = str(rec.get("machine_id") or "unknown")
        st = self.state.get(mid)
        if st is None:
            st = CorrState(rp=RollingPearson(maxlen=self.maxlen))
            self.state[mid] = st

        ts = _parse_ts_iso(rec.get("ts"))
        if ts is None:
            return []

        st.rp.add(rec.get("speed"), rec.get("hard_temp"))

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

    ap = argparse.ArgumentParser()
    ap.add_argument("--offset", choices=["earliest", "latest"], default="latest")
    ap.add_argument("--group", default="wireqc-corr")
    ap.add_argument("--window-s", type=int, default=120)
    ap.add_argument("--emit-every-s", type=int, default=5)
    ap.add_argument("--sample-hz", type=int, default=10)
    args = ap.parse_args()

    cfg = load_config()
    CorrelationService(
        cfg,
        window_s=args.window_s,
        emit_every_s=args.emit_every_s,
        sample_hz=args.sample_hz,
        group_id=args.group,
        auto_offset_reset=args.offset,
    ).run()


if __name__ == "__main__":
    main()