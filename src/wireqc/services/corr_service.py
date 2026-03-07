from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from math import sqrt
from typing import Iterable, Optional, Tuple, Dict

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
    """
    Rolling-Pearson-Korrelation zwischen speed und hard_temp.
    Ausgabe in separates Kafka-Topic.
    """
    def __init__(
        self,
        cfg: dict,
        window_samples: int = 1000,
        emit_every_s: int = 10,
        min_samples: int = 100,
        min_speed_std: float = 0.2,
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
        self.window_samples = window_samples
        self.emit_every_s = emit_every_s
        self.min_samples = min_samples
        self.min_speed_std = min_speed_std
        self.state = {}

    def _ensure_state(self, mid: str) -> CorrState:
        st = self.state.get(mid)
        if st is None:
            st = CorrState(rp=RollingPearson(maxlen=self.window_samples))
            self.state[mid] = st
        return st

    def _std_from_sums(self, n: int, s: float, ss: float) -> Optional[float]:
        if n < 2:
            return None
        mean = s / n
        var = (ss - n * mean * mean) / (n - 1)
        if var < 0:
            var = 0.0
        return sqrt(var)

    def handle(self, rec: dict) -> Iterable[Tuple[str, dict, Optional[str]]]:
        mid = str(rec.get("machine_id") or "unknown")
        machine_id = rec.get("machine_id")
        ts = _parse_ts_iso(rec.get("ts"))

        if ts is None or machine_id is None:
            return []

        st = self._ensure_state(mid)

        speed = rec.get("speed")
        hard_temp = rec.get("hard_temp")

        st.rp.add(speed, hard_temp)

        if st.last_emit is None or (ts - st.last_emit).total_seconds() >= self.emit_every_s:
            st.last_emit = ts

            corr = st.rp.value()
            n = st.rp.n
            speed_std = self._std_from_sums(st.rp.n, st.rp.sx, st.rp.sxx)
            hard_temp_std = self._std_from_sums(st.rp.n, st.rp.sy, st.rp.syy)

            corr_valid = True
            corr_reason = "ok"

            if n < self.min_samples:
                corr_valid = False
                corr_reason = "insufficient_samples"
            elif speed_std is None or speed_std < self.min_speed_std:
                corr_valid = False
                corr_reason = "insufficient_speed_variance"
            elif corr is None:
                corr_valid = False
                corr_reason = "corr_not_defined"

            event = {
                "ts": ts.isoformat(),
                "machine_id": machine_id,
                "window_samples": n,
                "speed_std_window": speed_std,
                "hard_temp_std_window": hard_temp_std,
                "pearson_corr_speed_hardtemp": corr,
                "corr_valid": corr_valid,
                "corr_reason": corr_reason,
            }

            print(
                "[corr] n={0} corr={1} speed_std={2} valid={3} reason={4}".format(
                    n,
                    "None" if corr is None else "{0:.6f}".format(corr),
                    "None" if speed_std is None else "{0:.6f}".format(speed_std),
                    corr_valid,
                    corr_reason,
                )
            )

            return [(self.out_topic, event, mid)]

        return []


def main():
    from wireqc.common.config import load_config

    ap = argparse.ArgumentParser()
    ap.add_argument("--offset", choices=["earliest", "latest"], default="latest")
    ap.add_argument("--group", default="wireqc-corr")
    ap.add_argument("--window-samples", type=int, default=1000)
    ap.add_argument("--emit-every-s", type=int, default=10)
    ap.add_argument("--min-samples", type=int, default=100)
    ap.add_argument("--min-speed-std", type=float, default=0.2)
    args = ap.parse_args()

    cfg = load_config()
    CorrelationService(
        cfg,
        window_samples=args.window_samples,
        emit_every_s=args.emit_every_s,
        min_samples=args.min_samples,
        min_speed_std=args.min_speed_std,
        group_id=args.group,
        auto_offset_reset=args.offset,
    ).run()


if __name__ == "__main__":
    main()