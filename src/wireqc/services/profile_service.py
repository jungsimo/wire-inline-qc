from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

from wireqc.services.base_service import BaseStreamService
from wireqc.io.processdata_parser import parse_raw_message


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
class ProfileFSM:
    # thresholds (based on your offline EDA; add hysteresis)
    low_entry: float = 12.30   # "definitely low"
    low_exit: float = 12.50    # leave low -> start rise
    high_entry: float = 16.50  # enter high -> start plateau
    high_exit: float = 16.30   # leave high -> start fall

    state: str = "LOW"  # LOW -> RISE -> HIGH -> FALL -> LOW
    low_start: Optional[float] = None
    rise_start: Optional[float] = None
    plateau_start: Optional[float] = None
    fall_start: Optional[float] = None

    last_len: Optional[float] = None
    last_ring: Optional[str] = None

    def reset(self, cur_len: Optional[float] = None, ring_id: Optional[str] = None):
        self.state = "LOW"
        self.low_start = cur_len
        self.rise_start = None
        self.plateau_start = None
        self.fall_start = None
        self.last_len = cur_len
        self.last_ring = ring_id

    def step(self, length_mm: float, diameter: float, ring_id: Optional[str]) -> dict | None:
        # reset conditions: length jumps backwards or ring changes
        if self.last_len is not None and length_mm < self.last_len:
            self.reset(cur_len=length_mm, ring_id=ring_id)
        if self.last_ring is not None and ring_id is not None and ring_id != self.last_ring:
            self.reset(cur_len=length_mm, ring_id=ring_id)

        self.last_len = length_mm
        self.last_ring = ring_id

        # initialize low_start
        if self.low_start is None:
            self.low_start = length_mm

        if self.state == "LOW":
            # keep low_start pinned when we're safely in low
            if diameter <= self.low_entry:
                self.low_start = self.low_start if self.low_start is not None else length_mm
            # start rise when leaving low
            if diameter > self.low_exit:
                self.rise_start = length_mm
                self.state = "RISE"

        elif self.state == "RISE":
            # plateau begins when entering high
            if diameter >= self.high_entry:
                self.plateau_start = length_mm
                self.state = "HIGH"

        elif self.state == "HIGH":
            # fall begins when leaving high
            if diameter < self.high_exit:
                self.fall_start = length_mm
                self.state = "FALL"

        elif self.state == "FALL":
            # profile ends when re-entering low
            if diameter <= self.low_entry:
                low2_start = length_mm

                # compute lengths if all markers exist
                if None not in (self.low_start, self.rise_start, self.plateau_start, self.fall_start):
                    L2 = self.rise_start - self.low_start
                    L3 = self.plateau_start - self.rise_start
                    L1 = self.fall_start - self.plateau_start
                    L4 = low2_start - self.fall_start

                    event = {
                        "wire_length_end_mm": low2_start,
                        "ring_id": ring_id,
                        "L1_plateau_mm": float(L1),
                        "L2_low_mm": float(L2),
                        "L3_rise_mm": float(L3),
                        "L4_fall_mm": float(L4),
                    }
                else:
                    event = None

                # reset for next profile, low starts at low2_start
                self.reset(cur_len=low2_start, ring_id=ring_id)
                return event

        return None


class ProfileService(BaseStreamService):
    def __init__(self, cfg: dict):
        super().__init__(
            cfg=cfg,
            group_id="wireqc-profiles",
            in_topics=[cfg["topics"]["raw"]],
            parse_fn=parse_raw_message,
        )
        self.out_topic = cfg["topics"]["profiles"]
        self.fsm_by_machine: dict[str, ProfileFSM] = {}
        self.profile_seq: dict[str, int] = {}

    def handle(self, rec: dict) -> Iterable[tuple[str, dict, str | None]]:
        mid = str(rec.get("machine_id") or "unknown")
        ts = _parse_ts_iso(rec.get("ts"))
        length_mm = rec.get("wire_length_mm")
        diameter = rec.get("diameter")
        ring_id = rec.get("ring_id")

        if ts is None or length_mm is None or diameter is None or rec.get("machine_id") is None:
            return []

        fsm = self.fsm_by_machine.get(mid)
        if fsm is None:
            fsm = ProfileFSM()
            fsm.reset(cur_len=length_mm, ring_id=ring_id)
            self.fsm_by_machine[mid] = fsm
            self.profile_seq[mid] = 0

        prof = fsm.step(length_mm=float(length_mm), diameter=float(diameter), ring_id=ring_id)
        if prof is None:
            return []

        self.profile_seq[mid] += 1
        event = {
            "ts": ts.isoformat(),
            "machine_id": rec["machine_id"],
            "profile_id": self.profile_seq[mid],
            **prof,
        }
        return [(self.out_topic, event, mid)]


def main():
    from wireqc.common.config import load_config
    cfg = load_config()
    ProfileService(cfg).run()


if __name__ == "__main__":
    main()