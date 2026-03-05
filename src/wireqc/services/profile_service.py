from __future__ import annotations
from dataclasses import dataclass, field
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
class RunningMean:
    n: int = 0
    mean: float = 0.0

    def update(self, x: float) -> float:
        self.n += 1
        self.mean += (x - self.mean) / self.n
        return self.mean


@dataclass
class ProfileStats:
    L1: RunningMean = field(default_factory=RunningMean)
    L2: RunningMean = field(default_factory=RunningMean)
    L3: RunningMean = field(default_factory=RunningMean)
    L4: RunningMean = field(default_factory=RunningMean)

    @property
    def n(self) -> int:
        return self.L1.n

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

        self.out_topic_nio = cfg["topics"]["profiles_nio"]

        self.stats_by_machine: dict[str, ProfileStats] = {}
        self.min_profiles_eval = 20  # erst ab N Profilen n.i.O. bewerten (Warmup)
        self.tol_mm = 30.0

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
        # --- n.i.O. Bewertung (Streaming) ---
        stats = self.stats_by_machine.get(mid)
        if stats is None:
            stats = ProfileStats()
            self.stats_by_machine[mid] = stats

        # expected = laufender Mittelwert VOR Update (Warmup beachten)
        expected = None
        deviations = None
        nio = False

        if stats.n >= self.min_profiles_eval:
            expected = {
                "L1": stats.L1.mean,
                "L2": stats.L2.mean,
                "L3": stats.L3.mean,
                "L4": stats.L4.mean,
            }
            deviations = {
                "dL1": event["L1_plateau_mm"] - expected["L1"],
                "dL2": event["L2_low_mm"] - expected["L2"],
                "dL3": event["L3_rise_mm"] - expected["L3"],
                "dL4": event["L4_fall_mm"] - expected["L4"],
            }
            nio = any(abs(d) > self.tol_mm for d in deviations.values())

        # Mittelwerte jetzt mit aktuellem Profil updaten (Mean über alle erkannten Profile)
        stats.L1.update(event["L1_plateau_mm"])
        stats.L2.update(event["L2_low_mm"])
        stats.L3.update(event["L3_rise_mm"])
        stats.L4.update(event["L4_fall_mm"])

        # optional fürs spätere Visualisieren/Debuggen:
        event["nio"] = nio
        event["profiles_seen"] = stats.n

        outs = [(self.out_topic, event, mid)]

        if nio:
            nio_event = {
                "ts": event["ts"],
                "machine_id": event["machine_id"],
                "profile_id": event["profile_id"],
                "ring_id": event.get("ring_id"),
                "wire_length_end_mm": event.get("wire_length_end_mm"),
                "tol_mm": self.tol_mm,
                "profiles_seen": stats.n,
                "expected": expected,
                "deviations": deviations,
                "lengths": {
                    "L1": event["L1_plateau_mm"],
                    "L2": event["L2_low_mm"],
                    "L3": event["L3_rise_mm"],
                    "L4": event["L4_fall_mm"],
                },
            }
            outs.append((self.out_topic_nio, nio_event, mid))

        return outs


def main():
    from wireqc.common.config import load_config
    cfg = load_config()
    ProfileService(cfg).run()


if __name__ == "__main__":
    main()