from __future__ import annotations

import argparse
import statistics
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
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


class CausalMedianMean:
    """
    Kausale Glättung für Live-Streaming:
    - Median über die letzten median_n Rohwerte
    - danach Mittelwert über die letzten mean_n Medianwerte
    """
    def __init__(self, median_n: int = 5, mean_n: int = 5):
        self.raw_q = deque(maxlen=median_n)
        self.med_q = deque(maxlen=mean_n)

    def update(self, x: Optional[float]) -> Optional[float]:
        if x is None:
            return None

        x = float(x)
        self.raw_q.append(x)

        med = statistics.median(self.raw_q)
        self.med_q.append(float(med))

        return float(sum(self.med_q) / len(self.med_q))


@dataclass
class ProfileFSMRobust:
    low_entry: float = 12.30
    low_exit: float = 12.50
    high_entry: float = 16.50
    high_exit: float = 16.30

    min_low_samples: int = 20
    min_rise_positive_slope: int = 3

    state: str = "UNSYNCED"

    low_count: int = 0
    pos_slope_count: int = 0

    low_candidate_start_len: Optional[float] = None
    low_candidate_start_ts: Optional[datetime] = None
    low_candidate_start_idx: Optional[int] = None

    low_start_len: Optional[float] = None
    rise_start_len: Optional[float] = None
    plateau_start_len: Optional[float] = None
    fall_start_len: Optional[float] = None

    low_start_ts: Optional[datetime] = None
    rise_start_ts: Optional[datetime] = None

    low_start_idx: Optional[int] = None
    rise_start_idx: Optional[int] = None

    last_len: Optional[float] = None

    def _clear_profile_markers(self) -> None:
        self.low_start_len = None
        self.rise_start_len = None
        self.plateau_start_len = None
        self.fall_start_len = None

        self.low_start_ts = None
        self.rise_start_ts = None

        self.low_start_idx = None
        self.rise_start_idx = None

    def _enter_unsynced(self) -> None:
        self.state = "UNSYNCED"
        self.low_count = 0
        self.pos_slope_count = 0

        self.low_candidate_start_len = None
        self.low_candidate_start_ts = None
        self.low_candidate_start_idx = None

        self._clear_profile_markers()

    def step(
        self,
        idx: int,
        ts: datetime,
        length_mm: float,
        diameter_smooth: float,
        slope: float,
    ) -> Optional[dict]:
        # Reset bei rücklaufender Länge
        if self.last_len is not None and length_mm < self.last_len:
            self._enter_unsynced()

        self.last_len = length_mm

        # ---------------- UNSYNCED ----------------
        if self.state == "UNSYNCED":
            if diameter_smooth <= self.low_entry:
                if self.low_count == 0:
                    self.low_candidate_start_len = length_mm
                    self.low_candidate_start_ts = ts
                    self.low_candidate_start_idx = idx

                self.low_count += 1

                if self.low_count >= self.min_low_samples:
                    self.state = "LOW"
                    self.low_start_len = self.low_candidate_start_len
                    self.low_start_ts = self.low_candidate_start_ts
                    self.low_start_idx = self.low_candidate_start_idx
            else:
                self.low_count = 0
                self.low_candidate_start_len = None
                self.low_candidate_start_ts = None
                self.low_candidate_start_idx = None

            return None

        # ---------------- LOW ----------------
        if self.state == "LOW":
            if diameter_smooth <= self.low_entry:
                self.pos_slope_count = 0
                return None

            if slope > 0.0:
                self.pos_slope_count += 1
            else:
                self.pos_slope_count = 0

            if diameter_smooth > self.low_exit and self.pos_slope_count >= self.min_rise_positive_slope:
                self.rise_start_len = length_mm
                self.rise_start_ts = ts
                self.rise_start_idx = idx
                self.state = "RISE"
                self.pos_slope_count = 0

            return None

        # ---------------- RISE ----------------
        if self.state == "RISE":
            if diameter_smooth >= self.high_entry:
                self.plateau_start_len = length_mm
                self.state = "HIGH"
            return None

        # ---------------- HIGH ----------------
        if self.state == "HIGH":
            if diameter_smooth < self.high_exit:
                self.fall_start_len = length_mm
                self.state = "FALL"
            return None

        # ---------------- FALL ----------------
        if self.state == "FALL":
            if diameter_smooth <= self.low_entry:
                end_len = length_mm
                end_ts = ts
                end_idx = idx

                event = None
                if None not in (
                    self.low_start_len,
                    self.rise_start_len,
                    self.plateau_start_len,
                    self.fall_start_len,
                    self.low_start_ts,
                    self.rise_start_ts,
                    self.low_start_idx,
                    self.rise_start_idx,
                ):
                    L2 = float(self.rise_start_len - self.low_start_len)
                    L3 = float(self.plateau_start_len - self.rise_start_len)
                    L1 = float(self.fall_start_len - self.plateau_start_len)
                    L4 = float(end_len - self.fall_start_len)
                    L_total = float(L1 + L2 + L3 + L4)

                    event = {
                        "low_start_mm": float(self.low_start_len),
                        "rise_start_mm": float(self.rise_start_len),
                        "plateau_start_mm": float(self.plateau_start_len),
                        "fall_start_mm": float(self.fall_start_len),
                        "wire_length_end_mm": float(end_len),

                        "L1_plateau_mm": L1,
                        "L2_low_mm": L2,
                        "L3_rise_mm": L3,
                        "L4_fall_mm": L4,
                        "L_total_mm": L_total,

                        "profile_raw_count": int(end_idx - self.low_start_idx + 1),
                        "profile_seconds": float((end_ts - self.low_start_ts).total_seconds()),

                        "bump_raw_count": int(end_idx - self.rise_start_idx + 1),
                        "bump_seconds": float((end_ts - self.rise_start_ts).total_seconds()),
                    }

                # Nach Profilende wieder neu synchronisieren
                self.state = "UNSYNCED"
                self.low_count = 1
                self.pos_slope_count = 0
                self.low_candidate_start_len = end_len
                self.low_candidate_start_ts = end_ts
                self.low_candidate_start_idx = end_idx
                self._clear_profile_markers()

                return event

            return None

        return None


@dataclass
class MachineProfileState:
    smoother: CausalMedianMean = field(default_factory=lambda: CausalMedianMean(median_n=5, mean_n=5))
    fsm: ProfileFSMRobust = field(default_factory=ProfileFSMRobust)
    raw_idx: int = 0
    last_smooth: Optional[float] = None


class ProfileService(BaseStreamService):
    def __init__(
        self,
        cfg: dict,
        group_id: str = "wireqc-profiles",
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
        self.out_topic = cfg["topics"]["profiles"]
        self.out_topic_nio = cfg["topics"]["profiles_nio"]

        self.machine_state: Dict[str, MachineProfileState] = {}
        self.profile_seq_valid: Dict[str, int] = {}
        self.profile_seq_detected: Dict[str, int] = {}

        self.stats_by_machine: Dict[str, ProfileStats] = {}

        self.min_profiles_eval = 20
        self.tol_mm = 30.0

    def _is_plausible_profile(self, event: dict) -> bool:
        L1 = event["L1_plateau_mm"]
        L2 = event["L2_low_mm"]
        L3 = event["L3_rise_mm"]
        L4 = event["L4_fall_mm"]
        L_total = event["L_total_mm"]
        raw_n = event["profile_raw_count"]
        sec = event["profile_seconds"]

        return (
            5550.0 <= L1 <= 5900.0 and
            930.0 <= L2 <= 1010.0 and
            190.0 <= L3 <= 260.0 and
            205.0 <= L4 <= 260.0 and
            6950.0 <= L_total <= 7300.0 and
            1008 <= raw_n <= 1018 and
            100.8 <= sec <= 101.8
        )

    def _get_machine_state(self, mid: str) -> MachineProfileState:
        st = self.machine_state.get(mid)
        if st is None:
            st = MachineProfileState()
            self.machine_state[mid] = st
            self.profile_seq_valid[mid] = 0
            self.profile_seq_detected[mid] = 0
        return st

    def _get_stats(self, mid: str) -> ProfileStats:
        st = self.stats_by_machine.get(mid)
        if st is None:
            st = ProfileStats()
            self.stats_by_machine[mid] = st
        return st

    def handle(self, rec: dict) -> Iterable[Tuple[str, dict, Optional[str]]]:
        mid = str(rec.get("machine_id") or "unknown")
        machine_id = rec.get("machine_id")
        ts = _parse_ts_iso(rec.get("ts"))
        length_mm = rec.get("wire_length_mm")
        diameter = rec.get("diameter")
        ring_id = rec.get("ring_id")

        if ts is None or machine_id is None or length_mm is None or diameter is None:
            return []

        mstate = self._get_machine_state(mid)
        mstate.raw_idx += 1

        smooth = mstate.smoother.update(float(diameter))
        if smooth is None:
            return []

        if mstate.last_smooth is None:
            slope = 0.0
        else:
            slope = float(smooth - mstate.last_smooth)
        mstate.last_smooth = smooth

        prof = mstate.fsm.step(
            idx=mstate.raw_idx,
            ts=ts,
            length_mm=float(length_mm),
            diameter_smooth=float(smooth),
            slope=float(slope),
        )
        if prof is None:
            return []

        self.profile_seq_detected[mid] += 1
        detected_id = self.profile_seq_detected[mid]

        event = {
            "ts": ts.isoformat(),
            "machine_id": machine_id,
            "detected_profile_id": detected_id,
            "ring_id": ring_id,
            **prof,
        }

        valid_profile = self._is_plausible_profile(event)
        event["valid_profile"] = valid_profile

        if not valid_profile:
            print(
                "[profiles] INVALID detected_profile_id={0} "
                "L1={1:.1f} L2={2:.1f} L3={3:.1f} L4={4:.1f} "
                "L_total={5:.1f} raw={6} sec={7:.1f}".format(
                    detected_id,
                    event["L1_plateau_mm"],
                    event["L2_low_mm"],
                    event["L3_rise_mm"],
                    event["L4_fall_mm"],
                    event["L_total_mm"],
                    event["profile_raw_count"],
                    event["profile_seconds"],
                )
            )
            return []

        self.profile_seq_valid[mid] += 1
        profile_id = self.profile_seq_valid[mid]
        event["profile_id"] = profile_id

        stats = self._get_stats(mid)

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

        stats.L1.update(event["L1_plateau_mm"])
        stats.L2.update(event["L2_low_mm"])
        stats.L3.update(event["L3_rise_mm"])
        stats.L4.update(event["L4_fall_mm"])

        event["nio"] = nio
        event["profiles_seen"] = stats.n

        print(
            "[profiles] VALID profile_id={0} "
            "L1={1:.1f} L2={2:.1f} L3={3:.1f} L4={4:.1f} "
            "L_total={5:.1f} raw={6} sec={7:.1f}".format(
                profile_id,
                event["L1_plateau_mm"],
                event["L2_low_mm"],
                event["L3_rise_mm"],
                event["L4_fall_mm"],
                event["L_total_mm"],
                event["profile_raw_count"],
                event["profile_seconds"],
            )
        )

        outs = [(self.out_topic, event, mid)]

        if nio:
            nio_event = {
                "ts": event["ts"],
                "machine_id": event["machine_id"],
                "profile_id": event["profile_id"],
                "detected_profile_id": event["detected_profile_id"],
                "ring_id": event.get("ring_id"),
                "wire_length_end_mm": event.get("wire_length_end_mm"),
                "L_total_mm": event.get("L_total_mm"),
                "profile_raw_count": event.get("profile_raw_count"),
                "profile_seconds": event.get("profile_seconds"),
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
            print("[profiles] n.i.O. profile_id={0}".format(event["profile_id"]))
            outs.append((self.out_topic_nio, nio_event, mid))

        return outs


def main():
    from wireqc.common.config import load_config

    ap = argparse.ArgumentParser()
    ap.add_argument("--offset", choices=["earliest", "latest"], default="latest")
    ap.add_argument("--group", default="wireqc-profiles")
    args = ap.parse_args()

    cfg = load_config()
    ProfileService(
        cfg,
        group_id=args.group,
        auto_offset_reset=args.offset,
    ).run()


if __name__ == "__main__":
    main()