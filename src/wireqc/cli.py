from __future__ import annotations

import argparse
import runpy
import sys
from typing import List, Optional


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="wireqc", description="Wire Inline QC – Streaming Services CLI")
    sub = ap.add_subparsers(dest="cmd")

    def add_cmd(name: str, module: str, help_text: str):
        p = sub.add_parser(name, help=help_text)
        p.set_defaults(_module=module)

    add_cmd("create-topics", "tools.create_topics", "Kafka Topics aus config anlegen (idempotent)")
    add_cmd("replay", "wireqc.services.replay_producer", "Replay-Producer (simuliert Datenstrom)")
    add_cmd("corr", "wireqc.services.corr_service", "Teil 1: Korrelation (speed vs hard_temp)")
    add_cmd("alarms", "wireqc.services.alarm_service", "Teil 2: Six-Sigma Alarmierung")
    add_cmd("profiles", "wireqc.services.profile_service", "Teil 3: Profilerkennung + n.i.O.")
    add_cmd("viz", "wireqc.services.viz_service", "Visualisierung: Counter (profiles/nio)")
    add_cmd("viz-plot", "wireqc.services.viz_plot_service", "Visualisierung: Dashboard/Plots")

    ns, forwarded = ap.parse_known_args(argv)

    if not getattr(ns, "cmd", None):
        ap.print_help()
        return 2

    module = ns._module

    if forwarded and forwarded[0] == "--":
        forwarded = forwarded[1:]

    sys.argv = [module] + forwarded
    runpy.run_module(module, run_name="__main__", alter_sys=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())