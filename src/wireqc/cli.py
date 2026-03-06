# src/wireqc/cli.py
from __future__ import annotations

import argparse
import runpy
import sys


def _add_cmd(sub, name: str, module: str, help_text: str):
    p = sub.add_parser(name, help=help_text)
    p.add_argument("args", nargs=argparse.REMAINDER, help="Args werden an das Modul weitergereicht")
    p.set_defaults(_module=module)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="wireqc", description="Wire Inline QC – Streaming Services CLI")
    sub = ap.add_subparsers(dest="cmd", required=True)

    _add_cmd(sub, "create-topics", "tools.create_topics", "Kafka Topics aus config anlegen (idempotent)")
    _add_cmd(sub, "replay", "wireqc.services.replay_producer", "Replay-Producer (simuliert Datenstrom)")
    _add_cmd(sub, "corr", "wireqc.services.corr_service", "Teil 1: Korrelation (speed vs hard_temp)")
    _add_cmd(sub, "alarms", "wireqc.services.alarm_service", "Teil 2: Six-Sigma Alarmierung")
    _add_cmd(sub, "profiles", "wireqc.services.profile_service", "Teil 3: Profilerkennung + n.i.O.")
    _add_cmd(sub, "viz", "wireqc.services.viz_service", "Visualisierung: Counter (profiles/nio)")
    _add_cmd(sub, "viz-plot", "wireqc.services.viz_plot_service", "Visualisierung: Live-Plots (raw + counters)")

    ns = ap.parse_args(argv)
    module = ns._module
    forwarded = ns.args
    # strip leading "--" if user used it to separate args
    if forwarded and forwarded[0] == "--":
        forwarded = forwarded[1:]

    # forwarded args an das Zielmodul weiterreichen
    sys.argv = [module, *forwarded]
    runpy.run_module(module, run_name="__main__", alter_sys=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())