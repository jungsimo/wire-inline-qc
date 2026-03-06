# src/tools/decompress_data.py
from __future__ import annotations
import argparse
import gzip
from pathlib import Path
import shutil

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inp", default="data/process_data.json.gz")
    ap.add_argument("--out", default="data/process_data.json")  # uncompressed JSONL
    args = ap.parse_args()

    inp = Path(args.inp)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    if out.exists():
        print(f"[decompress] exists: {out} (skip)")
        return

    print(f"[decompress] {inp} -> {out}")
    with gzip.open(inp, "rb") as f_in, out.open("wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    print("[decompress] done")

if __name__ == "__main__":
    main()