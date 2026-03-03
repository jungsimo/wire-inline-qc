import json
import gzip
from pathlib import Path
import pandas as pd

def load_process_json(path):
    path = Path(path)
    is_gz = path.suffix.lower() == ".gz"

    # JSON Lines
    try:
        if is_gz:
            return pd.read_json(path, lines=True, compression="gzip")
        return pd.read_json(path, lines=True)
    except ValueError:
        pass

    # JSON Array
    try:
        if is_gz:
            return pd.read_json(path, compression="gzip")
        return pd.read_json(path)
    except ValueError:
        pass

    # Fallback: line-by-line
    opener = gzip.open if is_gz else open
    mode = "rt" if is_gz else "r"

    records = []
    with opener(path, mode, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.endswith(","):
                line = line[:-1]
            records.append(json.loads(line))
    return pd.DataFrame.from_records(records)