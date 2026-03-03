from pathlib import Path
import yaml

def load_config(path: str = "config/base.yaml") -> dict:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)