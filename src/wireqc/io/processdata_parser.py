from datetime import datetime
import pandas as pd

# Zielschema:
# {
#   "ts": "2026-03-01T08:00:00.100Z" (oder ISO ohne Z),
#   "machine_id": 1031103,
#   "wire_length_mm": float,
#   "speed": float,
#   "diameter": float,
#   "hard_temp": float,
#   "temp_temp": float,
#   "ring_id": ...
# }

LENGTH_KEY = "fDrahtlaenge1 (Drehgeber)"
SPEED_KEY  = "fGeschwindigkeit1 (Drehgeber)"
DIAM_KEY   = "fDurchmesser1 (Haerten)"
HARDT_KEY  = "fHaertetemperatur (Haerten)"
TEMPT_KEY  = "fAnlasstemperatur (Anlassen)"
RING_KEY   = "Ring ID"

def parse_raw_message(raw: dict) -> dict:
    # raw ist vermutlich wie dein JSON: {"Time":..., "MachineId":..., "ProcessData":[{Name,Value},...]}
    ts = raw.get("Time")
    machine_id = raw.get("MachineId")

    values = {}
    for item in raw.get("ProcessData", []):
        name = item.get("Name") or item.get("name")
        val = item.get("Value") if "Value" in item else item.get("value")
        if name is not None:
            values[name] = val

    def _to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    rec = {
        "ts": ts,  # ISO-String aus Source; im Service ggf. in datetime parsen
        "machine_id": machine_id,
        "wire_length_mm": _to_float(values.get(LENGTH_KEY)),
        "speed": _to_float(values.get(SPEED_KEY)),
        "diameter": _to_float(values.get(DIAM_KEY)),
        "hard_temp": _to_float(values.get(HARDT_KEY)),
        "temp_temp": _to_float(values.get(TEMPT_KEY)),
        "ring_id": values.get(RING_KEY),
    }
    return rec