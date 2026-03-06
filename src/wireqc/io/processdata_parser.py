from typing import Optional, Dict, Any

LENGTH_KEY = "fDrahtlaenge1 (Drehgeber)"
SPEED_KEY = "fGeschwindigkeit1 (Drehgeber)"
DIAM_KEY = "fDurchmesser1 (Haerten)"
HARDT_KEY = "fHaertetemperatur (Haerten)"
TEMPT_KEY = "fAnlasstemperatur (Anlassen)"
RING_KEY = "Ring ID"


def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def parse_raw_message(raw: Dict[str, Any]) -> Dict[str, Any]:
    ts = raw.get("Time")
    machine_id = raw.get("MachineId")

    wire_length_mm = None
    speed = None
    diameter = None
    hard_temp = None
    temp_temp = None
    ring_id = None

    for item in raw.get("ProcessData", []):
        name = item.get("Name") or item.get("name")
        if name is None:
            continue

        val = item.get("Value") if "Value" in item else item.get("value")

        if name == LENGTH_KEY:
            wire_length_mm = _to_float(val)
        elif name == SPEED_KEY:
            speed = _to_float(val)
        elif name == DIAM_KEY:
            diameter = _to_float(val)
        elif name == HARDT_KEY:
            hard_temp = _to_float(val)
        elif name == TEMPT_KEY:
            temp_temp = _to_float(val)
        elif name == RING_KEY:
            ring_id = val

    return {
        "ts": ts,
        "machine_id": machine_id,
        "wire_length_mm": wire_length_mm,
        "speed": speed,
        "diameter": diameter,
        "hard_temp": hard_temp,
        "temp_temp": temp_temp,
        "ring_id": ring_id,
    }