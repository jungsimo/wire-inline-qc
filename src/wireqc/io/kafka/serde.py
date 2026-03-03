import json

def dumps(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")

def loads(b: bytes):
    return json.loads(b.decode("utf-8"))