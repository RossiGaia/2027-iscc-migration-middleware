import json, os, time, threading

_path = os.path.join(os.environ.get("EVENTS_DIR", "/events"),
                     f"{os.environ.get('POD_NAME', 'dt')}.jsonl")
_f = open(_path, "a", buffering=1)  # line-buffered: each line reaches disk
_lock = threading.Lock()
_base = {"run_id": os.environ.get("RUN_ID"),
         "pod": os.environ.get("POD_NAME"),
         "node": os.environ.get("NODE_NAME")}

def emit(event, **fields):
    line = json.dumps({**_base, "event": event, "ts": time.time_ns(), **fields})
    with _lock:
        _f.write(line + "\n")