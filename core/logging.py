import json, os, sys, time


class Logger:
def __init__(self, context: dict | None = None):
self.context = context or {}


def _log(self, level: str, msg: str, **kwargs):
record = {
"level": level,
"message": msg,
"ts": int(time.time()*1000),
**self.context,
**kwargs,
}
print(json.dumps(record))
sys.stdout.flush()


def info(self, msg: str, **kwargs):
self._log("INFO", msg, **kwargs)


def warn(self, msg: str, **kwargs):
self._log("WARN", msg, **kwargs)


def error(self, msg: str, **kwargs):
self._log("ERROR", msg, **kwargs)