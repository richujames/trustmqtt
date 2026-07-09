from tmq_worker.policy import Verdict, VerdictLevel
from tmq_worker.verdicts import write_verdict


class _FakePipeline:
    def __init__(self, store):
        self.store = store
        self.ops = []

    def set(self, key, value, ex=None):
        self.ops.append(("set", key, value, ex))
        return self

    def hset(self, key, mapping=None):
        self.ops.append(("hset", key, mapping))
        return self

    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl))
        return self

    def execute(self):
        for op in self.ops:
            if op[0] == "set":
                self.store[op[1]] = op[2]
            elif op[0] == "hset":
                self.store.setdefault(op[1], {}).update(op[2])
        self.ops = []


class _FakeRedis:
    def __init__(self):
        self.store = {}

    def pipeline(self, transaction=True):
        return _FakePipeline(self.store)


def test_write_verdict_writes_both_packed_and_hash_forms():
    r = _FakeRedis()
    v = Verdict(level=VerdictLevel.THROTTLE, score=0.62, rate=3.5, reason="fsm_violation:pub_seq")
    write_verdict(r, "dev-1", v, now=1000.0)

    packed = r.store["tmq:verdictp:dev-1"]
    level, score, expires_at, rate = packed.split("|")
    assert int(level) == VerdictLevel.THROTTLE
    assert abs(float(score) - 0.62) < 1e-6
    assert int(expires_at) == 1120
    assert abs(float(rate) - 3.5) < 1e-6

    h = r.store["tmq:verdict:dev-1"]
    assert h["level"] == VerdictLevel.THROTTLE
    assert h["reason"] == "fsm_violation:pub_seq"
    assert h["updated_at"] == 1000.0
