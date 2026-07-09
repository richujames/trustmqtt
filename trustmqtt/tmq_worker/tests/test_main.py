import json

from tmq_worker.__main__ import ScoringContext
from tmq_worker.config import TmqConfig
from tmq_worker.storage import FeatureWindowRow, Fingerprint, VerdictHistory


class _FakePipeline:
    def __init__(self, store):
        self.store = store
        self.ops = []

    def set(self, key, value, ex=None):
        self.ops.append(("set", key, value))
        return self

    def hset(self, key, mapping=None):
        self.ops.append(("hset", key, mapping))
        return self

    def expire(self, key, ttl):
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

    def hget(self, key, field):
        return None  # no trained cohort models -> drift cold-starts to 0.0


def make_ctx(fingerprint_only=False):
    config = TmqConfig(database_url="sqlite:///:memory:", window_s=60)
    return ScoringContext(config, _FakeRedis(), fingerprint_only=fingerprint_only)


def make_evt(ts, event, client_id="dev-1", **kw):
    e = {"v": 1, "ts": ts, "event": event, "client_id": client_id}
    e.update(kw)
    return e


def test_enforce_mode_end_to_end_writes_feature_window_verdict_and_redis():
    ctx = make_ctx()
    ctx.on_event(make_evt(0.0, "connect", keepalive=30, username="plant-a"))
    for i in range(5):
        ctx.on_event(make_evt(i * 5.0, "publish", topic="a/1", qos=1, payload_len=50))
    # Roll the window over.
    ctx.on_event(make_evt(65.0, "publish", topic="a/1", qos=1, payload_len=50))

    session = ctx.Session()
    fw_rows = session.query(FeatureWindowRow).all()
    verdicts = session.query(VerdictHistory).all()
    session.close()

    assert len(fw_rows) == 1
    assert fw_rows[0].trust is not None
    assert len(verdicts) == 1
    assert "tmq:verdictp:dev-1" in ctx.redis.store


def test_fingerprint_only_mode_skips_verdicts_and_exports_doc(tmp_path):
    ctx = make_ctx(fingerprint_only=True)
    ctx.on_event(make_evt(0.0, "connect", keepalive=30))
    ctx.on_event(make_evt(1.0, "publish", topic="a/1", qos=0, payload_len=10))
    ctx.on_event(make_evt(65.0, "publish", topic="a/1", qos=0, payload_len=10))  # rolls the window

    session = ctx.Session()
    fw_rows = session.query(FeatureWindowRow).all()
    verdicts = session.query(VerdictHistory).all()
    assert len(fw_rows) == 1
    assert len(verdicts) == 0  # never touched policy/verdicts in fingerprint-only mode
    session.close()
    assert "tmq:verdictp:dev-1" not in ctx.redis.store

    fp_dir = tmp_path / "fingerprints"
    ctx.export_fingerprints(str(fp_dir))

    session = ctx.Session()
    fingerprints = session.query(Fingerprint).all()
    assert len(fingerprints) == 1
    assert 0.0 <= fingerprints[0].stability <= 1.0
    session.close()

    doc_path = fp_dir / "dev-1.json"
    assert doc_path.exists()
    doc = json.loads(doc_path.read_text(encoding="utf-8"))
    assert doc["client_id"] == "dev-1"
    assert "feature_baseline" in doc
