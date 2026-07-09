from tmq_worker.config import load_config
from tmq_worker.incidents import IncidentService
from tmq_worker.policy import Verdict, VerdictLevel
from tmq_worker.storage import (
    Incident, get_engine, get_or_create_client, get_sessionmaker, init_db,
)


def make_session_and_config():
    engine = get_engine("sqlite:///:memory:")
    init_db(engine)
    session = get_sessionmaker(engine)()
    config = load_config(path="/nonexistent/tmq.yaml")  # defaults only
    config.llm.enabled = False  # never hit the network in tests
    return session, config


def test_incident_opens_on_throttle_and_gets_report():
    session, config = make_session_and_config()
    client = get_or_create_client(session, "dev-1")
    session.commit()
    svc = IncidentService(session_factory=None, config=config)

    verdict = Verdict(level=VerdictLevel.THROTTLE, score=0.6, rate=2.0, reason="fsm=0.8 drift=0.2 fleet=0.0")
    incident = svc.maybe_open_or_update(session, client, verdict, trust=0.6, fsm_diff=[], fleet_alarm=False)

    assert incident is not None
    assert incident.closed_ts is None
    assert incident.peak_level == VerdictLevel.THROTTLE
    assert incident.report_md is not None
    assert "deterministic fallback" in incident.report_md
    assert session.query(Incident).count() == 1


def test_incident_below_threshold_does_not_open_one():
    session, config = make_session_and_config()
    client = get_or_create_client(session, "dev-1")
    session.commit()
    svc = IncidentService(session_factory=None, config=config)

    verdict = Verdict(level=VerdictLevel.WATCH, score=0.35, rate=0.0, reason="")
    incident = svc.maybe_open_or_update(session, client, verdict, trust=0.35, fsm_diff=[], fleet_alarm=False)

    assert incident is None
    assert session.query(Incident).count() == 0


def test_incident_closes_when_verdict_drops_back_down():
    session, config = make_session_and_config()
    client = get_or_create_client(session, "dev-1")
    session.commit()
    svc = IncidentService(session_factory=None, config=config)

    svc.maybe_open_or_update(
        session, client,
        Verdict(level=VerdictLevel.QUARANTINE, score=0.75, rate=0.0, reason="r"),
        trust=0.75, fsm_diff=[], fleet_alarm=False,
    )
    assert session.query(Incident).filter_by(closed_ts=None).count() == 1

    svc.maybe_open_or_update(
        session, client,
        Verdict(level=VerdictLevel.ALLOW, score=0.1, rate=0.0, reason="r"),
        trust=0.1, fsm_diff=[], fleet_alarm=False,
    )
    assert session.query(Incident).filter_by(closed_ts=None).count() == 0


def test_incident_peak_level_and_score_only_increase():
    session, config = make_session_and_config()
    client = get_or_create_client(session, "dev-1")
    session.commit()
    svc = IncidentService(session_factory=None, config=config)

    svc.maybe_open_or_update(
        session, client,
        Verdict(level=VerdictLevel.QUARANTINE, score=0.75, rate=0.0, reason="r"),
        trust=0.75, fsm_diff=[], fleet_alarm=False,
    )
    incident = svc.maybe_open_or_update(
        session, client,
        Verdict(level=VerdictLevel.THROTTLE, score=0.55, rate=1.0, reason="r"),
        trust=0.55, fsm_diff=[], fleet_alarm=False,
    )
    assert incident.peak_level == VerdictLevel.QUARANTINE  # didn't regress
    assert incident.peak_score == 0.75


def test_fleet_alarm_alone_opens_an_incident():
    session, config = make_session_and_config()
    client = get_or_create_client(session, "dev-1")
    session.commit()
    svc = IncidentService(session_factory=None, config=config)

    verdict = Verdict(level=VerdictLevel.ALLOW, score=0.1, rate=0.0, reason="fleet coordinated drift")
    incident = svc.maybe_open_or_update(session, client, verdict, trust=0.1, fsm_diff=[], fleet_alarm=True)
    assert incident is not None
    assert incident.fleet is True
