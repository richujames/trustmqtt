"""Incident service (spec §7.1). Persists to Postgres first (source of
truth), then redacts and drafts a report. Opened when a client's verdict
reaches >= THROTTLE or a fleet alarm fires; closed once it drops back to
below that for a window.
"""
from __future__ import annotations

import datetime

from tmq_worker.llm import generate_report
from tmq_worker.policy import Verdict, VerdictLevel
from tmq_worker.redact import redact_incident_summary
from tmq_worker.storage import Client, Incident

OPEN_INCIDENT_LEVEL_THRESHOLD = VerdictLevel.THROTTLE


class IncidentService:
    def __init__(self, session_factory, config):
        self.session_factory = session_factory
        self.config = config
        self._open_incident_ids: dict[str, int] = {}

    def maybe_open_or_update(self, session, client: Client, verdict: Verdict, trust: float,
                              fsm_diff: list, fleet_alarm: bool):
        should_have_incident = verdict.level >= OPEN_INCIDENT_LEVEL_THRESHOLD or fleet_alarm
        incident_id = self._open_incident_ids.get(client.client_id)
        incident = session.get(Incident, incident_id) if incident_id else None

        if not should_have_incident:
            if incident is not None and incident.closed_ts is None:
                incident.closed_ts = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                session.commit()
                del self._open_incident_ids[client.client_id]
            return incident

        if incident is None:
            incident = Incident(
                client_id=client.id,
                fleet=fleet_alarm,
                opened_ts=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
                peak_level=int(verdict.level),
                peak_score=trust,
                fsm_diff=fsm_diff,
                summary={},
            )
            session.add(incident)
            session.flush()
            self._open_incident_ids[client.client_id] = incident.id
            escalated = True
        else:
            prev_peak = incident.peak_level
            incident.peak_level = max(incident.peak_level, int(verdict.level))
            incident.peak_score = max(incident.peak_score, trust)
            if fsm_diff:
                incident.fsm_diff = fsm_diff
            escalated = incident.peak_level > prev_peak

        summary = {
            "client_id": client.client_id,
            "username": client.username,
            "peak_level": incident.peak_level,
            "peak_score": incident.peak_score,
            "reason": verdict.reason,
            "fsm_diff": incident.fsm_diff or [],
            "window_stats": {},
        }
        redacted = redact_incident_summary(summary, self.config.redaction_secret,
                                            self.config.redaction.secret_topic_patterns)
        incident.summary = redacted
        # Only (re)draft the report when the incident first opens or its peak
        # level escalates — not on every window it stays open. This keeps the
        # narrative current for what an operator cares about (the worst the
        # client got) while avoiding a blocking LLM call on every window of a
        # long-running incident (spec §7.1: report must never gate scoring).
        if escalated:
            incident.report_md = generate_report(
                redacted, self.config.nvidia_api_key, self.config.llm.model,
                self.config.llm.timeout_s, self.config.llm.enabled,
                base_url=self.config.nvidia_base_url,
            )
        session.commit()
        return incident
