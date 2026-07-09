"""SQLAlchemy models + session factory (spec §7.2). Postgres in production
(JSONB columns), SQLite acceptable for local tests (generic JSON variant).
"""
from __future__ import annotations

import datetime

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text,
    create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.types import JSON

Base = declarative_base()

# Generic JSON on SQLite (tests), JSONB on Postgres (production).
JSONType = JSON().with_variant(JSONB(), "postgresql")


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


class Client(Base):
    __tablename__ = "clients"
    id = Column(Integer, primary_key=True)
    client_id = Column(String(255), unique=True, nullable=False, index=True)
    username = Column(String(255), nullable=True)
    first_seen = Column(DateTime, default=utcnow)
    last_seen = Column(DateTime, default=utcnow, onupdate=utcnow)
    cohort = Column(String(255), nullable=True)
    learning_complete = Column(Boolean, default=False)

    sessions = relationship("Session", back_populates="client")
    feature_windows = relationship("FeatureWindowRow", back_populates="client")


class Session(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False, index=True)
    connect_ts = Column(DateTime, nullable=True)
    disconnect_ts = Column(DateTime, nullable=True)
    ip_masked = Column(String(64), nullable=True)
    protocol = Column(String(32), nullable=True)
    keepalive = Column(Integer, nullable=True)
    clean_session = Column(Boolean, nullable=True)

    client = relationship("Client", back_populates="sessions")


class FeatureWindowRow(Base):
    __tablename__ = "feature_windows"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False, index=True)
    window_start = Column(DateTime, nullable=False)
    window_len_s = Column(Float, nullable=False)
    features = Column(JSONType, nullable=False)
    fsm_violation = Column(Float, nullable=True)
    drift = Column(Float, nullable=True)
    fleet = Column(Float, nullable=True)
    trust = Column(Float, nullable=True)

    client = relationship("Client", back_populates="feature_windows")


class VerdictHistory(Base):
    __tablename__ = "verdict_history"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False, index=True)
    ts = Column(DateTime, default=utcnow)
    level = Column(Integer, nullable=False)
    score = Column(Float, nullable=False)
    reason = Column(Text, nullable=True)


class Incident(Base):
    __tablename__ = "incidents"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=True, index=True)
    fleet = Column(Boolean, default=False)
    opened_ts = Column(DateTime, default=utcnow)
    closed_ts = Column(DateTime, nullable=True)
    peak_level = Column(Integer, nullable=False)
    peak_score = Column(Float, nullable=False)
    fsm_diff = Column(JSONType, nullable=True)
    summary = Column(JSONType, nullable=True)
    report_md = Column(Text, nullable=True)


class Fingerprint(Base):
    __tablename__ = "fingerprints"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False, index=True)
    created_ts = Column(DateTime, default=utcnow)
    doc = Column(JSONType, nullable=False)
    stability = Column(Float, nullable=True)


class ModelVersion(Base):
    __tablename__ = "model_versions"
    id = Column(Integer, primary_key=True)
    cohort = Column(String(255), nullable=False, index=True)
    trained_at = Column(DateTime, default=utcnow)
    n_samples = Column(Integer, nullable=False)
    metrics = Column(JSONType, nullable=True)
    path = Column(String(512), nullable=False)


def get_engine(database_url: str):
    return create_engine(database_url, future=True)


def get_sessionmaker(engine):
    return sessionmaker(bind=engine, future=True, expire_on_commit=False)


def init_db(engine):
    """Creates all tables if they don't exist yet. In a real deployment this
    is superseded by `alembic upgrade head` (see migrations/); kept here so
    tests and `make eval` can stand up a throwaway SQLite DB with one call."""
    Base.metadata.create_all(engine)


def get_or_create_client(session, client_id: str, username: str | None = None) -> Client:
    client = session.query(Client).filter_by(client_id=client_id).first()
    if client is None:
        client = Client(client_id=client_id, username=username)
        session.add(client)
        session.flush()
    elif username and not client.username:
        client.username = username
    return client
