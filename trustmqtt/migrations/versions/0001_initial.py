"""Initial schema (spec docs/SPEC.md §7.2)

Revision ID: 0001
Revises:
Create Date: 2026-07-07
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "clients",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id", sa.String(255), nullable=False, unique=True),
        sa.Column("username", sa.String(255), nullable=True),
        sa.Column("first_seen", sa.DateTime(), nullable=True),
        sa.Column("last_seen", sa.DateTime(), nullable=True),
        sa.Column("cohort", sa.String(255), nullable=True),
        sa.Column("learning_complete", sa.Boolean(), nullable=True),
    )
    op.create_index("ix_clients_client_id", "clients", ["client_id"])

    op.create_table(
        "sessions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id", sa.Integer(), sa.ForeignKey("clients.id"), nullable=False),
        sa.Column("connect_ts", sa.DateTime(), nullable=True),
        sa.Column("disconnect_ts", sa.DateTime(), nullable=True),
        sa.Column("ip_masked", sa.String(64), nullable=True),
        sa.Column("protocol", sa.String(32), nullable=True),
        sa.Column("keepalive", sa.Integer(), nullable=True),
        sa.Column("clean_session", sa.Boolean(), nullable=True),
    )
    op.create_index("ix_sessions_client_id", "sessions", ["client_id"])

    op.create_table(
        "feature_windows",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id", sa.Integer(), sa.ForeignKey("clients.id"), nullable=False),
        sa.Column("window_start", sa.DateTime(), nullable=False),
        sa.Column("window_len_s", sa.Float(), nullable=False),
        sa.Column("features", postgresql.JSONB(), nullable=False),
        sa.Column("fsm_violation", sa.Float(), nullable=True),
        sa.Column("drift", sa.Float(), nullable=True),
        sa.Column("fleet", sa.Float(), nullable=True),
        sa.Column("trust", sa.Float(), nullable=True),
    )
    op.create_index("ix_feature_windows_client_id", "feature_windows", ["client_id"])

    op.create_table(
        "verdict_history",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id", sa.Integer(), sa.ForeignKey("clients.id"), nullable=False),
        sa.Column("ts", sa.DateTime(), nullable=True),
        sa.Column("level", sa.Integer(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
    )
    op.create_index("ix_verdict_history_client_id", "verdict_history", ["client_id"])

    op.create_table(
        "incidents",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id", sa.Integer(), sa.ForeignKey("clients.id"), nullable=True),
        sa.Column("fleet", sa.Boolean(), nullable=True),
        sa.Column("opened_ts", sa.DateTime(), nullable=True),
        sa.Column("closed_ts", sa.DateTime(), nullable=True),
        sa.Column("peak_level", sa.Integer(), nullable=False),
        sa.Column("peak_score", sa.Float(), nullable=False),
        sa.Column("fsm_diff", postgresql.JSONB(), nullable=True),
        sa.Column("summary", postgresql.JSONB(), nullable=True),
        sa.Column("report_md", sa.Text(), nullable=True),
    )
    op.create_index("ix_incidents_client_id", "incidents", ["client_id"])

    op.create_table(
        "fingerprints",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id", sa.Integer(), sa.ForeignKey("clients.id"), nullable=False),
        sa.Column("created_ts", sa.DateTime(), nullable=True),
        sa.Column("doc", postgresql.JSONB(), nullable=False),
        sa.Column("stability", sa.Float(), nullable=True),
    )
    op.create_index("ix_fingerprints_client_id", "fingerprints", ["client_id"])

    op.create_table(
        "model_versions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("cohort", sa.String(255), nullable=False),
        sa.Column("trained_at", sa.DateTime(), nullable=True),
        sa.Column("n_samples", sa.Integer(), nullable=False),
        sa.Column("metrics", postgresql.JSONB(), nullable=True),
        sa.Column("path", sa.String(512), nullable=False),
    )
    op.create_index("ix_model_versions_cohort", "model_versions", ["cohort"])


def downgrade():
    op.drop_table("model_versions")
    op.drop_table("fingerprints")
    op.drop_table("incidents")
    op.drop_table("verdict_history")
    op.drop_table("feature_windows")
    op.drop_table("sessions")
    op.drop_table("clients")
