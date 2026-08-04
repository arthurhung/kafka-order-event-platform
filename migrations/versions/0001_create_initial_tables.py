"""Create the initial platform tables.

Revision ID: 0001
Revises: None
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create business, idempotency, and aggregate tables."""
    op.create_table(
        "valid_orders",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("order_id", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=50), nullable=False),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("product_id", sa.String(length=64), nullable=True),
        sa.Column("quantity", sa.Integer(), nullable=True),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("currency", sa.String(length=3), nullable=True),
        sa.Column("channel", sa.String(length=20), nullable=True),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kafka_topic", sa.String(length=255), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index("idx_valid_orders_order_id", "valid_orders", ["order_id"])
    op.create_index("idx_valid_orders_event_time", "valid_orders", ["event_time"])

    op.create_table(
        "processed_events",
        sa.Column("consumer_group", sa.String(length=100), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("partition_id", sa.Integer(), nullable=False),
        sa.Column("offset_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "processed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("consumer_group", "event_id"),
    )

    op.create_table(
        "log_metrics_minute",
        sa.Column("metric_minute", sa.DateTime(timezone=True), nullable=False),
        sa.Column("service", sa.String(length=100), nullable=False),
        sa.Column("endpoint", sa.String(length=255), nullable=False),
        sa.Column("request_count", sa.BigInteger(), nullable=False),
        sa.Column("success_count", sa.BigInteger(), nullable=False),
        sa.Column("client_error_count", sa.BigInteger(), nullable=False),
        sa.Column("server_error_count", sa.BigInteger(), nullable=False),
        sa.Column("response_time_sum_ms", sa.BigInteger(), nullable=False),
        sa.Column("max_response_time_ms", sa.Integer(), nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("metric_minute", "service", "endpoint"),
    )


def downgrade() -> None:
    """Drop the initial platform tables."""
    op.drop_table("log_metrics_minute")
    op.drop_table("processed_events")
    op.drop_index("idx_valid_orders_event_time", table_name="valid_orders")
    op.drop_index("idx_valid_orders_order_id", table_name="valid_orders")
    op.drop_table("valid_orders")
