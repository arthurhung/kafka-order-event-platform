"""SQLAlchemy engine and session factories."""

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from streaming_platform.config import Settings


def create_database_engine(settings: Settings) -> Engine:
    """Create a PostgreSQL engine with connection health checks enabled."""
    return create_engine(settings.database_url, pool_pre_ping=True)


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create an explicit-transaction session factory."""
    return sessionmaker(bind=engine, expire_on_commit=False)
