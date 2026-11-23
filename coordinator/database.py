import os

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base

# The database location is configurable so that Docker can mount a
# *directory* (not a single file) for controller persistence.
DB_PATH = os.getenv("DB_PATH", "coordinator.db")

_db_dir = os.path.dirname(os.path.abspath(DB_PATH))
os.makedirs(_db_dir, exist_ok=True)

DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")

engine = create_engine(
    DATABASE_URL,
    connect_args={
        "check_same_thread": False,
        # Wait for a competing writer instead of failing immediately
        # with "database is locked".
        "timeout": 30
    }
)


@event.listens_for(engine, "connect")
def _configure_sqlite(dbapi_connection, connection_record):
    """
    WAL lets readers proceed while a writer is active; busy_timeout makes
    concurrent writers (heartbeats, reconciliation, uploads) queue up.
    """
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=30000")
    cursor.close()


SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()


def get_db():
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()
