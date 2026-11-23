from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    CheckConstraint,
    JSON,
    Index
)
from sqlalchemy.sql import func

from coordinator.database import Base


# ==========================================
# OBJECT METADATA
# ==========================================

class ObjectMetadata(Base):
    __tablename__ = "objects"

    object_id = Column(
        String,
        primary_key=True
    )

    object_name = Column(
        String,
        nullable=False
    )

    size = Column(
        Integer,
        nullable=False
    )

    content_type = Column(
        String,
        nullable=False,
        default="application/octet-stream"
    )

    checksum = Column(
        String(64),
        nullable=False
    )

    chunk_count = Column(
        Integer,
        nullable=False
    )

    custom_metadata = Column(
        JSON,
        nullable=False,
        default=dict
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "size >= 0",
            name="ck_object_size_nonnegative"
        ),
        CheckConstraint(
            "chunk_count >= 0",
            name="ck_object_chunk_count_nonnegative"
        ),
    )


# ==========================================
# CHUNK METADATA
# ==========================================

class Chunk(Base):
    __tablename__ = "chunks"

    chunk_id = Column(
        String,
        primary_key=True
    )

    object_id = Column(
        String,
        ForeignKey("objects.object_id"),
        nullable=False,
        index=True
    )

    sequence_number = Column(
        Integer,
        nullable=False
    )

    size = Column(
        Integer,
        nullable=False
    )

    checksum = Column(
        String(64),
        nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "object_id",
            "sequence_number",
            name="uq_object_chunk_sequence"
        ),
        CheckConstraint(
            "sequence_number >= 1",
            name="ck_chunk_sequence_positive"
        ),
        CheckConstraint(
            "size > 0",
            name="ck_chunk_size_positive"
        ),
        Index(
            "ix_chunk_object_sequence",
            "object_id",
            "sequence_number"
        ),
    )


# ==========================================
# STORAGE WORKER
# ==========================================

class Worker(Base):
    __tablename__ = "workers"

    worker_id = Column(
        String,
        primary_key=True
    )

    base_url = Column(
        String,
        nullable=False,
        unique=True
    )

    status = Column(
        String,
        nullable=False,
        default="UNKNOWN"
    )

    last_heartbeat = Column(
        DateTime(timezone=True),
        nullable=True
    )


# ==========================================
# CHUNK REPLICA
# ==========================================

class ChunkReplica(Base):
    __tablename__ = "chunk_replicas"

    id = Column(
        Integer,
        primary_key=True,
        index=True
    )

    chunk_id = Column(
        String,
        ForeignKey("chunks.chunk_id"),
        nullable=False,
        index=True
    )

    worker_id = Column(
        String,
        ForeignKey("workers.worker_id"),
        nullable=False,
        index=True
    )

    status = Column(
        String,
        nullable=False,
        default="ACTIVE"
    )

    checksum = Column(
        String(64),
        nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "chunk_id",
            "worker_id",
            name="uq_chunk_worker"
        ),
        Index(
            "ix_replica_chunk_status",
            "chunk_id",
            "status"
        ),
    )


# ==========================================
# PENDING PHYSICAL DELETES
# ==========================================

class PendingDelete(Base):
    """
    A physical chunk copy that still has to be removed from a worker
    (for example because the worker was down when the object was deleted).
    The maintenance loop retries these once the worker is healthy again.
    """
    __tablename__ = "pending_deletes"

    id = Column(
        Integer,
        primary_key=True
    )

    worker_id = Column(
        String,
        nullable=False,
        index=True
    )

    chunk_id = Column(
        String,
        nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "worker_id",
            "chunk_id",
            name="uq_pending_delete"
        ),
    )
