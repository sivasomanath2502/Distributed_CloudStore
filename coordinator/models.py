from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    JSON
)
from sqlalchemy.sql import func

from coordinator.database import Base


# ==========================================
# OLD MODELS - RETAINED FOR PROTOTYPE
# ==========================================

class FileMetadata(Base):
    __tablename__ = "files"

    file_id = Column(String, primary_key=True)
    filename = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    storage_node = Column(String, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )


class FileReplica(Base):
    __tablename__ = "file_replicas"

    __table_args__ = (
        UniqueConstraint(
            "file_id",
            "node_id",
            name="uq_file_node"
        ),
    )

    id = Column(Integer, primary_key=True, index=True)

    file_id = Column(
        String,
        ForeignKey("files.file_id"),
        nullable=False
    )

    node_id = Column(String, nullable=False)
    status = Column(String, nullable=False, default="ACTIVE")
    checksum = Column(String(64), nullable=False)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )


# ==========================================
# NEW MODEL 1 - OBJECT METADATA
# ==========================================

class ObjectMetadata(Base):
    __tablename__ = "objects"

    object_id = Column(String, primary_key=True)

    object_name = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    content_type = Column(
        String,
        nullable=False,
        default="application/octet-stream"
    )

    checksum = Column(String(64), nullable=False)
    chunk_count = Column(Integer, nullable=False)

    custom_metadata = Column(
        JSON,
        nullable=False,
        default=dict
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )

    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )


# ==========================================
# NEW MODEL 2 - CHUNK
# ==========================================

class Chunk(Base):
    __tablename__ = "chunks"

    __table_args__ = (
        UniqueConstraint(
            "object_id",
            "sequence_number",
            name="uq_object_chunk_sequence"
        ),
    )

    chunk_id = Column(String, primary_key=True)

    object_id = Column(
        String,
        ForeignKey("objects.object_id"),
        nullable=False,
        index=True
    )

    sequence_number = Column(Integer, nullable=False)
    size = Column(Integer, nullable=False)
    checksum = Column(String(64), nullable=False)


# ==========================================
# NEW MODEL 3 - WORKER
# ==========================================

class Worker(Base):
    __tablename__ = "workers"

    worker_id = Column(String, primary_key=True)

    base_url = Column(String, nullable=False, unique=True)

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
# NEW MODEL 4 - CHUNK REPLICA
# ==========================================

class ChunkReplica(Base):
    __tablename__ = "chunk_replicas"

    __table_args__ = (
        UniqueConstraint(
            "chunk_id",
            "worker_id",
            name="uq_chunk_worker"
        ),
    )

    id = Column(Integer, primary_key=True, index=True)

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

    checksum = Column(String(64), nullable=False)