from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    ForeignKey,
    UniqueConstraint
)
from sqlalchemy.sql import func

from coordinator.database import Base


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

    status = Column(
        String,
        nullable=False,
        default="ACTIVE"
    )

    checksum = Column(String(64), nullable=False)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )