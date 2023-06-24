from datetime import datetime, timedelta

from decouple import config as env
from loguru import logger
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import object_session, relationship, scoped_session, sessionmaker
from sqlalchemy.orm.attributes import flag_modified

Base = declarative_base()
uri = env("DATABASE_URI")

# https://docs.sqlalchemy.org/en/13/core/pooling.html#dealing-with-disconnects
engine = create_engine(uri, pool_pre_ping=True, pool_size=30)
logger.debug(f"Connecting to database: {engine.url.host}")

session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)


class STATUS:
    ERROR = "error"
    DONE = "done"
    QUEUED = "queued"
    RUNNING = "running"
    LOST = "lost"


class Source(Base):
    __tablename__ = "sources"
    id = Column(
        Integer(),
        primary_key=True,
        autoincrement=True,
    )
    name = Column(  # linked to source config name
        String(),
        nullable=False,
    )
    config = Column(JSONB)
    client_id = Column(
        Integer(),
        ForeignKey("clients.id"),
        nullable=True,  # public api don't need client_id
    )
    client = relationship("Client", back_populates="sources")
    pipelines = relationship("Pipeline", back_populates="source")

    state = Column(JSONB, nullable=True)

    def save_state(self, id, state):
        _session = object_session(self)
        if self.state is None:
            self.state = {}
        self.state[id] = state
        flag_modified(self, "state")
        _session.add(self)
        _session.commit()

    def load_state(self, id):
        if self.state is None:
            return {}
        return self.state.get(id, {})


class Target(Base):
    __tablename__ = "targets"
    id = Column(
        Integer(),
        primary_key=True,
        autoincrement=True,
    )
    type = Column(
        String(),
        nullable=False,
    )
    uri = Column(
        String(),
        nullable=False,
    )
    client_id = Column(
        Integer(),
        ForeignKey("clients.id"),
        nullable=False,
    )
    client = relationship("Client", back_populates="targets")
    pipelines = relationship("Pipeline", back_populates="target")


class Client(Base):
    __tablename__ = "clients"

    id = Column(
        Integer(),
        primary_key=True,
        autoincrement=True,
    )
    name = Column(
        String(),
        nullable=False,
    )
    sources = relationship("Source", back_populates="client")
    targets = relationship("Target", back_populates="client")


class Task(Base):
    __tablename__ = "tasks"

    id = Column(
        Integer(),
        primary_key=True,
        autoincrement=True,
    )
    pipeline_id = Column(
        Integer(),
        ForeignKey("pipelines.id"),
        nullable=False,
    )
    pipeline = relationship(
        "Pipeline",
        back_populates="tasks",
    )

    type = Column(
        String(),
        nullable=False,
    )

    status = Column(
        String(),
        nullable=True,
    )

    error_log = Column(
        String(),
        nullable=True,
    )

    time_start = Column(
        DateTime(),
        nullable=True,
    )

    time_end = Column(
        DateTime(),
        nullable=True,
    )


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(
        Integer(),
        primary_key=True,
        autoincrement=True,
    )
    source_id = Column(
        Integer(),
        ForeignKey("sources.id"),
        nullable=False,
    )
    source = relationship(
        "Source",
        back_populates="pipelines",
    )

    target_id = Column(
        Integer(),
        ForeignKey("targets.id"),
        nullable=False,
    )

    target = relationship(
        "Target",
        back_populates="pipelines",
    )

    active = Column(
        Boolean(),
        default=True,
    )

    extract_status = Column(
        String(),
        nullable=True,
    )
    extract_started_at = Column(
        DateTime(),
        nullable=True,
    )

    transform_status = Column(
        String(),
        nullable=True,
    )
    transform_started_at = Column(
        DateTime(),
        nullable=True,
    )

    transfer_status = Column(
        String(),
        nullable=True,
    )
    transfer_started_at = Column(
        DateTime(),
        nullable=True,
    )

    interval_minutes = Column(
        Integer(),
        nullable=True,
    )

    tasks = relationship("Task", back_populates="pipeline")

    @property
    def stage_uri(self):
        """
        Change 'postgresql+psycopg2://localhost:5432/{database_name}'
        to 'postgresql+psycopg2://localhost:5432/stage_{id}'
        """
        # Recreate URI and change database name
        dialects = engine.url.get_dialect().name
        username = engine.url.username or ""
        password = engine.url.password or ""
        host = engine.url.host
        port = engine.url.port or "5432"
        database = f"source_{self.source_id}"
        return f"{dialects}://{username}:{password}@{host}:{port}/{database}"

    def should_start(self):
        # TODO: check if last task is done
        # Default to 1 hour
        default_interval = 60
        tzinfo = self.extract_started_at.tzinfo
        return self.extract_started_at + timedelta(
            minutes=self.interval_minutes or default_interval
        ) < datetime.now().replace(tzinfo=tzinfo)


if __name__ == "__main__":
    Base.metadata.create_all(engine)
