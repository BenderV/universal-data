import os
from datetime import datetime

from decouple import config as env
from loguru import logger
from sqlalchemy import (BIGINT, Boolean, Column, Date, DateTime, Float,
                        ForeignKey, Integer, Sequence, String, create_engine,
                        dialects, event)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, object_session, relationship
from sqlalchemy.orm.attributes import flag_modified

Base = declarative_base()
uri = env('DATABASE_URI')

# https://docs.sqlalchemy.org/en/13/core/pooling.html#dealing-with-disconnects
engine = create_engine(uri, pool_pre_ping=True)
logger.debug(f"Connecting to database: {engine.url.host}")

def create_session():
    return Session(bind=engine)

session = create_session()

class STATUS:
    ERROR = 'error'
    DONE = 'done'
    QUEUED = 'queued'
    RUNNING = 'running'

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
        if self.state is None:
            self.state = {}
        self.state[id] = state
        flag_modified(self, "state")
        session.add(self)
        session.commit()

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


@event.listens_for(Task.status, 'set')
def update_status(target, value, old_value, initiator):
    _session = object_session(target) or create_session()
    pipeline = _session.query(Pipeline).get(target.pipeline_id)
    if target.type == 'extract':
        pipeline.extract_status = value
        if value == STATUS.RUNNING:
            pipeline.extract_started_at = datetime.now()
    elif target.type == 'transform':
        pipeline.transform_status = value
        if value == STATUS.RUNNING:
            pipeline.transform_started_at = datetime.now()
    else:
        raise Exception('task type not recognized')
    _session.commit()



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

    tasks = relationship("Task", back_populates="pipeline")

if __name__ == "__main__":
    Base.metadata.create_all(engine)
