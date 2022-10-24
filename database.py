import pandas as pd
from sqlalchemy import (BIGINT, Column, Date, DateTime, Float, ForeignKey,
                        Integer, Sequence, String, create_engine, dialects)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql import func

Base = declarative_base()
# TODO: set in env variable
uri = "postgresql+psycopg2://localhost:5432/test"
engine = create_engine(uri)
session = Session(bind=engine)

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


if __name__ == "__main__":
    Base.metadata.create_all(engine)
