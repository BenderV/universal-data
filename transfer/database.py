from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class Transfer(Base):
    __tablename__ = "__ud_transfer"

    id = Column(
        Integer(),
        nullable=False,
        primary_key=True,
        autoincrement=True,
    )
    table_name = Column(
        String(),
        nullable=False,
        primary_key=True,
    )
    hash = Column(
        String(),
        nullable=False,
        primary_key=True,
    )
    target_id = Column(
        Integer(),
        nullable=False,
        primary_key=True,
    )
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())


class Stage:
    def __init__(self, uri):
        self.engine = create_engine(uri)
        Base.metadata.create_all(self.engine)
