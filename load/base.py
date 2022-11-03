from loguru import logger
from sqlalchemy import (Boolean, Column, DateTime, Integer, String,
                        create_engine)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

Base = declarative_base()

class Entity(Base):
    __tablename__ = "__ud_entities"

    id = Column(
        Integer(),
        nullable=False,
        primary_key=True,
        autoincrement=True,
    )
    source_id = Column(
        String(),
        nullable=False,
    )
    entity = Column(
        String(),
        nullable=False,
    )
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    processed = Column(Boolean(), default=False)
    data = Column(JSONB)


class DataWarehouse:
    def __init__(self, uri):
        self.engine = create_engine(uri)
        Base.metadata.create_all(self.engine)
        self.session = Session(bind=self.engine)

    def load(self, source_id, entity, items):
        logger.debug(f"Saving {len(items)} {entity} from {source_id}")
        for item in items:
            schema = Entity(source_id=source_id, entity=entity, data=item)
            self.session.add(schema)
        self.session.commit()
