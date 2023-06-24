import hashlib

import sqlalchemy as sa
import yaml
from loguru import logger
from sqlalchemy import Boolean, Column, DateTime, Integer, String, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

Base = declarative_base()


def read_source_name(source_name):
    with open(f"./extract/sources/{source_name}.yml") as f:
        source_config = yaml.safe_load(f)
    return source_config


def mini_hash(params):
    """Create a hash from a list of parameters"""
    hash = hashlib.sha1(str(params).encode("UTF-8")).hexdigest()
    return hash[:10]


class Entity(Base):
    __tablename__ = "__ud_entities"

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
    key = Column("__key", String(), nullable=True, primary_key=True)

    # unique constraint on entity and key
    __table_args__ = (sa.UniqueConstraint("entity", "__key", name="entity_key_unique"),)


class DataWarehouse:
    def __init__(self, uri):
        self.engine = create_engine(uri, pool_pre_ping=True)
        Base.metadata.create_all(self.engine)
        self.session = Session(bind=self.engine)

    def _get_entity_keys(self, source_id, entity):
        source_config = read_source_name(source_id)
        for source_entity, keys in source_config["entities"].items():
            if source_entity == entity:
                return keys
        raise ValueError(f"Keys not found for entity {entity}")

    def _get_active_entities(self):
        with self.engine.begin() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT source_id, entity
                FROM __ud_entities
            """
            ).all()
            return [dict(r) for r in rows]

    def load(self, source_id, entity, items):
        logger.debug(f"Saving {len(items)} {entity} from {source_id}")
        entity_keys = self._get_entity_keys(source_id, entity)

        for item in items:
            if len(entity_keys) > 1:
                # If multiple key, create a hash of the keys
                key = mini_hash([entity] + [item[k] for k in entity_keys])
            else:
                entity_key = entity_keys[0]
                key = mini_hash([entity] + [item[entity_key]])

            # Check if the entity already exists
            instance = (
                self.session.query(Entity).filter_by(entity=entity, key=key).first()
            )
            if instance:
                logger.debug(f"Entity {key} already exists")
                # If data is the same, skip
                if instance.data == item:
                    continue
                # If data is different, update
                else:
                    instance.data = item
                    instance.processed = False
            else:
                instance = Entity(
                    source_id=source_id,
                    entity=entity,
                    data=item,
                    key=key,
                    processed=False,
                )
                self.session.add(instance)
        self.session.commit()
