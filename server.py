import sys
import threading
import time
from datetime import datetime

import schedule
import sqlalchemy as sa
import yaml
from decouple import config as env
from loguru import logger
from sqlalchemy_utils import database_exists

from database import STATUS, Pipeline, Session, Task
from extract import scraper
from load.base import DataWarehouse
from transfer.database import Stage
from transfer.transfer import transfer
from transform.normalize import EntityNormalization

DEBUG = env("DEBUG", cast=bool, default=False)
PULSE_INTERVAL = env("PULSE_INTERVAL", cast=int, default=10)

logger.debug(f"DEBUG Model: {DEBUG}")

active_threads = {}
session = Session()

if DEBUG:
    logger.add(
        sys.stdout,
        format="{time} {level} {message}",
        filter="my_module",
        level="INFO",
        backtrace=True,
        diagnose=True,
    )
else:
    logger.add(sys.stdout, serialize=True, filter="my_module")


def update_pipeline_status(pipeline, task):
    if task.type == "extract":
        pipeline.extract_status = task.status
        if task.status == STATUS.RUNNING:
            pipeline.extract_started_at = datetime.now()
    elif task.type == "transform":
        pipeline.transform_status = task.status
        if task.status == STATUS.RUNNING:
            pipeline.transform_started_at = datetime.now()
    elif task.type == "transfer":
        pipeline.transfer_status = task.status
        if task.status == STATUS.RUNNING:
            pipeline.transfer_started_at = datetime.now()
    else:
        raise Exception("task type not recognized")


def record_task(task_type):
    def decorator(func):
        def wrapper(pipeline):
            task = Task(
                pipeline_id=pipeline.id,
                type=task_type,
                status=STATUS.RUNNING,
                time_start=datetime.now(),
            )
            session.add(task)
            update_pipeline_status(pipeline, task)
            session.commit()

            def run_task_in_thread(task, pipeline):
                _session = Session()
                task = _session.merge(task, load=False)
                pipeline = _session.merge(pipeline, load=False)
                value = None
                try:
                    value = func(pipeline)
                    task.status = STATUS.DONE
                except (Exception, KeyboardInterrupt) as e:
                    logger.exception("Error in task")
                    task.status = STATUS.ERROR
                    task.error_log = type(e).__name__ + "\n" + str(e)
                finally:
                    task.time_end = datetime.now()
                    update_pipeline_status(pipeline, task)
                    _session.commit()
                    Session.remove()
                return value

            thread = threading.Thread(
                target=run_task_in_thread,
                args=(
                    task,
                    pipeline,
                ),
            )
            thread.start()
            active_threads[task.id] = thread
            return thread

        return wrapper

    return decorator


@record_task(task_type="extract")
def run_extract_task(pipeline):
    _session = Session()

    source_name = pipeline.source.name
    with open(f"./extract/sources/{source_name}.yml") as f:
        source_config = yaml.safe_load(f)

    # Create stage_uri database if it doesn't exist
    stage_engine = sa.create_engine(pipeline.stage_uri)
    if not database_exists(stage_engine.url):
        logger.info(f"Create stage database: {stage_engine.url.database}")
        # Create database
        session.connection().connection.set_isolation_level(0)
        session.execute(f"CREATE DATABASE {stage_engine.url.database}")
        session.connection().connection.set_isolation_level(1)

    logger.info("Run extract")
    scraper.runner(
        source_config,
        pipeline.stage_uri,
        debug=DEBUG,
        memory=pipeline.source,
        params=pipeline.source.config,
    )
    # Start transform status to queued
    pipeline.transform_status = STATUS.QUEUED
    _session.commit()


@record_task(task_type="transform")
def run_transform_task(pipeline):
    _session = Session()

    db = DataWarehouse(pipeline.stage_uri)

    entities = db._get_active_entities()
    for entity in entities:
        print("Normalize entity:", entity)
        en = EntityNormalization(db, entity["source_id"], entity["entity"])
        en.normalize()

    # Start transfer status to queued
    pipeline.transfer_status = STATUS.QUEUED
    _session.commit()


@record_task(task_type="transfer")
def run_transfer_task(pipeline):
    """
    Find all the table on the stage and transfer them
    """
    logger.info("Create tables __ud_transfer")
    Stage(pipeline.stage_uri)

    engine = sa.create_engine(pipeline.stage_uri)
    metadata = sa.MetaData(bind=engine)
    metadata.reflect()
    tables = metadata.tables

    for table in tables:
        if table.startswith("__ud"):
            continue
        print("Transfer table:", table)
        transfer(pipeline.stage_uri, pipeline.target.uri, table)


def run_transforms():
    # We look in the queue for a pipeline to run
    pipelines = (
        session.query(Pipeline)
        .filter_by(active=True, transform_status=STATUS.QUEUED)
        .all()
    )
    if pipelines:
        logger.info(f"{len(pipelines)} tranform to run")

    for pipeline in pipelines:
        run_transform_task(pipeline)


def run_extracts():
    # We look in the queue for a pipeline to run
    pipelines = (
        session.query(Pipeline)
        .filter_by(active=True, extract_status=STATUS.QUEUED)
        .all()
    )
    if pipelines:
        logger.info(f"extract pipeline to run: {len(pipelines)}")
    for pipeline in pipelines:
        run_extract_task(pipeline)


def run_transfers():
    # We look in the queue for a pipeline to run
    pipelines = (
        session.query(Pipeline)
        .filter_by(active=True, transfer_status=STATUS.QUEUED)
        .all()
    )
    if pipelines:
        logger.info(f"{len(pipelines)} transfer to run")
    for pipeline in pipelines:
        run_transfer_task(pipeline)


def add_pipelines_to_queue():
    pipelines = (
        session.query(Pipeline)
        .filter_by(active=True)
        .filter(Pipeline.extract_status.not_in([STATUS.QUEUED, STATUS.RUNNING]))
        .all()
    )
    for pipeline in pipelines:
        if pipeline.should_start():
            pipeline.extract_status = STATUS.QUEUED
    session.commit()


def detect_lost_threads():
    running_tasks = session.query(Task).filter_by(status=STATUS.RUNNING).all()
    for task in running_tasks:
        if task.id in active_threads and active_threads[task.id].is_alive():
            continue

        task.status = STATUS.LOST
        if task.id in active_threads:
            task.error_log = "Thread is not alive"
            del active_threads[task.id]
        else:
            task.error_log = "Thread has been lost"

        task.time_end = datetime.now()
        update_pipeline_status(task.pipeline, task)
        session.commit()

    running_task_ids = [task.id for task in running_tasks]
    for task_id in list(active_threads.keys()):
        if task_id not in running_task_ids:
            del active_threads[task_id]


if __name__ == "__main__":
    while True:
        logger.info("Pulse")
        detect_lost_threads()
        add_pipelines_to_queue()
        run_extracts()
        run_transforms()
        run_transfers()
        schedule.run_pending()
        time.sleep(PULSE_INTERVAL)
