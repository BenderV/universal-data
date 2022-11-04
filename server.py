import os
import sys
import threading
import time
from datetime import datetime

import schedule
import yaml
from loguru import logger

from database import STATUS, Pipeline, Task, session
from extract import scraper
from run_transform import transform

DEBUG = os.environ.get('DEBUG', False)

logger.debug(f"DEBUG Model: {DEBUG}")

if DEBUG:
    logger.add(sys.stdout, format="{time} {level} {message}", filter="my_module", level="INFO", backtrace=True, diagnose=True)
else:
    logger.add(sys.stdout, serialize=True, filter="my_module")

def record_task(task_type):
    def decorator(func):
        def wrapper(pipeline):
            from database import create_session
            session = create_session()
            
            task = Task(
                pipeline_id=pipeline.id,
                type=task_type,
                status=STATUS.RUNNING,
                time_start=datetime.now()
            )
            session.add(task)
            session.commit()
            
            value = None
            try:
                value = func(pipeline=pipeline)
                task.status = STATUS.DONE
            except (Exception, KeyboardInterrupt) as e:
                logger.exception("Error in task")
                task.status = STATUS.ERROR
                task.error_log = type(e).__name__ + '\n' + str(e)
            finally:
                task.time_end = datetime.now()
                session.commit()
            return value
        return wrapper
    return decorator


@record_task(task_type='extract')
def run_extract_task(pipeline):
    source_name = pipeline.source.name
    with open(f'sources/{source_name}.yml') as f:
        source_config = yaml.safe_load(f)

    logger.info("Run extract")
    scraper.runner(source_config, pipeline.target.uri, debug=DEBUG, memory=pipeline.source, params=pipeline.source.config)
    # Start transform status to queued
    pipeline.transform_status = STATUS.QUEUED
    session.commit()

@record_task(task_type='transform')
def run_transform_task(pipeline):
    transform(pipeline.target.uri)


def run_transforms():
    # We look in the queue for a pipeline to run
    pipelines = session.query(Pipeline).filter_by(active=True, transform_status=STATUS.QUEUED).all()
    if pipelines:
        logger.info(f"{len(pipelines)} tranform to run")

    for pipeline in pipelines:
        threading.Thread(target=run_transform_task, args=(pipeline,)).start()

def run_extracts():
    # We look in the queue for a pipeline to run
    pipelines = session.query(Pipeline).filter_by(active=True, extract_status=STATUS.QUEUED).all()
    if pipelines:
        logger.info(f"extract pipeline to run: {len(pipelines)}")
    for pipeline in pipelines:
        threading.Thread(target=run_extract_task, args=(pipeline,)).start()
  
def add_pipelines_to_queue():
    pipelines = session.query(Pipeline).filter_by(active=True).filter(
        Pipeline.extract_status.not_in([STATUS.QUEUED, STATUS.RUNNING, STATUS.ERROR])
    ).all()
    for pipeline in pipelines:
        if pipeline.should_start():
            pipeline.extract_status = STATUS.QUEUED
    session.commit()


if __name__ == "__main__":
    while True:
        logger.info("Pulse")
        add_pipelines_to_queue()
        run_extracts()
        run_transforms()
        schedule.run_pending()
        time.sleep(10)
