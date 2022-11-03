import os
import threading
import time
from datetime import datetime

import schedule
import yaml

from database import STATUS, Pipeline, Task, session
from extract import scraper
from run_transform import transform

DEBUG = os.environ.get('DEBUG', False)

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

    print("Run extract")
    scraper.runner(source_config, pipeline.target.uri, debug=DEBUG, memory=pipeline.source, params=pipeline.source.config)
    run_transform_task(pipeline)

@record_task(task_type='transform')
def run_transform_task(pipeline):
    transform(pipeline.target.uri)


def run_transforms():
    # We look in the queue for a pipeline to run
    pipelines = session.query(Pipeline).filter_by(active=True, transform_status=STATUS.QUEUED).all()
    if pipelines:
        print(f"{len(pipelines)} tranform to run")

    for pipeline in pipelines:
        # run_transform_task(pipeline)
        threading.Thread(target=run_transform_task, args=(pipeline,)).start()

def run_extracts():
    # We look in the queue for a pipeline to run
    pipelines = session.query(Pipeline).filter_by(active=True, extract_status=STATUS.QUEUED).all()
    if pipelines:
        print(f"{len(pipelines)} to run")
    for pipeline in pipelines:
        threading.Thread(target=run_extract_task, args=(pipeline,)).start()
  
def add_pipelines_to_queue():
    pipelines = session.query(Pipeline).filter_by(active=True).filter(
        Pipeline.extract_status.not_in([STATUS.QUEUED, STATUS.RUNNING, STATUS.ERROR])
    ).all()
    for pipeline in pipelines:
        pipeline.extract_status = STATUS.QUEUED
    session.commit()


if __name__ == "__main__":
    schedule.every().day.at("22:30").do(add_pipelines_to_queue)

    while True:
        run_extracts()
        run_transforms()
        schedule.run_pending()
        time.sleep(1)
