import time

import schedule
import yaml

from database import Pipeline, session, Task, STATUS
from extract import scraper
from run_transform import transform
from datetime import datetime



def extract(source_config, pipeline_id):
    from database import Pipeline, session
    pipeline = session.query(Pipeline).filter_by(id=pipeline_id).first()
    print("Run extract")
    scraper.runner(source_config, pipeline.target.uri, debug=False, memory=pipeline.source, params=pipeline.source.config)
    # After scraping we transform
    pipeline.transform_status = STATUS.QUEUED
    session.commit()

def run_extract_task(pipeline):
    task = Task(
        pipeline_id=pipeline.id,
        type="extract",
        status=STATUS.RUNNING,
        time_start=datetime.now()
    )
    session.add(task)
    session.commit()

    try:
        source_name = pipeline.source.name
        with open(f'sources/{source_name}.yml') as f:
            source_config = yaml.safe_load(f)

        extract(source_config, pipeline.id)

        task.status = STATUS.DONE
    except (Exception, KeyboardInterrupt) as e:
        task.status = STATUS.ERROR
        task.error_log = type(e).__name__ + '\n' + str(e)
    finally:
        task.time_end = datetime.now()
        session.commit()


def run_extracts():
    # Check if a pipeline is already running
    running_pipeline = session.query(Pipeline).filter_by(extract_status=STATUS.RUNNING).first()
    if running_pipeline is not None:
        return

    # If not, we look in the queue for a pipeline to run
    pipelines = session.query(Pipeline).filter_by(active=True, extract_status=STATUS.QUEUED).all()
    if pipelines:
        print(f"{len(pipelines)} to run")
    for pipeline in pipelines:
        run_extract_task(pipeline)
  

def run_transform_task(pipeline):
    print("Running transform", pipeline.id)
    task = Task(
        pipeline_id=pipeline.id,
        type="transform",
        status=STATUS.RUNNING,
        time_start=datetime.now()
    )
    session.add(task)
    session.commit()
    try:
        print("Starting job")
        transform(pipeline.target.uri)
        print("Job done")
        task.status = STATUS.DONE
    except (Exception, KeyboardInterrupt) as e:
        task.status = STATUS.ERROR
        task.error_log = type(e).__name__ + '\n' + str(e)
    finally:
        task.time_end = datetime.now()
        session.commit()


def run_transforms():
    # Check if a pipeline is already running
    pipeline = session.query(Pipeline).filter_by(transform_status=STATUS.RUNNING).first()
    if pipeline is not None:
        return

    # If not, we look in the queue for a pipeline to run
    pipelines = session.query(Pipeline).filter_by(active=True,transform_status=STATUS.QUEUED).all()
    if pipelines:
        print(f"{len(pipelines)} tranform to run")

    for pipeline in pipelines:
        run_transform_task(pipeline)


def add_pipelines_to_queue():
    pipelines = session.query(Pipeline).filter_by(active=True).filter(
        Pipeline.extract_status.not_in([STATUS.QUEUED, STATUS.RUNNING, STATUS.ERROR])
    ).all()
    for pipeline in pipelines:
        pipeline.extract_status = STATUS.QUEUED
    session.commit()


schedule.every().day.at("22:30").do(add_pipelines_to_queue)

if __name__ == "__main__":
    while True:
        run_extracts()
        run_transforms()
        schedule.run_pending()
        time.sleep(1)
