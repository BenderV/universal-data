import time

import schedule
import yaml

from database import Pipeline, session
from extract import scraper


class STATUS:
    ERROR = 'error'
    DONE = 'done'
    QUEUED = 'queued'
    RUNNING = 'running'


def run_extract(source_config, pipeline_id):
    from database import Pipeline, session
    pipeline = session.query(Pipeline).filter_by(id=pipeline_id).first()
    print("Run extract")
    scraper.runner(source_config, pipeline.target.uri, debug=False, memory=pipeline.source, params=pipeline.source.config)

def run_pipeline(pipeline):
    source_name = pipeline.source.name
    with open(f'sources/{source_name}.yml') as f:
        source_config = yaml.safe_load(f)
   
    run_extract(source_config, pipeline.id)

def run_pipelines():
    print("run_pipelines")
    # Check if a pipeline is already running
    running_pipeline = session.query(Pipeline).filter_by(status=STATUS.RUNNING).first()
    if running_pipeline is not None:
        return


    # If not, we look in the queue for a pipeline to run
    pipelines = session.query(Pipeline).filter_by(active=True, status=STATUS.QUEUED).all()
    for pipeline in pipelines:
        print("Running pipeline", pipeline.id)
        pipeline.status = STATUS.RUNNING
        session.commit()
        print(pipeline)
        try:
            print("Starting job")
            run_pipeline(pipeline)
            print("Job done")
            pipeline.status = STATUS.DONE
        except Exception as e:
            print(e)
            pipeline.status = STATUS.ERROR
        finally:
            session.commit()

def add_pipelines_to_queue():
    pipelines = session.query(Pipeline).filter_by(active=True).filter(
        Pipeline.status.not_in([STATUS.QUEUED, STATUS.RUNNING, STATUS.ERROR])
    ).all()
    for pipeline in pipelines:
        pipeline.status = STATUS.QUEUED
    session.commit()

schedule.every().day.at("22:30").do(add_pipelines_to_queue)

if __name__ == "__main__":
    while True:
        run_pipelines()
        schedule.run_pending()
        time.sleep(1)
