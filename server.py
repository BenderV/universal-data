import yaml

from database import Pipeline, session
from extract import scraper

pipelines = session.query(Pipeline).filter_by(active=True).all()

def run_extract(source_config, pipeline_id):
    from database import Pipeline, session
    pipeline = session.query(Pipeline).filter_by(id=pipeline_id).first()
    scraper.runner(source_config, pipeline.target.uri, debug=True, memory=pipeline.source)

def run_pipeline(pipeline):
    source_name = pipeline.source.name
    with open(f'sources/{source_name}.yml') as f:
        source_config = yaml.safe_load(f)
   
    run_extract(source_config, pipeline.id, params=pipeline.source.config)


def run_pipelines():
    for pipeline in pipelines:
        print(pipeline)
        try:
            run_pipeline(pipeline)
        except Exception as e:
            print(e)
            raise
            
if __name__ == '__main__':
    run_pipelines()
