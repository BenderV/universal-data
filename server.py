import yaml

import scraper
from database import Client, Pipeline, Target, session

pipelines = session.query(Pipeline).all()

for pipeline in pipelines:
    print(pipeline)

    source_name = pipeline.source.name
    with open(f'sources/{source_name}.yml') as f:
        source_config = yaml.safe_load(f)
    
    print(source_config)

    scraper.runner(source_config, pipeline.target.uri, True, pipeline.source)
 