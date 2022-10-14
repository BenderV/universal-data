import argparse

import yaml

import scraper
from utils import apply_nested, partial_format

if __name__ == "__main__":
    # Next: switch to dynamic config
    # https://stackoverflow.com/questions/50499340/specify-options-and-arguments-dynamically
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=argparse.FileType("r"))
    args, unknown_args = parser.parse_known_args()
    with open(args.source.name) as f:
        source_config = yaml.safe_load(f)

    # Transform ['config1', 'value1', 'config2', 'value2'] into {'config1': 'value1', 'config2': 'value2'}
    # https://stackoverflow.com/a/5389547/2131871
    params = dict(zip([l.strip("--") for l in unknown_args[::2]], unknown_args[1::2]))

    source_config_with_params = apply_nested(
        source_config,
        lambda x: partial_format(x, **params),
    )

    scraper.runner(source_config_with_params)
