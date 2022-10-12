import argparse

import yaml

import scraper


def run(config):
    print(config)
    scraper.runner(config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=argparse.FileType("r"))
    args = parser.parse_args()
    with open(args.file.name) as f:
        config = yaml.safe_load(f)
    run(config)
