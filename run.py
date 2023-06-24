import argparse

from database import Pipeline, Session
from server import run_extract_task, run_transfer_task, run_transform_task

session = Session()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("pipeline_id", type=int)
    parser.add_argument("--extract", action="store_true")
    parser.add_argument("--transform", action="store_true")
    parser.add_argument("--transfer", action="store_true")
    args = parser.parse_args()
    pipeline = session.query(Pipeline).get(args.pipeline_id)
    if args.extract:
        thread = run_extract_task(pipeline)
        thread.join()
    if args.transform:
        thread = run_transform_task(pipeline)
        thread.join()
    if args.transfer:
        thread = run_transfer_task(pipeline)
        thread.join()

    if not args.extract and not args.transform and not args.transfer:
        raise ValueError("Need to specify --extract or --transform or --transfer")
