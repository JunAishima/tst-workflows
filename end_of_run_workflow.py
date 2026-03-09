import os

from prefect import task, flow, get_run_logger
from data_validation import data_validation
from test_extra_client import get_other_docs
from dotenv import load_dotenv
# from long_flow import long_flow


def get_api_key_from_env(api_key=None):
    logger = get_run_logger()
    try:
        with open("/srv/container.secret", "r") as secrets:
            load_dotenv(stream=secrets)
        api_key = os.environ["TILED_API_KEY"]
    except Exception:
        logger.exception("Exception while getting Tiled API key")
    return api_key


@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! Dry run = {dry_run}")


@flow
def end_of_run_workflow(stop_doc, dry_run=False):
    uid = stop_doc["run_start"]
    # hello_world()
    api_key = get_api_key_from_env(api_key=None)
    data_validation(uid, return_state=True, api_key=api_key, dry_run=dry_run)
    get_other_docs(uid, api_key=api_key, dry_run=dry_run)
    # long_flow(iterations=100, sleep_length=10, dry_run=dry_run)
    log_completion(dry_run=dry_run)
