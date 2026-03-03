from prefect import task, get_run_logger
from utils import get_tiled_client


@task
def get_other_docs(uid, api_key=None):
    logger = get_run_logger()
    result = get_tiled_client()["raw"][uid]
    for name, doc in result.documents():
        logger.info(f"name: {name}, doc: {doc}")
