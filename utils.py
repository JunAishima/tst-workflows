from dotenv import load_dotenv
from prefect import get_run_logger
from tiled.client import from_profile

import os

LOCATION = "tst"


def get_tiled_client():
    logger = get_run_logger()
    with open("/srv/env.secrets", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    logger.info(f"first 4 characters of key: {api_key:4}")
    tiled_client = from_profile("nsls2", api_key=api_key)[LOCATION]
    return tiled_client
