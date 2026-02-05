from dotenv import load_dotenv
from tiled.client import from_profile

import os

LOCATION = "tst"


def get_tiled_client():
    with open("/srv/tiled.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    tiled_client = from_profile("nsls2", api_key=api_key)[LOCATION]
    return tiled_client
