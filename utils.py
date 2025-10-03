from tiled.client import from_profile
from prefect.blocks.system import Secret

import os

os.environ["TILED_API_KEY"] = Secret.load("tiled-tst-api-key").get()
tiled_client = from_profile("nsls2")
os.environ.pop("TILED_API_KEY")

def get_tiled_client():
    return tiled_client
