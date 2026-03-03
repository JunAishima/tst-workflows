from tiled.client import from_profile


LOCATION = "tst"


def get_tiled_client(api_key=None):
    tiled_client = from_profile("nsls2", api_key=api_key)[LOCATION]
    return tiled_client
