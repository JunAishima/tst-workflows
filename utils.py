from tiled.client import from_uri


LOCATION = "tst"


def get_tiled_client(api_key=None):
    tiled_client = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)[LOCATION]
    return tiled_client
