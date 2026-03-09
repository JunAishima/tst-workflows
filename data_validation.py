from prefect import task, flow, get_run_logger
import time as ttime
from tiled.client import from_uri


@task(retries=2, retry_delay_seconds=10)
def read_all_streams(uid, beamline_acronym, api_key=None, dry_run=False):
    logger = get_run_logger()
    if dry_run:
        logger.info("Dry run: not creating tiled client or checking streams")
        return
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl["tst"]["raw"][uid]
    logger.info(f"Validating uid {run.start['uid']}")
    start_time = ttime.monotonic()
    for stream in run:
        logger.info(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = run[stream].read()
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")


@flow
def data_validation(uid, api_key=None, dry_run=False):
    read_all_streams(uid, beamline_acronym="tst", api_key=api_key, dry_run=dry_run)
