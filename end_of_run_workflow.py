from prefect import task, flow, get_run_logger
from data_validation import data_validation
from  prefect2_test_flow import hello_world
from test_extra_client import get_other_docs
# from long_flow import long_flow


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    # hello_world()
    data_validation(uid, return_state=True)
    get_other_docs(uid)
    # long_flow(iterations=100, sleep_length=10)
    log_completion()

