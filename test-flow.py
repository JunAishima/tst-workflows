import time as ttime

from prefect import task, flow, get_run_logger


@task
def print_task():
    logger = get_run_logger()
    logger.info("Agent connection testing")
    # print("Agent connection testing")


@task
def simple_task():
    logger = get_run_logger()
    logger = logger.info("logger")
    with open("/nsls2/data/dssi/scratch/prefect-outputs/tst/test-data.txt", "w") as f:
        f.write(f"Flow test - {ttime.ctime()}")


@flow
def test_flow():
    logger = get_run_logger()
    logger.info("Starting flow")
    print_task()
    simple_task()


if __name__ == "__main__":
    test_flow()
