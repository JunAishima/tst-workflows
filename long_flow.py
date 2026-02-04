from prefect import task, flow
import time as ttime


@task
def print_and_sleep(iterations, sleep_length):
    # Long running task
    print("Long task...")
    for i in range(int(iterations)):
        print(f"Iteration number {i}")
        ttime.sleep(int(sleep_length))


@flow(log_prints=True)
def long_flow(iterations, sleep_length):
    print("Starting long flow...")
    print_and_sleep(iterations, sleep_length)
    print("Done!")
