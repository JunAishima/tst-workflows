import traceback

from prefect import flow, task, get_run_logger
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext


@task
def print_task():
    print("Hello world!")
    logger = get_run_logger()
    logger.info("Hello world!")


def slack(func):
    def wrapper(*args, **kwargs):
        flow_run_name = FlowRunContext.get().flow_run.dict().get("name")
        slack_webhook = SlackWebhook.load("mon-prefect")

        try:
            result = func(*args, **kwargs)
            slack_webhook.notify(
                f":white_check_mark: Flow-run successful. (*{flow_run_name}*)"
            )
            return result
        except Exception as e:
            tb = traceback.format_exception_only(e)
            slack_webhook.notify(f":bangbang: Flow-run failed. (*{flow_run_name}*)")
            raise

    return wrapper


@flow
@slack
def hello_world():
    logger = get_run_logger()
    logger.info("Starting flow")
    print_task()
    test_dict = dict()
    # test_dict["key"]  # Trying to create a key error here.


if __name__ == "__main__":
    hello_world()
