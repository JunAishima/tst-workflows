import traceback

from data_validation import data_validation, get_run
from prefect import flow, get_run_logger, task
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext
from test_extra_client import get_other_docs

CATEGORY_NAME = "tst"


def slack(func):
    def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
        flow_run_name = FlowRunContext.get().flow_run.dict().get("name")
        mon_prefect = SlackWebhook.load("mon-prefect")
        mon_prefect_tst = SlackWebhook.load("mon-prefect-tst")
        mon_bluesky = SlackWebhook.load("mon-bluesky")

        uid = stop_doc["run_start"]
        run = get_run(uid, api_key=api_key)
        scan_id = run.start["scan_id"]

        if stop_doc.get("exit_status") == "fail":
            mon_bluesky.notify(
                f":bangbang: {CATEGORY_NAME} bluesky-run failed. "
                f"(*{flow_run_name}*)\n ```run_start: {uid}\n"
                f"scan_id: {scan_id}``` ```reason: "
                f"{stop_doc.get('reason', 'none')}```"
            )
        try:
            result = func(stop_doc, api_key=api_key, dry_run=dry_run)
            mon_prefect_tst.notify(
                f":white_check_mark: {CATEGORY_NAME} flow-run successful. "
                f"(*{flow_run_name}*)\n ```run_start: {uid}\n"
                f"scan_id: {scan_id}```"
            )
            return result
        except Exception as error:
            tb = traceback.format_exception_only(error)
            message = (
                f":bangbang: {CATEGORY_NAME} flow-run failed. "
                f"(*{flow_run_name}*)\n ```run_start: {uid}\n"
                f"scan_id: {scan_id}``` ```{tb[-1]}```"
            )
            mon_prefect_tst.notify(message)
            mon_prefect.notify(message)
            raise

    return end_of_run_workflow


# from long_flow import long_flow


@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! dry_run: {dry_run}")


@flow
@slack
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    uid = stop_doc["run_start"]
    data_validation(uid, api_key=api_key, dry_run=dry_run)
    get_other_docs(uid, api_key=api_key)
    # long_flow(iterations=100, sleep_length=10)  # keep in to potentially run as a test in the future
    log_completion(dry_run=dry_run)
