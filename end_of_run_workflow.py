import traceback

from prefect import task, flow, get_run_logger
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext
from prefect.settings import PREFECT_UI_URL
from data_validation import data_validation
from test_extra_client import get_other_docs

# from long_flow import long_flow

CATALOG_NAME = "tst"


def slack(func):
    """
    Send a message to mon-prefect-tst slack channel about the flow-run status.
    Send a message to mon-prefect slack channel if the flow-run failed.
    Send a message to mon-bluesky slack channel if the bluesky-run failed.
    Skip sending a message to mon-prefect-gr slack channel because it is not part of a group.

    NOTE: the name of this inner function is the same as the real end_of_workflow() function because
    when the decorator is used, Prefect sees the name of this inner function as the name of
    the flow. To keep the naming of workflows consistent, the name of this inner function had to match the expected name.
    """

    def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
        flow_run_name = FlowRunContext.get().flow_run.dict().get("name")

        # Load slack credentials that are saved in Prefect.
        mon_prefect = SlackWebhook.load("mon-prefect")
        mon_prefect_tst = SlackWebhook.load("mon-prefect-tst")
        mon_bluesky = SlackWebhook.load("mon-bluesky")

        # Get the uid.
        uid = stop_doc["run_start"]

        # Get Tiled API key, if not set already
        if not api_key:
            api_key = get_api_key_from_env()

        # Get the scan_id.
        run = get_run(uid, api_key=api_key)
        scan_id = run.start["scan_id"]

        # Send a message to mon-bluesky if bluesky-run failed.
        if stop_doc.get("exit_status") == "fail":
            mon_bluesky.notify(
                f":bangbang: {CATALOG_NAME} bluesky-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```reason: {stop_doc.get('reason', 'none')}```"
            )

        try:
            result = func(stop_doc, api_key=api_key, dry_run=dry_run)

            # Send a message to mon-prefect if flow-run is successful.
            mon_prefect_tst.notify(
                f":white_check_mark: {CATALOG_NAME} flow-run successful. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}```"
            )
            return result
        except Exception as e:
            tb = traceback.format_exception_only(e)

            # Send a message to mon-prefect if flow-run failed.
            mon_prefect_tst.notify(
                f":bangbang: {CATALOG_NAME} flow-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            )
            mon_prefect.notify(
                f":bangbang: {CATALOG_NAME} flow-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            )
            flow_run = FlowRunContext.get().flow_run
            group_message = f":bangbang: {CATALOG_NAME} flow-run failed. <https://{PREFECT_UI_URL.value()}/flow-runs/"
                            f"flow-run/{flow_run.id}|the flow run link> (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            mon_prefect_tst.notify(group_message)

            raise

    return end_of_run_workflow


@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! Dry run = {dry_run}")


@flow
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    uid = stop_doc["run_start"]
    # hello_world()
    data_validation(uid, return_state=True, api_key=api_key)
    get_other_docs(uid, api_key=api_key)
    # long_flow(iterations=100, sleep_length=10, dry_run=dry_run)
    log_completion(dry_run=dry_run)
