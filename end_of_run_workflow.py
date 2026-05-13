import traceback

from prefect import task, flow, get_run_logger
from data_validation import data_validation, get_run, get_api_key_from_env
from test_extra_client import get_other_docs
from prefect.context import FlowRunContext
from prefect.settings import PREFECT_UI_URL
from prefect.blocks.notifications import SlackWebhook

# from long_flow import long_flow

CATALOG_NAME = "tst"


def slack(func):
    """
    Send a message to mon-prefect-tst slack channel about the flow-run status (pass or fail).
    Send a message to mon-prefect slack channel if the flow-run failed.
    Send a message to mon-bluesky slack channel if the bluesky-run failed.
    Skip sending a message to mon-prefect-<group> slack channel because it is not part of a group.

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

            # Send a message to mon-prefect-tst if flow-run is successful.
            mon_prefect_tst.notify(
                f":white_check_mark: {CATALOG_NAME} flow-run successful. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}```"
            )
            flow_run = FlowRunContext.get().flow_run

            return result
        except Exception as e:
            tb = traceback.format_exception_only(e)

            # Send a message to mon-prefect and mon-prefect-tst if flow-run failed.
            message = f":bangbang: {CATALOG_NAME} flow-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            mon_prefect_tst.notify(message)
            mon_prefect.notify(message)
            flow_run = FlowRunContext.get().flow_run

            # Send a message to mon-prefect-<group> if flow-run failed. Add link to flow-run
            group_message = (
                f":bangbang: {CATALOG_NAME} flow-run failed. <{PREFECT_UI_URL.value()}/flow-runs/"
                + f"flow-run/{flow_run.id}|the flow run link> (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            )

            raise

    return end_of_run_workflow


@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! Dry run = {dry_run}")

@task
def test_all_channels():
    mon_prefect = SlackWebhook.load("mon-prefect")
    mon_prefect_cms = SlackWebhook.load("mon-prefect-cms")
    mon_prefect_chx = SlackWebhook.load("mon-prefect-chx")
    mon_prefect_opls = SlackWebhook.load("mon-prefect-opls")
    mon_prefect_smi = SlackWebhook.load("mon-prefect-smi")
    mon_prefect_cs = SlackWebhook.load("mon-prefect-cs")

    mon_prefect_arpes = SlackWebhook.load("mon-prefect-arpes")
    mon_prefect_est = SlackWebhook.load("mon-prefect-est")

    mon_prefect_hex = SlackWebhook.load("mon-prefect-hex")
    mon_prefect_hxm = SlackWebhook.load("mon-prefect-hxm")

    mon_prefect_srx = SlackWebhook.load("mon-prefect-srx")
    mon_prefect_cdi = SlackWebhook.load("mon-prefect-cdi")
    mon_prefect_fxi = SlackWebhook.load("mon-prefect-fxi")
    mon_prefect_im = SlackWebhook.load("mon-prefect-im")

    mon_prefect_haxpes = SlackWebhook.load("mon-prefect-haxpes")
    mon_prefect_rsoxs = SlackWebhook.load("mon-prefect-rsoxs")
    mon_prefect_qas = SlackWebhook.load("mon-prefect-qas")
    mon_prefect_spec = SlackWebhook.load("mon-prefect-spec")
    mon_bluesky = SlackWebhook.load("mon-bluesky")

    mon_prefect.notify("mon-prefect")
    mon_prefect_cms.notify("mon-prefect-cms")
    mon_prefect_chx.notify("mon-prefect-chx")
    mon_prefect_opls.notify("mon-prefect-opls")
    mon_prefect_smi.notify("mon-prefect-smi")
    mon_prefect_cs.notify("mon-prefect-cs")

    mon_prefect_arpes.notify("mon-prefect-arpes")
    mon_prefect_est.notify("mon-prefect-est")

    mon_prefect_hex.notify("mon-prefect-hex")
    mon_prefect_hxm.notify("mon-prefect-hxm")

    mon_prefect_srx.notify("mon-prefect-srx")
    mon_prefect_cdi.notify("mon-prefect-cdi")
    mon_prefect_fxi.notify("mon-prefect-fxi")
    mon_prefect_im.notify("mon-prefect-im")

    mon_prefect_haxpes.notify("mon-prefect-haxpes")
    mon_prefect_rsoxs.notify("mon-prefect-rsoxs")
    mon_prefect_qas.notify("mon-prefect-qas")
    mon_prefect_spec.notify("mon-prefect-spec")



@flow
@slack
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    uid = stop_doc["run_start"]
    # hello_world()
    test_all_channels()
    data_validation(uid, return_state=True, api_key=api_key)
    get_other_docs(uid, api_key=api_key)
    # long_flow(iterations=100, sleep_length=10, dry_run=dry_run)
    log_completion(dry_run=dry_run)
