from __future__ import annotations

from prefect.deployments import run_deployment
from datetime import datetime

run_deployment(
    name="end-of-run-workflow/tst-end-of-run-workflow-docker",
    parameters={"stop_doc": {"run_start": "adfwerwerwerw"}},
    timeout=15,  # don't wait for the run to finish # edit to 15 sec
)
