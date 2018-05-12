from rkstr8.cloud.google import HandoffStatusPoller
import os
import sys


def handler(event, context):
    print("THIS IS THE EVENT FROM HANDOFF_POLLER LAMBDA")
    print(event)
    credentials = event['GCP_creds']
    project_id = event['project_id']
    transfer_job_id = event['transferJobID']
    results_uri = event['results_uri']
    cohort_prefix = event['cohort_prefix']
    poller = HandoffStatusPoller(credentials, project_id, transfer_job_id)
    print(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    sys.stdout.flush()
    poller_outcome = poller.polling_outcome()
    return poller_outcome