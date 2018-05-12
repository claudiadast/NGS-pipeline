from rkstr8.cloud.google import DataprocSubmitStatusPoller
from pprint import pprint


def handler(event, context):
    """
    Handler to submit tasks to dataproc cluster.
    :param event: Dict
    :param context: Lambda Context Obj
    """
    print("EVENT FROM DATAPROC_SUBMIT_POLLING")
    pprint(event)
    credentials = event[0]['gcp-administrative']['credentials']
    project_id = event[0]['gcp-administrative']['project']
    region_temp = event[0]['gcp-administrative']['zone'].split('-')
    region = "{}-{}".format(region_temp[0], region_temp[1])
    job_id = event[1]['reference']['jobId']

    poller = DataprocSubmitStatusPoller(credentials, project_id, region, job_id)
    poller_outcome = poller.polling_outcome()
    return poller_outcome