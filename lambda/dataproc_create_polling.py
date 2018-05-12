from rkstr8.cloud.google import DataprocCreateStatusPoller
from pprint import pprint


def handler(event, context):
    """
    Handler to poll dataproc cluster creation completion.
    :param event: Dict
    :param context: Lambda Context Obj
    """
    print("EVENT FROM DATAPROC_CREATE_POLLING.PY")
    pprint(event)
    credentials = event[0]['gcp-administrative']['credentials']
    project_id = event[0]['gcp-administrative']['project']
    region_temp = event[0]['gcp-administrative']['zone'].split('-')
    region = "{}-{}".format(region_temp[0], region_temp[1])
    cluster_name = event[0]['dataproc-administrative']['cluster_name']

    poller = DataprocCreateStatusPoller(credentials, project_id, region, cluster_name)
    poller_outcome = poller.polling_outcome()
    return poller_outcome