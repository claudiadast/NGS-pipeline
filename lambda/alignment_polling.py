from rkstr8.cloud.batch import BatchJobListStatusPoller

def handler(event, context):
    """
    Handler for aws batch polling lambda function - polls for
    the completion of all jobs in event[jobs]
    :param event: Dict
    :param context: Lambda Context Obj
    """
    print(event)
    job_ids = event
    poller = BatchJobListStatusPoller(job_ids=job_ids)
    poller_outcome = poller.polling_outcome()
    return poller_outcome
    