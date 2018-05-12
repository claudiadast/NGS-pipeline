from rkstr8.cloud.google import GoogleCloudLambdaAuth
from googleapiclient import discovery
import json
import datetime
from pprint import pprint


def handler(event, context):
    """
    Handler to create a one-time transfer from Amazon S3 to Google Cloud Storage
    :param event: Dict
    :param context: Lambda Context Obj
    """

    GoogleCloudLambdaAuth(event['GCP_creds']).configure_google_creds()

    storagetransfer = discovery.build('storagetransfer', 'v1')
    description = "-".join(("transfer-job", event['queue']))
    project_id = event['project_id']
    source_bucket = event['results_uri'].split("/")[2]
    print("HANDOFF SOURCE BUCKET", source_bucket)
    sink_bucket = event['sink_bucket'].split("/")[2]
    print("HANDOFF SINK BUCKET", sink_bucket)
    include_prefix = "{}{}/{}{}".format("/".join(event['results_uri'].split("/")[3:]), "cohort-vcf-vqsr", event['cohort_prefix'], ".gt.snp.indel.recal.vcf")
    print("INCLUDE_PREFIX", include_prefix)
    access_key = event['aws_access_key']
    secret_access_key = event['aws_secret_key']

    now = datetime.datetime.utcnow()

    day = now.day
    month = now.month
    year = now.year

    hours = now.hour 
    minutes_obj = now + datetime.timedelta(minutes=2)
    minutes = minutes_obj.minute 

    transfer_job = {
        'description': description,
        'status': 'ENABLED',
        'projectId': project_id,
        'schedule': {
            'scheduleStartDate': {
                'day': day,
                'month': month,
                'year': year
            },
            'scheduleEndDate': {
                'day': day,
                'month': month,
                'year': year
            },
            'startTimeOfDay': {
                'hours': hours,
                'minutes': minutes
            }
        },
        'transferSpec': {
            'objectConditions': {
                'includePrefixes': [
                    include_prefix
                ]
            },
            'awsS3DataSource': {
                'bucketName': source_bucket,
                'awsAccessKey': {
                    'accessKeyId': access_key,
                    'secretAccessKey': secret_access_key
                }
            },
            'gcsDataSink': {
                'bucketName': sink_bucket
            }
        }
    }


    result = storagetransfer.transferJobs().create(body=transfer_job).execute()
    print('Returned transferJob: {}'.format(
        json.dumps(result, indent=4)))

    print("TRANSFER RESULT")
    pprint(result)

    try:
        event['transferJobID'] = result['name']
    except KeyError as e:
        print("The transfer job ID does not exist.")
        raise e

    return event
