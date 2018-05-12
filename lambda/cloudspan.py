"""
Module for AWS Lambda handler function to test multi-cloud (GCP, AWS) orchestration using resp. python client libraries.
"""
from google.cloud import storage
import googleapiclient.discovery
import uuid
import os
import json
from pprint import pprint


def handler(event, context):
    """
    Lambda handler function to test multi-cloud orchestration using resp. python client libraries.

    :param event: the event dictionary passed in on invocation, handled by container, defined by the caller
    :param context: the context dictionary passed in on invocation, handled by the container, defined by the caller
    :return: test results as json-like dict
    """

    print("EVENT")
    pprint(event)

    with open ('/tmp/service_creds.json', 'w') as fh:
        gcp_creds_dict = json.dump(event['gcp-administrative']['credentials'], fh)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/service_creds.json'

    return hail_event_dispatch(event)


def get_dataproc_client():
    """Builds an http client authenticated with the service account credentials."""
    dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    return dataproc


def hail_event_dispatch(event):
    hail_command = event['hail-administrative']['hail_control']['hail_command']

    dispatcher = {
        'create': gcp_dataproc_cluster_create,
        'submit': gcp_dataproc_cluster_submit
    }

    hail_callable = dispatcher[hail_command]
    return hail_callable(event)


def gcp_dataproc_cluster_create(event):

    project = event['gcp-administrative']['project']
    zone = event['gcp-administrative']['zone'] 
    cluster_name = event['dataproc-administrative']['cluster_name']

    bucket = event['hail-administrative']['script']['gs_bucket'].split("/")[0]

    master_type = event['dataproc-administrative']['masterConfig']['master_type']
    worker_type = event['dataproc-administrative']['workerConfig']['worker_type']
    preempt_type = event['dataproc-administrative']['preemptConfig']['preempt_type']

    num_masters = event['dataproc-administrative']['masterConfig']['num_masters']
    num_workers = event['dataproc-administrative']['workerConfig']['num_workers']
    num_preempt = event['dataproc-administrative']['preemptConfig']['num_preempt']

    master_boot_disk_size = event['dataproc-administrative']['masterConfig']['boot_disk_size']
    worker_boot_disk_size = event['dataproc-administrative']['workerConfig']['boot_disk_size']
    preempt_boot_disk_size = event['dataproc-administrative']['preemptConfig']['boot_disk_size']

    master_num_ssds = event['dataproc-administrative']['masterConfig']['num_local_ssds']
    worker_num_ssds = event['dataproc-administrative']['workerConfig']['num_local_ssds']
    preempt_num_ssds = event['dataproc-administrative']['preemptConfig']['num_local_ssds']

    image_version = event['dataproc-administrative']['image_version']
    metadata = event['hail-administrative']['metadata']


    try:
        region_as_list = zone.split('-')[:-1]
        region = '-'.join(region_as_list)
    except (AttributeError, IndexError, ValueError):
        raise ValueError('Invalid zone provided, please check your input.')

    dataproc=get_dataproc_client()

    zone_uri = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(project, zone)

    cluster_data = {
        'projectId': project,
        'clusterName': cluster_name,
        'config': {
            'configBucket': bucket,
            'gceClusterConfig': {
                'zoneUri': zone_uri,
                'metadata': metadata
            },
            'masterConfig': {
                'machineTypeUri': master_type,
                'numInstances': num_masters,
                'diskConfig': {
                    'bootDiskSizeGb': master_boot_disk_size,
                    'numLocalSsds': master_num_ssds
                }

            },
            'workerConfig': {
                'machineTypeUri': worker_type,
                'numInstances': num_workers,
                'diskConfig': {
                    'bootDiskSizeGb': worker_boot_disk_size,
                    'numLocalSsds': worker_num_ssds
                }
            },
            'secondaryWorkerConfig': {
                'isPreemptible': True, 
                'machineTypeUri': preempt_type,
                'numInstances': num_preempt,
                'diskConfig': {
                    'bootDiskSizeGb': preempt_boot_disk_size,
                    'numLocalSsds': preempt_num_ssds
                }
            },
            'softwareConfig': {
                'imageVersion': image_version
            },
            'initializationActions': [
              {
                'executableFile': 'gs://dataproc-initialization-actions/conda/bootstrap-conda.sh'
              },    
              {
                'executableFile': 'gs://hail-common/cloudtools/init_notebook1.py'
              }
            ]
        }
    }

    result = dataproc.projects().regions().clusters().create(
        projectId=project,
        region=region,
        body=cluster_data).execute()

    return event, result

def gcp_dataproc_cluster_submit(event):

    print("EVENT FROM CLOUDSPAN CLUSTER SUBMIT")
    pprint(event)

    dataproc = get_dataproc_client()
    project = event['gcp-administrative']['project']
    zone = event['gcp-administrative']['zone']
    try:
        region_as_list = zone.split('-')[:-1]
        region = '-'.join(region_as_list)
    except (AttributeError, IndexError, ValueError):
        raise ValueError('Invalid zone provided, please check your input.')

    cluster_name = event['dataproc-administrative']['cluster_name']

    hail_script_bucket = event['hail-administrative']['script']['gs_bucket']
    hail_script_key = event['hail-administrative']['script']['gs_key']
    hail_hash = event['hail-administrative']['metadata']['HASH']
    spark_version = event['hail-administrative']['metadata']['SPARK']
    hail_version = event['hail-administrative']['metadata']['HAIL_VERSION']
    cohort_prefix = event['hail-administrative']['cohort_prefix']
    final_vcf = cohort_prefix + ".gt.snp.indel.recal.vcf"
    results_uri = list(filter(None, event['hail-administrative']['results_uri'].split("/")))
    final_vcf_bucket = "{}/{}/{}".format(event['hail-administrative']["sink_bucket"], ''.join(results_uri[2:]), "cohort-vcf-vqsr/")
    print("FINAL_VCF_BUCKET", final_vcf_bucket)
    reference_build = event['hail-administrative']['reference_build']
    giab_bucket = event['hail-administrative']['giab_bucket']

    """Submits the Pyspark job to the cluster, assuming `hail_script` has
    already been uploaded to `bucket_name`"""

    job_details = {
        'projectId': project,
        'region': region,
        'job': {
            'placement': {
                'clusterName': cluster_name
            },
            'pysparkJob': {
                'mainPythonFileUri': 'gs://{}/{}'.format(hail_script_bucket, hail_script_key),
                'args': [
                    final_vcf_bucket,
                    final_vcf,
                    cohort_prefix,
                    reference_build,
                    giab_bucket
                ],
                'pythonFileUris': 'gs://hail-common/builds/{}/python/hail-{}-{}.zip'.format(hail_version, hail_version, hail_hash),
                'fileUris': 'gs://hail-common/builds/{}/jars/hail-{}-{}-Spark-{}.jar'.format(hail_version, hail_version, hail_hash, spark_version),
                'properties': { 'spark.driver.extraClassPath' : './hail-{}-{}-Spark-{}.jar'.format(hail_version, hail_hash, spark_version), 'spark.executor.extraClassPath': './hail-{}-{}-Spark-{}.jar'.format(hail_version, hail_hash, spark_version) }
            }
        }
    }

    result = dataproc.projects().regions().jobs().submit(
        projectId=project,
        region=region,
        body=job_details).execute()

    job_id = result['reference']['jobId']

    print('Submitted job ID {}'.format(job_id))

    print("RESULT FROM CLOUDSPAN JOB SUBMIT")
    pprint(result)

    return event, result