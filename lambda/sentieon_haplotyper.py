import datetime
import boto3
import json

def handler(event, context):
    """
    Handler to submit jobs for sentieon_haplotyper.
    :param event: Dict
    :param context: Lambda Context Obj
    """
    samples = event['samples']
    job_defs = event['job_defs']
    job_queue = event['queue']

    ref_uri = event['ref_uri']
    in_uri = event['in_uri']
    results_uri = event['results_uri']
    param_uri = event['param_uri']
    license_uri = event['license_uri']
    sentieon_pkg = event['sentieon_pkg']

    build = event['build']
    mode = event['mode']['label']
    ome = event['mode']['ome']
    hap_threads = str(event['mode'][mode]['threads']['hap'])

    if ome == "wes" and "target_file" in event:
        target_file = event['target_file']
    elif ome == "wgs":
        target_file = "None"
    else:
        raise ValueError("Ome set to wes, but no target_file was set!")


    job_ids = []

    for sample in samples:
        now_unformat = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        now = now_unformat.replace(" ", "_").replace(":", "-")
        haplotyper_submit = boto3.client("batch").submit_job(
            jobName="haplotyper_{}_{}".format(sample, now),
            jobQueue=job_queue,
            jobDefinition=job_defs["sentieon_haplotyper_job"],
            containerOverrides={
                'environment': [
                    {
                        'name': 'build',
                        'value': build
                    },
                    {
                        'name': 'ome',
                        'value': ome
                    },
                    {
                        'name': 'target_file',
                        'value': target_file
                    },
                    {
                        'name': 'prefix',
                        'value': sample
                    },
                    {
                        'name': 'threads',
                        'value': hap_threads
                    },
                    {
                        'name': 'param_uri',
                        'value': param_uri
                    },
                    {
                        'name': 'ref_uri',
                        'value': ref_uri
                    },
                    {
                        'name': 'license_uri',
                        'value': license_uri
                    },
                    {
                        'name': 'sentieon_pkg',
                        'value': sentieon_pkg
                    },
                    {
                        'name': 'in_uri',
                        'value': "{}processed-bams/".format(results_uri)
                    },
                    {
                        'name': 'out_uri',
                        'value': "{}bgz-gvcfs/".format(results_uri)
                    },
                    {
                        'name': 'log_uri',
                        'value': "{}logs/".format(results_uri)
                    }
                ]
            },
        )
        job_ids.append(haplotyper_submit["jobId"])

      
    return job_ids










