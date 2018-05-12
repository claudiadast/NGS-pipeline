import datetime
import boto3
import json

def handler(event, context):
    """
    Handler to submit jobs for sentieon_genotyper.
    :param event: Dict
    :param context: Lambda Context Obj
    """
    samples = event['samples']
    job_defs = event['job_defs']
    job_queue = event['queue']
    cohort_prefix = event['cohort_prefix']

    ref_uri = event['ref_uri']
    in_uri = event['in_uri']
    param_uri = event['param_uri']
    results_uri = event['results_uri']
    license_uri = event['license_uri']
    sentieon_pkg = event['sentieon_pkg']
    build = event['build']
    ome = event['mode']['ome']

    if ome == "wes" and "target_file" in event:
        target_file = event['target_file']
    elif ome == "wgs":
        target_file = "None"
    else:
        raise ValueError("Ome set to wes, but no target_file was set!")

    # Build in_files from samples; suffixes and index files are added
    # in the container level
    gvcfs = ["{}.gvcf.gz".format(s) for s in samples]
    in_files = ",".join(gvcfs)
    job_ids = []

    now_unformat = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now = now_unformat.replace(" ", "_").replace(":", "-")

    sentieon_genotyper_submit = boto3.client("batch").submit_job(
        jobName="sentieon_genotyper_{}_{}".format(cohort_prefix, now),
        jobQueue=job_queue,
        jobDefinition=job_defs["sentieon_genotyper_job"],
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
                        'value': cohort_prefix
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
                        'name': 'in_uri',
                        'value': "{}bgz-gvcfs/".format(results_uri)
                    },
                    {
                        'name': 'out_uri',
                        'value': "{}cohort-vcf-vqsr/".format(results_uri)
                    },
                    {
                        'name': 'log_uri',
                        'value': "{}logs/".format(results_uri)
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
                        'name': 'in_files',
                        'value': in_files
                    }
            ]
        },
    )

    job_ids.append(sentieon_genotyper_submit["jobId"])

    return job_ids










