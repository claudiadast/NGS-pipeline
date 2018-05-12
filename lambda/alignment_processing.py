import datetime
import boto3

def handler(event, context):
    """
    Handler to submit jobs for bwa-base_recal_table.
    :param event: Dict
    :param context: Lambda Context Obj
    """

    samples = event['samples']
    suffix = event['suffix']
    job_defs = event['job_defs']
    job_queue = event['queue']

    ref_uri = event['ref_uri']
    in_uri = event['in_uri']
    results_uri = event['results_uri']
    param_uri = event['param_uri']

    build = event['build']
    mode = event['mode']['label']
    ome = event['mode']['ome']
    bwa_threads = str(event['mode'][mode]['threads']['bwa'])
    brt_threads = str(event['mode'][mode]['threads']['brt'])

    if ome == "wes" and "target_file" in event:
        target_file = event['target_file']
    elif ome == "wgs":
        # Environment variable override must be a string
        target_file = "None"
    else:
        raise ValueError("Ome set to wes, but no target_file was set!")

    job_ids = []

    for sample in samples:
        now_unformat = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        now = now_unformat.replace(" ", "_").replace(":", "-")
        bwa_mem_submit = boto3.client("batch").submit_job(
            jobName="bwa_mem_{}_{}".format(sample, now),
            jobQueue=job_queue,
            jobDefinition=job_defs["bwa_mem_job"],
            containerOverrides={
                'environment': [
                    {
                        'name': 'build',
                        'value': build
                    },
                    {
                        'name': 'prefix',
                        'value': sample
                    },
                    {
                        'name': 'suffix',
                        'value': suffix
                    },
                    {
                        'name': 'threads',
                        'value': bwa_threads
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
                        'value': in_uri
                    },
                    {
                        'name': 'out_uri',
                        'value': "{}bam-processing/".format(results_uri)
                    },
                    {
                        'name': 'log_uri',
                        'value': "{}logs/".format(results_uri)
                    }
                ]
            },
        )
        job_ids.append(bwa_mem_submit["jobId"])

        sort_sam_submit = boto3.client("batch").submit_job(
            jobName="sort_sam_{}_{}".format(sample, now),
            jobQueue=job_queue,
            jobDefinition=job_defs["sort_sam_job"],
            dependsOn=[{"jobId":bwa_mem_submit["jobId"]}],
            containerOverrides={
                'environment': [
                    {
                        'name': 'build',
                        'value': build
                    },
                    {
                        'name': 'prefix',
                        'value': sample
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
                        'value': "{}bam-processing/".format(results_uri)
                    },
                    {
                        'name': 'out_uri',
                        'value': "{}bam-processing/".format(results_uri)
                    },
                    {
                        'name': 'log_uri',
                        'value': "{}logs/".format(results_uri)
                    }
                ]
            },
        )
        job_ids.append(sort_sam_submit["jobId"])

        mark_dups_submit = boto3.client("batch").submit_job(
            jobName="mark_dups_{}_{}".format(sample, now),
            jobQueue=job_queue,
            jobDefinition=job_defs["mark_dups_job"],
            dependsOn=[{"jobId":sort_sam_submit["jobId"]}],
            containerOverrides={
                'environment': [
                    {
                        'name': 'build',
                        'value': build
                    },
                    {
                        'name': 'prefix',
                        'value': sample
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
                        'value': "{}bam-processing/".format(results_uri)
                    },
                    {
                        'name': 'out_uri',
                        'value': "{}bam-processing/".format(results_uri)
                    },
                    {
                        'name': 'log_uri',
                        'value': "{}logs/".format(results_uri)
                    }
                ]
            },
        )
        job_ids.append(mark_dups_submit["jobId"])

        index_bam_submit = boto3.client("batch").submit_job(
            jobName="index_bam_{}_{}".format(sample, now),
            jobQueue=job_queue,
            jobDefinition=job_defs["index_bam_job"],
            dependsOn=[{"jobId":mark_dups_submit["jobId"]}],
            containerOverrides={
                'environment': [
                    {
                        'name': 'build',
                        'value': build
                    },
                    {
                        'name': 'prefix',
                        'value': sample
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
                        'value': "{}bam-processing/".format(results_uri)
                    },
                    {
                        'name': 'out_uri',
                        'value': "{}bam-processing/".format(results_uri)
                    },
                    {
                        'name': 'log_uri',
                        'value': "{}logs/".format(results_uri)
                    }
                ]
            },
        )
        job_ids.append(index_bam_submit["jobId"])

        base_recal_table_submit = boto3.client("batch").submit_job(
            jobName="base_recal_table_{}_{}".format(sample, now),
            jobQueue=job_queue,
            jobDefinition=job_defs["base_recal_table_job"],
            dependsOn=[{"jobId":index_bam_submit["jobId"]}],
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
                        'value': brt_threads
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
                        'value': "{}bam-processing/".format(results_uri)
                    },
                    {
                        'name': 'out_uri',
                        'value': "{}bam-processing/".format(results_uri)
                    },
                    {
                        'name': 'log_uri',
                        'value': "{}logs/".format(results_uri)
                    }
                ]
            },
        )
        job_ids.append(base_recal_table_submit["jobId"])

        base_recal_submit = boto3.client("batch").submit_job(
            jobName="base_recal_{}_{}".format(sample, now),
            jobQueue=job_queue,
            jobDefinition=job_defs["base_recal_job"],
            dependsOn=[{"jobId":base_recal_table_submit["jobId"]}],
            containerOverrides={
                'environment': [
                    {
                        'name': 'build',
                        'value': build
                    },
                    {
                        'name': 'prefix',
                        'value': sample
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
                        'value': "{}bam-processing/".format(results_uri)
                    },
                    {
                        'name': 'out_uri',
                        'value': "{}processed-bams/".format(results_uri)
                    },
                    {
                        'name': 'log_uri',
                        'value': "{}logs/".format(results_uri)
                    }
                ]
            },
        )
        job_ids.append(base_recal_submit["jobId"])

    #Return all job ids for next state in state machines
    return job_ids
