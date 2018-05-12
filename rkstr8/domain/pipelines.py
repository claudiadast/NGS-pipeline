"""Configures the state machines for each phase of the variant-calling pipeline. Each phase has two
designated state machines, both of which are of type "task". One runs the actual task (e.g. HaplotypeCaller),
while the second conducts "polling". This polling mechanism provides a way of monitoring the running task
and not moving onto the next phase until the previous one has completed or exited due to error.""" 

from rkstr8.cloud.stepfunctions import PipelineSpecification, AsyncPoller
from rkstr8.cloud.stepfunctions_mx import *
from rkstr8.cloud.cloudformation import CloudFormationTemplate, Stack
import json


class MultiSampleWGSRefactoredNested(PipelineSpecification):

    def __init__(self, conf):
        super().__init__()

        self.ALIGNMT_SUBMIT_LAMBDA_ARN_TOKEN = 'alignmentSubmitLambdaArn'
        self.ALIGNMT_POLLER_LAMBDA_ARN_TOKEN = 'alignmentPollerLambdaArn'
        self.HAPLO_SUBMIT_LAMBDA_ARN_TOKEN = 'haploSubmitLambdaArn'
        self.HAPLO_POLLER_LAMBDA_ARN_TOKEN = 'haploPollerLambdaArn'
        self.GENO_SUBMIT_LAMBDA_ARN_TOKEN = 'genoSubmitLambdaArn'
        self.GENO_POLLER_LAMBDA_ARN_TOKEN = 'genoPollerLambdaArn'
        self.VQSR_SUBMIT_LAMBDA_ARN_TOKEN = 'vqsrSubmitLambdaArn'
        self.VQSR_POLLER_LAMBDA_ARN_TOKEN = 'vqsrPollerLambdaArn'
        self.TRIO_SUBMIT_LAMBDA_ARN_TOKEN = 'trioSubmitLambdaArn'
        self.TRIO_POLLER_LAMBDA_ARN_TOKEN = 'trioPollerLambdaArn'
        self.conf = conf

    def build_substitutions(self):

        self.mx_substitutions = {
            self.ALIGNMT_SUBMIT_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-AlignmentLambdaFunction'}},
            self.ALIGNMT_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.HAPLO_SUBMIT_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-HaploLambdaFunction'}},
            self.HAPLO_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.GENO_SUBMIT_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-GenoLambdaFunction'}},
            self.GENO_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.VQSR_SUBMIT_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-VQSRLambdaFunction'}},
            self.VQSR_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
        }

        return self.mx_substitutions

    def _cfn_sub_variable(self, name):
        return '${' + name + '}'

    @staticmethod
    def flatten(*state_composites):
        """
        Take in variable number of either stepfunctions_mx.States or lists of them, return flattened list
        :param state_composites:
        :return:
        """
        flattened = []
        for scomp in state_composites:
            if isinstance(scomp, State):
                flattened.append(scomp)
            elif isinstance(scomp, list):
                are_states = [isinstance(item, State) for item in scomp]
                if not all(are_states):
                    raise ValueError('Not every component is a stepfunctions_mx.State')
                flattened.extend(scomp)
        return flattened

    def build_template_params(self):
        parameters = [
            {
                'key': self.conf["RKSTR8_PKG_CFN_PARAM_BUCKET_NAME"],
                'val': self.conf["RKSTR8_PKG_CFN_ARGMT_BUCKET_NAME"]
            },
            {
                'key': self.conf["RKSTR8_PKG_CFN_PARAM_KEY_NAME"],
                'val': self.conf["RKSTR8_PKG_CFN_ARGMT_KEY_NAME"]
            },
            {
                'key': self.conf["ALIGN_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["ALIGN_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["ALIGN_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["ALIGN_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["ALIGN_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["ALIGN_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["HAPLO_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["HAPLO_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["HAPLO_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["HAPLO_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["HAPLO_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["HAPLO_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["GENO_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["GENO_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["GENO_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["GENO_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["GENO_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["GENO_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["VQSR_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["VQSR_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["VQSR_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["VQSR_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["VQSR_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["VQSR_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {

                'key': self.conf["HDOF_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["HDOF_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["HDOF_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["HDOF_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["HDOF_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["HDOF_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["HDOF_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["HDOF_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["HDOF_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["HDOF_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["HDOF_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["HDOF_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["CLOUDSPAN_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["CLOUDSPAN_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["CLOUDSPAN_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["DPROC_CREATE_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["DPROC_CREATE_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["DPROC_CREATE_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["DPROC_CREATE_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["DPROC_CREATE_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["DPROC_CREATE_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["DPROC_SUBMIT_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["DPROC_SUBMIT_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["DPROC_SUBMIT_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["DPROC_SUBMIT_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["DPROC_SUBMIT_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["DPROC_SUBMIT_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["BATCH_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["BATCH_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["BATCH_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["BATCH_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["BATCH_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["BATCH_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': 'StackUID',
                'val': self.conf["STACK_UID"]
            },
            {
                'key': self.conf["CFN_PARAM_GPCE_VPC_ID"],
                'val': self.conf["CFN_ARGMT_GPCE_VPC_ID"]
            },
            {
                'key': self.conf["CFN_PARAM_GPCE_INSTANCE_TYPES"],
                'val': self.conf["CFN_ARGMT_GPCE_INSTANCE_TYPES"]
            },
            {
                'key': self.conf["CFN_PARAM_GPCE_MAX_CPUS"],
                'val': self.conf["CFN_ARGMT_GPCE_MAX_CPUS"]
            },
            {
                'key': self.conf["CFN_PARAM_GPCE_SSH_KEY_PAIR"],
                'val': self.conf["CFN_ARGMT_GPCE_SSH_KEY_PAIR"]
            },
            {
                'key': self.conf["LAMBDA_CFN_PARAM_TEMPLATE_URL"],
                'val': self.conf["LAMBDA_CFN_ARGMT_TEMPLATE_URL"]
            },
            {
                'key': self.conf["NETWORK_CFN_PARAM_TEMPLATE_URL"],
                'val': self.conf["NETWORK_CFN_ARGMT_TEMPLATE_URL"]
            },
            {
                'key': self.conf["BATCH_CFN_PARAM_TEMPLATE_URL"],
                'val': self.conf["BATCH_CFN_ARGMT_TEMPLATE_URL"]
            },
            {
                'key': self.conf["STEP_FUNCTIONS_PARAM_TEMPLATE_URL"],
                'val': self.conf["STEP_FUNCTIONS_ARGMT_TEMPLATE_URL"]
            }
        ]

        # Cast all parameters to string, boto can only create params from `str`
        str_parameters = [
            {
                'key': p['key'],
                'val': str(p['val'])
            } for p in parameters]
            
        # Render the parameters above into format expected by boto.cloudformation.create_stack
        return [CloudFormationTemplate.Parameter(**p).to_cfn() for p in str_parameters]

    def build_machine(self):
        """
        Holds the machine definition, or otherwise gets that definition, and returns the built machine.
        Also setting self.mx to the built machine.

        :return:
        """
        MACHINE_NAME = 'MultiSampleWGS'

        ALIGNMT_SUBMIT = 'AlignmentSubmitTask'
        ALIGNMT_POLLER = 'AlignmentPollerTask'

        HAPLO_SUBMIT = 'HaploSubmitTask'
        HAPLO_POLLER = 'HaploPollerTask'

        GENO_SUBMIT = 'GenoSubmitTask'
        GENO_POLLER = 'GenoPollerTask'

        VQSR_SUBMIT = 'VQSRSubmitTask'
        VQSR_POLLER = 'VQSRPollerTask'

        ALIGNMT_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.ALIGNMT_SUBMIT_LAMBDA_ARN_TOKEN)
        ALIGNMT_POLLER_ARN_VAR = self._cfn_sub_variable(self.ALIGNMT_POLLER_LAMBDA_ARN_TOKEN)

        HAPLO_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.HAPLO_SUBMIT_LAMBDA_ARN_TOKEN)
        HAPLO_POLLER_ARN_VAR = self._cfn_sub_variable(self.HAPLO_POLLER_LAMBDA_ARN_TOKEN)

        GENO_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.GENO_SUBMIT_LAMBDA_ARN_TOKEN)
        GENO_POLLER_ARN_VAR = self._cfn_sub_variable(self.GENO_POLLER_LAMBDA_ARN_TOKEN)

        VQSR_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.VQSR_SUBMIT_LAMBDA_ARN_TOKEN)
        VQSR_POLLER_ARN_VAR = self._cfn_sub_variable(self.VQSR_POLLER_LAMBDA_ARN_TOKEN)

        ALIGNMT_POLLER_STATUS_PATH = '$.alignment_poll_status'
        HAPLO_POLLER_STATUS_PATH = '$.haplo_poll_status'
        GENO_POLLER_STATUS_PATH = '$.geno_poll_status'
        VQSR_POLLER_STATUS_PATH = '$.vqsr_poll_status'

        ALIGNMT_JOB_IDS_PATH = '$.alignment_job_ids'
        HAPLO_JOB_IDS_PATH = '$.haplo_job_ids'
        GENO_JOB_IDS_PATH = '$.geno_job_ids'
        VQSR_JOB_IDS_PATH = '$.vqsr_job_ids'


        """State machines are defined below for each pipeline phase"""

        self.mx = StateMachine(
            name=MACHINE_NAME,
            start=ALIGNMT_SUBMIT,
            states=States(
                *self.flatten(
                    AsyncPoller(
                        async_task=Task(
                            name=ALIGNMT_SUBMIT,
                            resource=ALIGNMT_SUBMIT_ARN_VAR,
                            result_path=ALIGNMT_JOB_IDS_PATH,
                            next=ALIGNMT_POLLER
                        ),
                        pollr_task=Task(
                            name=ALIGNMT_POLLER,
                            resource=ALIGNMT_POLLER_ARN_VAR,
                            input_path=ALIGNMT_JOB_IDS_PATH,
                            result_path=ALIGNMT_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='AlignmentProcessingFailed'
                        ),
                        succd_task=HAPLO_SUBMIT,
                        stats_path=ALIGNMT_POLLER_STATUS_PATH,
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    AsyncPoller(
                        stats_path=HAPLO_POLLER_STATUS_PATH,
                        async_task=Task(
                            name=HAPLO_SUBMIT,
                            resource=HAPLO_SUBMIT_ARN_VAR,
                            result_path=HAPLO_JOB_IDS_PATH,
                            next=HAPLO_POLLER
                        ),
                        pollr_task=Task(
                            name=HAPLO_POLLER,
                            resource=HAPLO_POLLER_ARN_VAR,
                            input_path=HAPLO_JOB_IDS_PATH,
                            result_path=HAPLO_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='HaploProcessingFailed'
                        ),
                        succd_task=GENO_SUBMIT,
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    AsyncPoller(
                        stats_path=GENO_POLLER_STATUS_PATH,
                        async_task=Task(
                            name=GENO_SUBMIT,
                            resource=GENO_SUBMIT_ARN_VAR,
                            result_path=GENO_JOB_IDS_PATH,
                            next=GENO_POLLER
                        ),
                        pollr_task=Task(
                            name=GENO_POLLER,
                            resource=GENO_POLLER_ARN_VAR,
                            input_path=GENO_JOB_IDS_PATH,
                            result_path=GENO_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='GenoProcessingFailed'
                        ),
                        succd_task=VQSR_SUBMIT,
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    AsyncPoller(
                        stats_path=VQSR_POLLER_STATUS_PATH,
                        async_task=Task(
                            name=VQSR_SUBMIT,
                            resource=VQSR_SUBMIT_ARN_VAR,
                            result_path=VQSR_JOB_IDS_PATH,
                            next=VQSR_POLLER
                        ),
                        pollr_task=Task(
                            name=VQSR_POLLER,
                            resource=VQSR_POLLER_ARN_VAR,
                            input_path=VQSR_JOB_IDS_PATH,
                            result_path=VQSR_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='VQSRProcessingFailed'
                        ),
                        succd_task='PipelineSucceeded',
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    Succeed(name='PipelineSucceeded')
                )
            )
        )

        return self.mx

    def get_nested_batch_stack_hack(self, parent_stack):
        batch_stack_name = parent_stack.Resource("BatchResourcesStack").physical_resource_id.split("/")[1]
        print('Batch stack name: {}'.format(batch_stack_name))
        batch_stack = Stack.from_stack_name(stack_name=batch_stack_name).stack
        print('Batch stack type: {}'.format(type(batch_stack)))
        return batch_stack

    def build_input(self, stack):
        """
        The following are input objects are provided to the state machines above.

        :param stack: boto3.CloudFormation.Stack
        :return: dictionary structure representing json object to be passed as input to machine execution
        """

        stack = self.get_nested_batch_stack_hack(parent_stack=stack)

        def get_phys_resource_id(resource):
            return stack.Resource(resource).physical_resource_id.split("/")[-1]

        queue_physical_resource_id = get_phys_resource_id("GeneralPurposeQueue")

        # Alignment and alignment processing job defs
        bwa_job_def = get_phys_resource_id("bwamemJobDef")
        sort_sam_job_def = get_phys_resource_id("sortsamJobDef")
        mark_dups_job_def = get_phys_resource_id("markdupsJobDef")
        index_bam_job_def = get_phys_resource_id("indexbamJobDef")
        base_recal_table_job_def = get_phys_resource_id("baserecaltableJobDef")
        base_recal_job_def = get_phys_resource_id("baserecalJobDef")

        # Variant calling and genotyping job defs
        sentieon_haplotyper_job_def = get_phys_resource_id("sentieonhaplotyperJobDef")
        sentieon_genotyper_job_def = get_phys_resource_id("sentieongenotyperJobDef")

        # VQSR job defs
        vqsr_snp_model_job_def = get_phys_resource_id("vqsrsnpmodelJobDef")
        vqsr_snp_apply_job_def = get_phys_resource_id("vqsrsnpapplyJobDef")
        vqsr_indel_model_job_def = get_phys_resource_id("vqsrindelmodelJobDef")
        vqsr_indel_apply_job_def = get_phys_resource_id("vqsrindelapplyJobDef")

        # Interpret the ref bucket to use depending on user input
        ref_uri = self.conf["REF_BUCKET_URI"]
        in_uri = self.conf["INPUT_PREFIX_URI"]
        results_uri = self.conf["OUTPUT_PREFIX_URI"]
        samples = self.conf["SAMPLE_ID_LIST"]
        cohort_prefix = self.conf["COHORT_LABEL"]
        test_cohort = self.conf["VQSR_TEST_COHORT_KEY"]
        test_in_uri = self.conf["VQSR_TEST_DATA_URI_PREFIX"]
        suffix = self.conf["FASTQ_SUFFIX"]
        mode = self.conf["MODE"]
        build = self.conf["BUILD"]
        ome = self.conf["OME"]
        target_file = self.conf["TARGET_FILE_NAME"]
        sentieon_pkg = self.conf["SENTIEON_PACKAGE_NAME"]


        # Check if license_uri has prefix, join appropriately
        if self.conf["SENTIEON_S3_KEY_PREFIX"] != "":
            license_uri = '/'.join(('s3:/',
                                    self.conf["SENTIEON_S3_BUCKET"],
                                    self.conf["SENTIEON_S3_KEY_PREFIX"],
                                    self.conf["SENTIEON_LICENSE_FILE_NAME"]))
        else:
            license_uri = '/'.join(('s3:/',
                                    self.conf["SENTIEON_S3_BUCKET"],
                                    self.conf["SENTIEON_LICENSE_FILE_NAME"]))
        if self.conf["PIPELINE_CMD_TOOL_PARAM_S3_KEY_PREFIX"] != "":
            param_uri = '/'.join(('s3:/',
                                  self.conf["PIPELINE_CMD_TOOL_PARAM_S3_BUCKET"],
                                  self.conf["PIPELINE_CMD_TOOL_PARAM_S3_KEY_PREFIX"],
                                  self.conf["PIPELINE_CMD_TOOL_PARAM_FILE"]))
        else:
            param_uri = '/'.join(('s3:/',
                                  self.conf["PIPELINE_CMD_TOOL_PARAM_S3_BUCKET"],
                                  self.conf["PIPELINE_CMD_TOOL_PARAM_FILE"]))

        #
        # Mode argument is one of ['test', 'prod'], this is only checked in vqsr.py
        #

        self.mx_input = {
            'samples': samples,
            'suffix' : suffix,
            'cohort_prefix': cohort_prefix,
            'build': build,
            #
            # For testing modes
            #
            'mode' : {
                'label':mode,
                'ome':ome,
                'test': {
                    'threads': {
                        'bwa' : 8,
                        'brt' : 4,
                        'hap' : 8
                    }

                },
                'prod': {
                    'threads': {
                        'bwa' : 36,
                        'brt' : 16,
                        'hap' : 36
                    }

                }
            },
            'target_file': target_file,
            'test_in_uri': test_in_uri,
            'test_cohort': test_cohort,
            'in_uri': in_uri,
            'results_uri': results_uri,
            'ref_uri': ref_uri,
            'license_uri' : license_uri,
            'sentieon_pkg' : sentieon_pkg,
            'param_uri': param_uri,
            'queue': queue_physical_resource_id,
            'job_defs': {
                'bwa_mem_job': bwa_job_def,
                'sort_sam_job': sort_sam_job_def,
                'mark_dups_job': mark_dups_job_def,
                'index_bam_job': index_bam_job_def,
                'base_recal_table_job': base_recal_table_job_def,
                'base_recal_job': base_recal_job_def,
                'sentieon_haplotyper_job': sentieon_haplotyper_job_def,
                'sentieon_genotyper_job': sentieon_genotyper_job_def,
                'vqsr_snp_model_job' : vqsr_snp_model_job_def,
                'vqsr_snp_apply_job' : vqsr_snp_apply_job_def,
                'vqsr_indel_model_job' : vqsr_indel_model_job_def,
                'vqsr_indel_apply_job' : vqsr_indel_apply_job_def
            }
        }

        return self.mx_input


class Validation(PipelineSpecification):

    def __init__(self, conf):
        super().__init__()
        self.ALIGNMT_SUBMIT_LAMBDA_ARN_TOKEN = 'alignmentSubmitLambdaArn'
        self.ALIGNMT_POLLER_LAMBDA_ARN_TOKEN = 'alignmentPollerLambdaArn'
        self.HAPLO_SUBMIT_LAMBDA_ARN_TOKEN = 'haploSubmitLambdaArn'
        self.HAPLO_POLLER_LAMBDA_ARN_TOKEN = 'haploPollerLambdaArn'
        self.GENO_SUBMIT_LAMBDA_ARN_TOKEN = 'genoSubmitLambdaArn'
        self.GENO_POLLER_LAMBDA_ARN_TOKEN = 'genoPollerLambdaArn'
        self.VQSR_SUBMIT_LAMBDA_ARN_TOKEN = 'vqsrSubmitLambdaArn'
        self.VQSR_POLLER_LAMBDA_ARN_TOKEN = 'vqsrPollerLambdaArn'
        self.TRIO_SUBMIT_LAMBDA_ARN_TOKEN = 'trioSubmitLambdaArn'
        self.TRIO_POLLER_LAMBDA_ARN_TOKEN = 'trioPollerLambdaArn'
        self.HDOF_SUBMIT_LAMBDA_ARN_TOKEN = 'handoffSubmitLambdaArn'
        self.HDOF_POLLER_LAMBDA_ARN_TOKEN = 'handoffPollerLambdaArn'
        self.CLOUDSPAN_SUBMIT_LAMBDA_ARN_TOKEN = 'cloudspanLambdaArn'
        self.DPROC_CREATE_POLLER_LAMBDA_ARN_TOKEN = 'dataprocCreatePollerLambdaArn'
        self.DPROC_SUBMIT_POLLER_LAMBDA_ARN_TOKEN = 'dataprocSubmitPollerLambdaArn'
        self.conf = conf

    def build_substitutions(self):

        self.mx_substitutions = {
            self.ALIGNMT_SUBMIT_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-AlignmentLambdaFunction'}},
            self.ALIGNMT_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.HAPLO_SUBMIT_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-HaploLambdaFunction'}},
            self.HAPLO_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.GENO_SUBMIT_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-GenoLambdaFunction'}},
            self.GENO_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.VQSR_SUBMIT_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-VQSRLambdaFunction'}},
            self.VQSR_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.HDOF_SUBMIT_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-HandoffLambdaFunction'}},
            self.HDOF_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-HandoffPollerLambdaFunction'}},
            self.CLOUDSPAN_SUBMIT_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-CloudspanLambdaFunction'}},
            self.DPROC_CREATE_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-DataprocCreatePollerLambdaFunction'}},
            self.DPROC_SUBMIT_POLLER_LAMBDA_ARN_TOKEN: {'Fn::ImportValue': {'Fn::Sub': '${StackUID}-DataprocSubmitPollerLambdaFunction'}},
        }

        return self.mx_substitutions

    def _cfn_sub_variable(self, name):
        return '${' + name + '}'

    @staticmethod
    def flatten(*state_composites):
        """
        Take in variable number of either stepfunctions_mx.States or lists of them, return flattened list
        :param state_composites:
        :return:
        """
        flattened = []
        for scomp in state_composites:
            if isinstance(scomp, State):
                flattened.append(scomp)
            elif isinstance(scomp, list):
                are_states = [isinstance(item, State) for item in scomp]
                if not all(are_states):
                    raise ValueError('Not every component is a stepfunctions_mx.State')
                flattened.extend(scomp)
        return flattened

    def build_template_params(self):
        parameters = [
            {
                'key': self.conf["RKSTR8_PKG_CFN_PARAM_BUCKET_NAME"],
                'val': self.conf["RKSTR8_PKG_CFN_ARGMT_BUCKET_NAME"]
            },
            {
                'key': self.conf["RKSTR8_PKG_CFN_PARAM_KEY_NAME"],
                'val': self.conf["RKSTR8_PKG_CFN_ARGMT_KEY_NAME"]
            },
            {
                'key': self.conf["ALIGN_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["ALIGN_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["ALIGN_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["ALIGN_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["ALIGN_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["ALIGN_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["HAPLO_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["HAPLO_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["HAPLO_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["HAPLO_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["HAPLO_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["HAPLO_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["GENO_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["GENO_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["GENO_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["GENO_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["GENO_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["GENO_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["VQSR_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["VQSR_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["VQSR_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["VQSR_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["VQSR_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["VQSR_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["HDOF_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["HDOF_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["HDOF_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["HDOF_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["HDOF_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["HDOF_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["CLOUDSPAN_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["CLOUDSPAN_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["CLOUDSPAN_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["HDOF_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["HDOF_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["HDOF_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["HDOF_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["HDOF_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["HDOF_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["DPROC_CREATE_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["DPROC_CREATE_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["DPROC_CREATE_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["DPROC_CREATE_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["DPROC_CREATE_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["DPROC_CREATE_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["DPROC_SUBMIT_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["DPROC_SUBMIT_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["DPROC_SUBMIT_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["DPROC_SUBMIT_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["DPROC_SUBMIT_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["DPROC_SUBMIT_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["BATCH_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["BATCH_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["BATCH_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["BATCH_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["BATCH_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["BATCH_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': 'StackUID',
                'val': self.conf["STACK_UID"]
            },
            {
                'key': self.conf["CFN_PARAM_GPCE_VPC_ID"],
                'val': self.conf["CFN_ARGMT_GPCE_VPC_ID"]
            },
            {
                'key': self.conf["CFN_PARAM_GPCE_INSTANCE_TYPES"],
                'val': self.conf["CFN_ARGMT_GPCE_INSTANCE_TYPES"]
            },
            {
                'key': self.conf["CFN_PARAM_GPCE_SSH_KEY_PAIR"],
                'val': self.conf["CFN_ARGMT_GPCE_SSH_KEY_PAIR"]
            },
            {
                'key': self.conf["LAMBDA_CFN_PARAM_TEMPLATE_URL"],
                'val': self.conf["LAMBDA_CFN_ARGMT_TEMPLATE_URL"]
            },
            {
                'key': self.conf["NETWORK_CFN_PARAM_TEMPLATE_URL"],
                'val': self.conf["NETWORK_CFN_ARGMT_TEMPLATE_URL"]
            },
            {
                'key': self.conf["BATCH_CFN_PARAM_TEMPLATE_URL"],
                'val': self.conf["BATCH_CFN_ARGMT_TEMPLATE_URL"]
            },
            {
                'key': self.conf["STEP_FUNCTIONS_PARAM_TEMPLATE_URL"],
                'val': self.conf["STEP_FUNCTIONS_ARGMT_TEMPLATE_URL"]
            }
        ]

        # Cast all parameters to string, boto can only create params from `str`
        str_parameters = [
            {
                'key': p['key'],
                'val': str(p['val'])
            } for p in parameters]
            
        # Render the parameters above into format expected by boto.cloudformation.create_stack
        return [CloudFormationTemplate.Parameter(**p).to_cfn() for p in str_parameters]

    def build_machine(self):
        """
        Holds the machine definition, or otherwise gets that definition, and returns the built machine.
        Also setting self.mx to the built machine.

        :return:
        """
        MACHINE_NAME = 'Validation'

        ALIGNMT_SUBMIT = 'AlignmentSubmitTask'
        ALIGNMT_POLLER = 'AlignmentPollerTask'

        HAPLO_SUBMIT = 'HaploSubmitTask'
        HAPLO_POLLER = 'HaploPollerTask'

        GENO_SUBMIT = 'GenoSubmitTask'
        GENO_POLLER = 'GenoPollerTask'

        VQSR_SUBMIT = 'VQSRSubmitTask'
        VQSR_POLLER = 'VQSRPollerTask'

        HDOF_SUBMIT = 'HandoffSubmitTask'
        HDOF_POLLER = 'HandoffPollerTask'

        DPROC_CREATE = 'ClusterCreateTask'
        DPROC_CREATE_POLLER = 'ClusterCreatePollerTask'

        DPROC_SUBMIT = 'ValidationSubmitTask'
        DPROC_SUBMIT_POLLER = 'ValidationPollerTask'

        ALIGNMT_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.ALIGNMT_SUBMIT_LAMBDA_ARN_TOKEN)
        ALIGNMT_POLLER_ARN_VAR = self._cfn_sub_variable(self.ALIGNMT_POLLER_LAMBDA_ARN_TOKEN)

        HAPLO_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.HAPLO_SUBMIT_LAMBDA_ARN_TOKEN)
        HAPLO_POLLER_ARN_VAR = self._cfn_sub_variable(self.HAPLO_POLLER_LAMBDA_ARN_TOKEN)

        GENO_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.GENO_SUBMIT_LAMBDA_ARN_TOKEN)
        GENO_POLLER_ARN_VAR = self._cfn_sub_variable(self.GENO_POLLER_LAMBDA_ARN_TOKEN)

        VQSR_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.VQSR_SUBMIT_LAMBDA_ARN_TOKEN)
        VQSR_POLLER_ARN_VAR = self._cfn_sub_variable(self.VQSR_POLLER_LAMBDA_ARN_TOKEN)

        HDOF_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.HDOF_SUBMIT_LAMBDA_ARN_TOKEN)
        HDOF_POLLER_ARN_VAR = self._cfn_sub_variable(self.HDOF_POLLER_LAMBDA_ARN_TOKEN)        

        DPROC_CREATE_ARN_VAR = self._cfn_sub_variable(self.CLOUDSPAN_SUBMIT_LAMBDA_ARN_TOKEN)
        DPROC_CREATE_POLLER_ARN_VAR = self._cfn_sub_variable(self.DPROC_CREATE_POLLER_LAMBDA_ARN_TOKEN)

        DPROC_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.CLOUDSPAN_SUBMIT_LAMBDA_ARN_TOKEN)
        DPROC_SUBMIT_POLLER_ARN_VAR = self._cfn_sub_variable(self.DPROC_SUBMIT_POLLER_LAMBDA_ARN_TOKEN)


        ALIGNMT_POLLER_STATUS_PATH = '$.alignment_poll_status'
        HAPLO_POLLER_STATUS_PATH = '$.haplo_poll_status'
        GENO_POLLER_STATUS_PATH = '$.geno_poll_status'
        VQSR_POLLER_STATUS_PATH = '$.vqsr_poll_status'
        HDOF_POLLER_STATUS_PATH = '$.handoff_poll_status'

        DPROC_CREATE_POLLER_STATUS_PATH = '$.dproc_create_poll_status'
        DPROC_CREATE_INPUT_PATH = '$.dataproc_create'
        DPROC_CREATE_RESULT_PATH = '$.dataproc_create.response'

        DPROC_SUBMIT_POLLER_STATUS_PATH = '$.dproc_submit_poll_status'
        DPROC_SUBMIT_INPUT_PATH = '$.dataproc_submit'
        DPROC_SUBMIT_RESULT_PATH = '$.dataproc_submit.response'

        ALIGNMT_JOB_IDS_PATH = '$.alignment_job_ids'
        HAPLO_JOB_IDS_PATH = '$.haplo_job_ids'
        GENO_JOB_IDS_PATH = '$.geno_job_ids'
        VQSR_JOB_IDS_PATH = '$.vqsr_job_ids'
        HDOF_RESULT_PATH = '$.handoff_submit'


        """State machines are defined below for each pipeline phase"""

        self.mx = StateMachine(
            name=MACHINE_NAME,
            start=ALIGNMT_SUBMIT,
            states=States(
                *self.flatten(
                    AsyncPoller(
                        async_task=Task(
                            name=ALIGNMT_SUBMIT,
                            resource=ALIGNMT_SUBMIT_ARN_VAR,
                            result_path=ALIGNMT_JOB_IDS_PATH,
                            next=ALIGNMT_POLLER
                        ),
                        pollr_task=Task(
                            name=ALIGNMT_POLLER,
                            resource=ALIGNMT_POLLER_ARN_VAR,
                            input_path=ALIGNMT_JOB_IDS_PATH,
                            result_path=ALIGNMT_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='AlignmentProcessingFailed'
                        ),
                        succd_task=HAPLO_SUBMIT,
                        stats_path=ALIGNMT_POLLER_STATUS_PATH,
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    AsyncPoller(
                        stats_path=HAPLO_POLLER_STATUS_PATH,
                        async_task=Task(
                            name=HAPLO_SUBMIT,
                            resource=HAPLO_SUBMIT_ARN_VAR,
                            result_path=HAPLO_JOB_IDS_PATH,
                            next=HAPLO_POLLER
                        ),
                        pollr_task=Task(
                            name=HAPLO_POLLER,
                            resource=HAPLO_POLLER_ARN_VAR,
                            input_path=HAPLO_JOB_IDS_PATH,
                            result_path=HAPLO_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='HaploFailed'
                        ),
                        succd_task=GENO_SUBMIT,
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    AsyncPoller(
                        stats_path=GENO_POLLER_STATUS_PATH,
                        async_task=Task(
                            name=GENO_SUBMIT,
                            resource=GENO_SUBMIT_ARN_VAR,
                            result_path=GENO_JOB_IDS_PATH,
                            next=GENO_POLLER
                        ),
                        pollr_task=Task(
                            name=GENO_POLLER,
                            resource=GENO_POLLER_ARN_VAR,
                            input_path=GENO_JOB_IDS_PATH,
                            result_path=GENO_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='GenoFailed'
                        ),
                        succd_task=VQSR_SUBMIT,
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    AsyncPoller(
                        stats_path=VQSR_POLLER_STATUS_PATH,
                        async_task=Task(
                            name=VQSR_SUBMIT,
                            resource=VQSR_SUBMIT_ARN_VAR,
                            result_path=VQSR_JOB_IDS_PATH,
                            next=VQSR_POLLER
                        ),
                        pollr_task=Task(
                            name=VQSR_POLLER,
                            resource=VQSR_POLLER_ARN_VAR,
                            input_path=VQSR_JOB_IDS_PATH,
                            result_path=VQSR_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='VQSRFailed'
                        ),
                        succd_task=HDOF_SUBMIT,
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    AsyncPoller(
                        stats_path=HDOF_POLLER_STATUS_PATH,
                        async_task=Task(
                            name=HDOF_SUBMIT,
                            resource=HDOF_SUBMIT_ARN_VAR,
                            result_path=HDOF_RESULT_PATH,
                            next=HDOF_POLLER
                        ),
                        pollr_task=Task(
                            name=HDOF_POLLER,
                            resource=HDOF_POLLER_ARN_VAR,
                            input_path=HDOF_RESULT_PATH,
                            result_path=HDOF_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='HandoffFailed'
                        ),
                        succd_task=DPROC_CREATE,
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    AsyncPoller(
                        stats_path=DPROC_CREATE_POLLER_STATUS_PATH,
                        async_task=Task(
                            name=DPROC_CREATE,
                            resource=DPROC_CREATE_ARN_VAR,
                            input_path=DPROC_CREATE_INPUT_PATH,
                            result_path=DPROC_CREATE_RESULT_PATH,
                            next=DPROC_CREATE_POLLER
                        ),
                        pollr_task=Task(
                            name=DPROC_CREATE_POLLER,
                            resource=DPROC_CREATE_POLLER_ARN_VAR,
                            input_path=DPROC_CREATE_RESULT_PATH,
                            result_path=DPROC_CREATE_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='ClusterCreationFailed'
                        ),
                        succd_task=DPROC_SUBMIT,
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    AsyncPoller(
                        stats_path=DPROC_SUBMIT_POLLER_STATUS_PATH,
                        async_task=Task(
                            name=DPROC_SUBMIT,
                            resource=DPROC_SUBMIT_ARN_VAR,
                            input_path=DPROC_SUBMIT_INPUT_PATH,
                            result_path=DPROC_SUBMIT_RESULT_PATH,
                            next=DPROC_SUBMIT_POLLER
                        ),
                        pollr_task=Task(
                            name=DPROC_SUBMIT_POLLER,
                            resource=DPROC_SUBMIT_POLLER_ARN_VAR,
                            input_path=DPROC_SUBMIT_RESULT_PATH,
                            result_path=DPROC_SUBMIT_POLLER_STATUS_PATH
                        ),
                        faild_task=Fail(
                            name='ValidationFailed'
                        ),
                        succd_task='ValidationSucceeded',
                        pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
                    ).states(),
                    Succeed(name='ValidationSucceeded')
                )
            )
        )

        return self.mx

    def get_nested_child_stack_hack(self, parent_stack):
        batch_stack_name = parent_stack.Resource("BatchResourcesStack").physical_resource_id.split("/")[1]
        print('Batch stack name: {}'.format(batch_stack_name))
        batch_stack = Stack.from_stack_name(stack_name=batch_stack_name).stack
        print('Batch stack type: {}'.format(type(batch_stack)))
      
        lambda_stack_name = parent_stack.Resource("LambdaResourcesStack").physical_resource_id.split("/")[1]
        print('Lambda stack name: {}'.format(lambda_stack_name))
        lambda_stack = Stack.from_stack_name(stack_name=lambda_stack_name).stack
        print('Lambda stack type: {}'.format(type(lambda_stack)))
        return batch_stack,lambda_stack


    def build_input(self, stack):
        """
        The following are input objects are provided to the state machines above.

        :param stack: boto3.CloudFormation.Stack
        :return: dictionary structure representing json object to be passed as input to machine execution
        """

        batch_stack, lambda_stack = self.get_nested_child_stack_hack(parent_stack=stack)

        def get_batch_phys_resource_id(resource):
            return batch_stack.Resource(resource).physical_resource_id.split("/")[-1]

        queue_physical_resource_id = get_batch_phys_resource_id("GeneralPurposeQueue")

        # Alignment and alignment processing job defs
        bwa_job_def = get_batch_phys_resource_id("bwamemJobDef")
        sort_sam_job_def = get_batch_phys_resource_id("sortsamJobDef")
        mark_dups_job_def = get_batch_phys_resource_id("markdupsJobDef")
        index_bam_job_def = get_batch_phys_resource_id("indexbamJobDef")
        base_recal_table_job_def = get_batch_phys_resource_id("baserecaltableJobDef")
        base_recal_job_def = get_batch_phys_resource_id("baserecalJobDef")

        # Variant calling and genotyping job defs
        sentieon_haplotyper_job_def = get_batch_phys_resource_id("sentieonhaplotyperJobDef")
        sentieon_genotyper_job_def = get_batch_phys_resource_id("sentieongenotyperJobDef")

        # VQSR job defs
        vqsr_snp_model_job_def = get_batch_phys_resource_id("vqsrsnpmodelJobDef")
        vqsr_snp_apply_job_def = get_batch_phys_resource_id("vqsrsnpapplyJobDef")
        vqsr_indel_model_job_def = get_batch_phys_resource_id("vqsrindelmodelJobDef")
        vqsr_indel_apply_job_def = get_batch_phys_resource_id("vqsrindelapplyJobDef")

        # Interpret the ref bucket to use depending on user input
        ref_uri = self.conf["REF_BUCKET_URI"]
        in_uri = self.conf["INPUT_PREFIX_URI"]
        results_uri = self.conf["OUTPUT_PREFIX_URI"]
        samples = self.conf["SAMPLE_ID_LIST"]
        cohort_prefix = self.conf["COHORT_LABEL"]
        test_cohort = self.conf["VQSR_TEST_COHORT_KEY"]
        test_in_uri = self.conf["VQSR_TEST_DATA_URI_PREFIX"]
        suffix = self.conf["FASTQ_SUFFIX"]
        mode = self.conf["MODE"]
        build = self.conf["BUILD"]
        ome = self.conf["OME"]
        target_file = self.conf["TARGET_FILE_NAME"]
        sentieon_pkg = self.conf["SENTIEON_PACKAGE_NAME"]

        access_key_id = self.conf["ACCESS_KEY_ID"]
        secret_access_key = self.conf["SECRET_ACCESS_KEY"]

        # Handoff params

        cloud_transfer_outbucket = self.conf["CLOUD_TRANSFER_OUTBUCKET"]
        access_key_id = self.conf["ACCESS_KEY_ID"]
        secret_access_key = self.conf["SECRET_ACCESS_KEY"]

        # Google cloud params

        #Load json file as a python dict
        with open(self.conf["GCP_CREDS"]) as fh:
            gcp_credentials = json.loads(fh.read())          

        giab_bucket = self.conf["GIAB_BUCKET"]
        resource_id = self.conf["CLUSTER_NAME"]
        project_id = self.conf["PROJECT_ID"]
        zone = self.conf["ZONE"]

        master_type = self.conf["MASTER_TYPE"]
        worker_type = self.conf["WORKER_TYPE"]
        preempt_type = self.conf["WORKER_TYPE"]

        num_masters = self.conf["NUM_MASTERS"]
        num_workers = self.conf["NUM_WORKERS"]
        num_preempt = self.conf["NUM_PREEMPT"]

        master_boot_disk_size = self.conf["MASTER_BOOT_DISK_SIZE"]
        worker_boot_disk_size = self.conf["WORKER_BOOT_DISK_SIZE"]
        preempt_boot_disk_size = self.conf["PREEMPT_BOOT_DISK_SIZE"]

        master_num_ssds = self.conf["MASTER_NUM_SSDS"]
        worker_num_ssds = self.conf["WORKER_NUM_SSDS"]
        preempt_num_ssds = self.conf["PREEMPT_NUM_SSDS"]

        hail_script_bucket = self.conf["HAIL_SCRIPT_BUCKET"]
        hail_script_key = self.conf["HAIL_SCRIPT_KEY"]

        hail_hash = self.conf["HAIL_HASH"]
        spark_version = self.conf["SPARK_VERSION"]
        hail_version = self.conf["HAIL_VERSION"]
        
        # Check if license_uri has prefix, join appropriately
        if self.conf["SENTIEON_S3_KEY_PREFIX"] != "":
            license_uri = '/'.join(('s3:/',
                                    self.conf["SENTIEON_S3_BUCKET"],
                                    self.conf["SENTIEON_S3_KEY_PREFIX"],
                                    self.conf["SENTIEON_LICENSE_FILE_NAME"]))
        else:
            license_uri = '/'.join(('s3:/',
                                    self.conf["SENTIEON_S3_BUCKET"],
                                    self.conf["SENTIEON_LICENSE_FILE_NAME"]))
        if self.conf["PIPELINE_CMD_TOOL_PARAM_S3_KEY_PREFIX"] != "":
            param_uri = '/'.join(('s3:/',
                                  self.conf["PIPELINE_CMD_TOOL_PARAM_S3_BUCKET"],
                                  self.conf["PIPELINE_CMD_TOOL_PARAM_S3_KEY_PREFIX"],
                                  self.conf["PIPELINE_CMD_TOOL_PARAM_FILE"]))
        else:
            param_uri = '/'.join(('s3:/',
                                  self.conf["PIPELINE_CMD_TOOL_PARAM_S3_BUCKET"],
                                  self.conf["PIPELINE_CMD_TOOL_PARAM_FILE"]))

        #
        # Mode argument is one of ['test', 'prod'], this is only checked in vqsr.py
        #

        self.mx_input = {
            'cohort_prefix': cohort_prefix,
            'results_uri': results_uri,
            'queue': queue_physical_resource_id,
            'samples': samples,
            'suffix' : suffix,
            'build': build,
            #
            # For testing modes
            #
            'mode' : {
                'label':mode,
                'ome':ome,
                'test': {
                    'threads': {
                        'bwa' : 8,
                        'brt' : 4,
                        'hap' : 8
                    }

                },
                'prod': {
                    'threads': {
                        'bwa' : 36,
                        'brt' : 16,
                        'hap' : 36
                    }

                }
            },
            'target_file': target_file,
            'test_in_uri': test_in_uri,
            'test_cohort': test_cohort,
            'in_uri': in_uri,
            'ref_uri': ref_uri,
            'license_uri' : license_uri,
            'sentieon_pkg' : sentieon_pkg,
            'param_uri': param_uri,
            'job_defs': {
                'bwa_mem_job': bwa_job_def,
                'sort_sam_job': sort_sam_job_def,
                'mark_dups_job': mark_dups_job_def,
                'index_bam_job': index_bam_job_def,
                'base_recal_table_job': base_recal_table_job_def,
                'base_recal_job': base_recal_job_def,
                'sentieon_haplotyper_job': sentieon_haplotyper_job_def,
                'sentieon_genotyper_job': sentieon_genotyper_job_def,
                'vqsr_snp_model_job' : vqsr_snp_model_job_def,
                'vqsr_snp_apply_job' : vqsr_snp_apply_job_def,
                'vqsr_indel_model_job' : vqsr_indel_model_job_def,
                'vqsr_indel_apply_job' : vqsr_indel_apply_job_def
            },
            #
            # Handoff
            #
            'project_id' : project_id,
            'sink_bucket': cloud_transfer_outbucket,
            'aws_access_key': access_key_id,
            'aws_secret_key': secret_access_key,
            'GCP_creds': gcp_credentials,
            #
            # Dataproc Create
            #
            'dataproc_create': {
                'gcp-administrative': {
                    'project': project_id,
                    'zone': zone,
                    'credentials': gcp_credentials,
                },
                'dataproc-administrative': {
                    'cluster_name': resource_id,
                    'image_version': '1.1' if spark_version == "2.0.2" else '1.2',
                    'masterConfig': {
                        'master_type': master_type,
                        'num_masters': num_masters,
                        'boot_disk_size': master_boot_disk_size,
                        'num_local_ssds': master_num_ssds
                    },
                    'workerConfig': {
                        'worker_type': worker_type,
                        'num_workers': num_workers,
                        'boot_disk_size': worker_boot_disk_size,
                        'num_local_ssds': worker_num_ssds
                    },
                    'preemptConfig': {
                        'preempt_type': preempt_type,
                        'num_preempt': num_preempt,
                        'boot_disk_size': preempt_boot_disk_size,
                        'num_local_ssds': preempt_num_ssds
                    }
                },
                'hail-administrative': {
                    'hail_control': {
                        'hail_command': 'create',
                        'resource_id': resource_id
                    },
                    'script': {
                        'gs_bucket': hail_script_bucket,
                        'gs_key': hail_script_key
                    },
                    'metadata': {
                        'HASH': hail_hash,
                        'SPARK': spark_version,
                        'HAIL_VERSION': hail_version,
                        'MINICONDA_VERSION': '4.4.10' if hail_version == "devel" else '2',
                        'JAR': 'gs://hail-common/builds/{}/jars/hail-{}-{}-Spark-{}.jar'.format(hail_version, hail_version, hail_hash, spark_version),
                        'ZIP': 'hail-{}-{}.zip'.format(hail_version, hail_hash)
                    }
                }
            },
            #
            # Dataproc submit
            #
            'dataproc_submit': {
                'gcp-administrative': {
                    'project': project_id,
                    'zone': zone,
                    'credentials': gcp_credentials,
                },
                'dataproc-administrative': {
                    'cluster_name': resource_id,
                    'image_version': '1.1' if spark_version == "2.0.2" else '1.2',
                    'masterConfig': {
                        'master_type': master_type,
                        'num_masters': num_masters,
                        'boot_disk_size': master_boot_disk_size,
                        'num_local_ssds': master_num_ssds
                    },
                    'workerConfig': {
                        'worker_type': worker_type,
                        'num_workers': num_workers,
                        'boot_disk_size': worker_boot_disk_size,
                        'num_local_ssds': worker_num_ssds
                    },
                    'preemptConfig': {
                        'preempt_type': preempt_type,
                        'num_preempt': num_preempt,
                        'boot_disk_size': preempt_boot_disk_size,
                        'num_local_ssds': preempt_num_ssds
                    }
                },
                'hail-administrative': {
                    'hail_control': {
                        'hail_command': 'submit',
                        'resource_id': resource_id
                    },
                    'cohort_prefix': cohort_prefix,
                    'reference_build': build,
                    'results_uri': results_uri,
                    'sink_bucket': cloud_transfer_outbucket,
                    'giab_bucket': giab_bucket,
                    'script': {
                        'gs_bucket': hail_script_bucket,
                        'gs_key': hail_script_key
                    },
                    'metadata': {
                        'HASH': hail_hash,
                        'SPARK': spark_version,
                        'HAIL_VERSION': hail_version,
                        'MINICONDA_VERSION': '4.4.10' if hail_version == "devel" else '2',
                        'JAR': 'gs://hail-common/builds/{}/jars/hail-{}-{}-Spark-{}.jar'.format(hail_version, hail_version, hail_hash, spark_version),
                        'ZIP': 'hail-{}-{}.zip'.format(hail_version, hail_hash)
                    }
                }
            }
        }

        return self.mx_input

