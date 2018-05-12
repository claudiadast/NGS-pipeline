import subprocess
import uuid

def calculate_max_cpus(num_samples, mode):
    """
    Calculate the max number of CPUs for the compute environment 
    based on the number of samples.
    :param num_samples: Int
    :return: max_cpus: Int
    """
    if mode == "prod":
        instance_num_vcpus = [
            ("c5.9xlarge", 36, "multi"),
            ("c5.18xlarge", 72, "single"),
            ("r4.2xlarge", 8, "multi"),
            ("r4.4xlarge", 16, "single")]
    elif mode == "test":
        instance_num_vcpus = [
            ("c5.2xlarge", 8, "multi"),
            ("r4.xlarge", 4, "multi"),
            ("r4.4xlarge", 16, "single")]
    else:
        raise ValueError("Invalid mode!")
    max_cpus = 0
    for inst in instance_num_vcpus:
        if inst[2] == "multi":
            max_cpus += num_samples * inst[1]
        elif inst[2] == "single":
            max_cpus += 1
    return max_cpus

def generate_uid():
    return str(uuid.uuid4())[:4]

def merge_configs(user_settings, args):
    """
    Generate system configuration (conf) object and merge
    it with user configuration
    :param user_settings: Dict
    :return conf: Dict
    """
    conf = dict()
    default_params = [
        "DOCKER_ACCOUNT",
        "RESOURCE_CFN_TMPL_DEPLOY_BUCKET",
        "STACK_NAME",
        "GPCE_VPC_ID",
        "GPCE_INSTANCE_TYPES",
        "CONTAINER_NAMES",
        "PARAM_FILE",
        "PARAM_PATH",
        "PARAM_BUCKET",
        "PARAM_PREFIX",
        "FASTQ_SUFFIX",
        "BUILD",
        "OME",
        "COHORT_PREFIX",
        "MODE",
        "POLL_TIME"]

    #
    # Credentials for Handoff
    #
    conf["ACCESS_KEY_ID"] = args.access_key_id
    conf["SECRET_ACCESS_KEY"] = args.secret_access_key

    #
    # Runtime/launch settings from user, no defaults
    #

    conf["REF_BUCKET_URI"] = user_settings["REF_BUCKET_URI"]
    conf["INPUT_PREFIX_URI"] = user_settings["INPUT"]
    conf["OUTPUT_PREFIX_URI"] = user_settings["OUTPUT"]
    conf["SAMPLE_ID_LIST"] = user_settings["SAMPLES"]
    conf["COHORT_LABEL"] = user_settings["COHORT_PREFIX"]

    conf["MODE"] = user_settings["MODE"]
    conf["BUILD"] = user_settings["BUILD"]
    conf["OME"] = user_settings["OME"]
    if conf["OME"] == "wes":
        conf["TARGET_FILE_NAME"] = user_settings["TARGET"]
        conf["TARGET_FILE_LOCAL_PATH"] = user_settings["TARGET_FILE_LOCAL_PATH"]
        conf["TARGET_FILE_S3_KEY_PREFIX"] = user_settings["TARGET_FILE_S3_KEY_PREFIX"]
        conf["TARGET_FILE_S3_BUCKET"] = user_settings["TARGET_FILE_S3_BUCKET"]

    else:
        conf["TARGET_FILE_NAME"] = "None"
        conf["TARGET_FILE_LOCAL_PATH"] = "None"
        conf["TARGET_FILE_S3_KEY_PREFIX"] = "None"
        conf["TARGET_FILE_S3_BUCKET"] = "None"

    conf["POLLER_WAIT_TIME"] = int(user_settings["POLL_TIME"])
    conf["CFN_PARAM_GPCE_MAX_CPUS"] = "GPCEMaxVcpus"

    #
    # Default params which can be altered by user
    #

    conf["DOCKER_ACCOUNT"] = "ucsfpsychcore"
    conf["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"] = "CFNTemplate"
    conf["STACK_NAME"] = "psychcore-ngs-pipeline"

    conf["GPCE_VPC_ID"] = 1234
    conf["GPCE_INSTANCE_TYPES"] = "c5.2xlarge, c5.9xlarge, c5.18xlarge, r4.xlarge, r4.2xlarge, r4.4xlarge"
    conf["GPCE_INSTANCE_TYPE_MEMS"] = {
        "c5.2xlarge": 16,
        "c5.9xlarge": 72,
        "c5.18xlarge": 144,
        "r4.xlarge": 30.5,
        "r4.2xlarge": 61,
        "r4.4xlarge": 122
    }
    conf["CONTAINER_NAMES"] = [
        "bwa_mem", "sort_sam", "mark_dups", "index_bam", "base_recal_table", "base_recal",
        "sentieon_haplotyper", "sentieon_genotyper",
        "vqsr_snp_model", "vqsr_snp_apply", "vqsr_indel_model", "vqsr_indel_apply"]
    conf["PARAM_FILE"] = "tool_parameter_{}_{}.yaml".format(conf["OME"], conf["BUILD"])
    conf["PARAM_PATH"] = "./docker"
    conf["PARAM_BUCKET"] = "wgs-pipeline-param-jsons"
    conf["PARAM_PREFIX"] = ""
    conf["FASTQ_SUFFIX"] = ""

    conf["BUILD"] = "GRCh38"
    conf["OME"] = "wgs"

    conf["COHORT_PREFIX"] = "HG001-validation"
    conf["MODE"] = "prod"
    conf["POLL_TIME"] = 300

    conf["VQSR_TEST_DATA_URI_PREFIX"] = "s3://pipeline-validation/sfn-test/"
    conf["VQSR_TEST_COHORT_KEY"] = "SSC_chr17_02_28_sentieon"

    conf["CFN_ARGMT_GPCE_MAX_CPUS"] = calculate_max_cpus(len(conf["SAMPLE_ID_LIST"]), conf["MODE"])

    #
    # Change user changeable configuration if the param is in run.yaml
    #

    for param in default_params:
        if param in user_settings:
            conf[param] = user_settings[param]
            if param == "DOCKER_ACCOUNT":
                conf["DOCKER_PASSWORD"] = user_settings["DOCKER_PASSWORD"]

    conf["RKSTR8_PKG_LOCAL_PATH"] = 'rkstr8'
    conf["RKSTR8_PKG_ARCHIVE_LOCAL_PATH_PREFIX"] = 'rkstr8.pkg'
    conf["RKSTR8_PKG_ARCHIVE_LOCAL_PATH"] = 'rkstr8.pkg.zip'
    conf["RKSTR8_PKG_REMOTE_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["RKSTR8_PKG_REMOTE_KEY"] = conf["RKSTR8_PKG_ARCHIVE_LOCAL_PATH"]

    #
    # Legacy configuration
    #

    conf["LAMBDA_MODULE_NAME"] = 'cloudspan'

    conf["LAMBDA_SCRIPT_FILE"] = 'lambda/{module_name}.py'.format(module_name=conf["LAMBDA_MODULE_NAME"])
    conf["LAMBDA_REQUIREMENTS_FILE"] = 'lambda/cloudspan_requirements.txt'
    conf["LAMBDA_BUILD_DIR"] = '.lambda_build'
    conf["LAMBDA_DEPLOYMENT_ZIP"] = 'sfn.deployable'
    conf["LAMBDA_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["LAMBDA_DEPLOY_KEY"] = 'sfn.deployable.zip'


    #
    # Lambda (and StateMachine) building config
    #

    conf["ALIGN_POLLER_LAMBDA_MODULE_NAME"] = 'alignment_polling'
    conf["ALIGN_POLLER_LAMBDA_SCRIPT_FILE"] = 'lambda/{module_name}.py'.format(module_name=conf["ALIGN_POLLER_LAMBDA_MODULE_NAME"])
    conf["ALIGN_POLLER_LAMBDA_REQUIREMENTS_FILE"] = 'lambda/alignment_polling_requirements.txt'
    conf["ALIGN_POLLER_LAMBDA_BUILD_DIR"] = '.alignment_polling_lambda_build'
    conf["ALIGN_POLLER_LAMBDA_DEPLOYMENT_ZIP"] = 'alignment_polling.deployable'
    conf["ALIGN_POLLER_LAMBDA_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["ALIGN_POLLER_LAMBDA_DEPLOY_KEY"] = 'alignment_polling.deployable.zip'

    conf["VQSR_LAMBDA_MODULE_NAME"] = 'vqsr'
    conf["VQSR_LAMBDA_SCRIPT_FILE"] = 'lambda/{module_name}.py'.format(module_name=conf["VQSR_LAMBDA_MODULE_NAME"])
    conf["VQSR_LAMBDA_REQUIREMENTS_FILE"] = 'lambda/vqsr_requirements.txt'
    conf["VQSR_LAMBDA_BUILD_DIR"] = '.vqsr_lambda_build'
    conf["VQSR_LAMBDA_DEPLOYMENT_ZIP"] = 'vqsr.deployable'
    conf["VQSR_LAMBDA_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["VQSR_LAMBDA_DEPLOY_KEY"] = 'vqsr.deployable.zip'

    conf["ALIGN_LAMBDA_MODULE_NAME"] = 'alignment_processing'
    conf["ALIGN_LAMBDA_SCRIPT_FILE"] = 'lambda/{module_name}.py'.format(module_name=conf["ALIGN_LAMBDA_MODULE_NAME"])
    conf["ALIGN_LAMBDA_REQUIREMENTS_FILE"] = 'lambda/alignment_processing_requirements.txt'
    conf["ALIGN_LAMBDA_BUILD_DIR"] = '.alignment_processing_lambda_build'
    conf["ALIGN_LAMBDA_DEPLOYMENT_ZIP"] = 'alignment_processing.deployable'
    conf["ALIGN_LAMBDA_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["ALIGN_LAMBDA_DEPLOY_KEY"] = 'alignment_processing.2.deployable.zip'

    conf["HAPLO_LAMBDA_MODULE_NAME"] = 'sentieon_haplotyper'
    conf["HAPLO_LAMBDA_SCRIPT_FILE"] = 'lambda/{module_name}.py'.format(module_name=conf["HAPLO_LAMBDA_MODULE_NAME"])
    conf["HAPLO_LAMBDA_REQUIREMENTS_FILE"] = 'lambda/sentieon_haplotyper_requirements.txt'
    conf["HAPLO_LAMBDA_BUILD_DIR"] = '.sentieon_haplotyper_lambda_build'
    conf["HAPLO_LAMBDA_DEPLOYMENT_ZIP"] = 'sentieon_haplotyper.deployable'
    conf["HAPLO_LAMBDA_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["HAPLO_LAMBDA_DEPLOY_KEY"] = 'sentieon_haplotyper.deployable.zip'

    conf["GENO_LAMBDA_MODULE_NAME"] = 'sentieon_genotyper'
    conf["GENO_LAMBDA_SCRIPT_FILE"] = 'lambda/{module_name}.py'.format(module_name=conf["GENO_LAMBDA_MODULE_NAME"])
    conf["GENO_LAMBDA_REQUIREMENTS_FILE"] = 'lambda/sentieon_genotyper_requirements.txt'
    conf["GENO_LAMBDA_BUILD_DIR"] = '.sentieon_genotyper_lambda_build'
    conf["GENO_LAMBDA_DEPLOYMENT_ZIP"] = 'sentieon_genotyper.deployable'
    conf["GENO_LAMBDA_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["GENO_LAMBDA_DEPLOY_KEY"] = 'sentieon_genotyper.deployable.zip'

    conf["TRIO_LAMBDA_MODULE_NAME"] = 'triodenovo'
    conf["TRIO_LAMBDA_DEPLOY_KEY"] = 'triodenovo.deployable.zip'

    conf["HDOF_LAMBDA_MODULE_NAME"] = 'handoff'
    conf["HDOF_LAMBDA_DEPLOY_KEY"] = 'handoff.deployable.zip'

    conf["VAL_LAMBDA_MODULE_NAME"] = 'validation'

    conf["DPROC_CREATE_POLLER_LAMBDA_MODULE_NAME"] = 'dataproc_create_polling'
    conf["DPROC_CREATE_POLLER_LAMBDA_DEPLOY_KEY"] = 'dataproc_create_polling.deployable.zip'
    conf["DPROC_SUBMIT_POLLER_LAMBDA_MODULE_NAME"] = 'dataproc_submit_polling'
    conf["DPROC_SUBMIT_POLLER_LAMBDA_DEPLOY_KEY"] = 'dataproc_submit_polling.deployable.zip'

    #
    # Lambda Cloudformation (Parameter, Argument) pairs
    #

    conf["CLOUDSPAN_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'CloudspanLambdaFuncS3BucketName'
    conf["CLOUDSPAN_CFN_PARAM_LAMBDA_KEY_NAME"] = 'CloudspanLambdaFuncS3KeyName'
    conf["CLOUDSPAN_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'CloudspanLambdaFuncModuleName'

    conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["LAMBDA_DEPLOY_KEY"]
    conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["LAMBDA_MODULE_NAME"]

    conf["ALIGN_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'AlignmentLambdaFuncS3BucketName'
    conf["ALIGN_CFN_PARAM_LAMBDA_KEY_NAME"] = 'AlignmentLambdaFuncS3KeyName'
    conf["ALIGN_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'AlignmentLambdaFuncModuleName'

    conf["ALIGN_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["ALIGN_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["ALIGN_LAMBDA_DEPLOY_KEY"]
    conf["ALIGN_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["ALIGN_LAMBDA_MODULE_NAME"]

    conf["HAPLO_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'HaploLambdaFuncS3BucketName'
    conf["HAPLO_CFN_PARAM_LAMBDA_KEY_NAME"] = 'HaploLambdaFuncS3KeyName'
    conf["HAPLO_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'HaploLambdaFuncModuleName'

    conf["HAPLO_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["HAPLO_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["HAPLO_LAMBDA_DEPLOY_KEY"]
    conf["HAPLO_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["HAPLO_LAMBDA_MODULE_NAME"]

    conf["GENO_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'GenoLambdaFuncS3BucketName'
    conf["GENO_CFN_PARAM_LAMBDA_KEY_NAME"] = 'GenoLambdaFuncS3KeyName'
    conf["GENO_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'GenoLambdaFuncModuleName'

    conf["GENO_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["GENO_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["GENO_LAMBDA_DEPLOY_KEY"]
    conf["GENO_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["GENO_LAMBDA_MODULE_NAME"]

    conf["VQSR_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'VQSRLambdaFuncS3BucketName'
    conf["VQSR_CFN_PARAM_LAMBDA_KEY_NAME"] = 'VQSRLambdaFuncS3KeyName'
    conf["VQSR_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'VQSRLambdaFuncModuleName'

    conf["VQSR_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["VQSR_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["VQSR_LAMBDA_DEPLOY_KEY"]
    conf["VQSR_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["VQSR_LAMBDA_MODULE_NAME"]

    conf["HDOF_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'HandoffLambdaFuncS3BucketName'
    conf["HDOF_CFN_PARAM_LAMBDA_KEY_NAME"] = 'HandoffLambdaFuncS3KeyName'
    conf["HDOF_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'HandoffLambdaFuncModuleName'

    conf["HDOF_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["HDOF_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["HDOF_LAMBDA_DEPLOY_KEY"]
    conf["HDOF_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["HDOF_LAMBDA_MODULE_NAME"]

    conf["BATCH_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'BatchPollerLambdaFuncS3BucketName'
    conf["BATCH_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["BATCH_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"] = 'BatchPollerLambdaFuncS3KeyName'
    conf["BATCH_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["ALIGN_POLLER_LAMBDA_DEPLOY_KEY"]
    conf["BATCH_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'BatchPollerLambdaFuncModuleName'
    conf["BATCH_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["ALIGN_POLLER_LAMBDA_MODULE_NAME"]

    conf["DPROC_CREATE_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'DataprocCreatePollerLambdaFuncS3BucketName'
    conf["DPROC_CREATE_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["DPROC_CREATE_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"] = 'DataprocCreatePollerLambdaFuncS3KeyName'
    conf["DPROC_CREATE_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["DPROC_CREATE_POLLER_LAMBDA_DEPLOY_KEY"]
    conf["DPROC_CREATE_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'DataprocCreatePollerLambdaFuncModuleName'
    conf["DPROC_CREATE_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["DPROC_CREATE_POLLER_LAMBDA_MODULE_NAME"]

    conf["DPROC_SUBMIT_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'DataprocSubmitPollerLambdaFuncS3BucketName'
    conf["DPROC_SUBMIT_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["DPROC_SUBMIT_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"] = 'DataprocSubmitPollerLambdaFuncS3KeyName'
    conf["DPROC_SUBMIT_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["DPROC_SUBMIT_POLLER_LAMBDA_DEPLOY_KEY"]
    conf["DPROC_SUBMIT_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'DataprocSubmitPollerLambdaFuncModuleName'
    conf["DPROC_SUBMIT_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["DPROC_SUBMIT_POLLER_LAMBDA_MODULE_NAME"]

    #
    # Other Cloudformation (Parameter, Argument) config pairs
    #

    conf["STACK_UID"] = generate_uid()
    conf["STACK_NAME"] = "{}-{}".format(user_settings["STACK_NAME"], conf["STACK_UID"])
    conf["CFN_ARGMT_GPCE_VPC_ID"] = conf["GPCE_VPC_ID"]
    conf["CFN_PARAM_GPCE_VPC_ID"] = "GPCEVpcId"
    conf["CFN_PARAM_GPCE_INSTANCE_TYPES"] = 'GPCEInstanceTypes'
    conf["CFN_PARAM_GPCE_SSH_KEY_PAIR"] = 'GPCESSHKeyPair'
    conf["CFN_ARGMT_GPCE_INSTANCE_TYPES"] = conf["GPCE_INSTANCE_TYPES"]
    conf["CFN_ARGMT_GPCE_SSH_KEY_PAIR"] = user_settings["GPCE_SSH_KEY_PAIR"]

    conf["CFN_FSA_LOGICAL_RESOURCE_ID"] = 'PipelineStateMachine'

    #
    # Cloudformation template deployment config
    #

    # TODO: This should probably be deleted

    conf["TEMPLATE"] = 'rkstr8.stack.yaml'
    conf["RESOURCE_CFN_TMPL_DEPLOY_KEY"] = conf["TEMPLATE"]
    conf["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]

    #
    # CFN Templates and related
    #

    conf["PARENT_TEMPLATE_PATH"] = 'templates/platform_parent.stack.yaml'
    conf["LAMBDA_TEMPLATE_PATH"] = 'templates/lambda_resources.stack.yaml'
    conf["NETWORK_TEMPLATE_PATH"] = 'templates/network_resources.stack.yaml'
    conf["BATCH_TEMPLATE_PATH"] = 'templates/batch_resources.stack.yaml'
    conf["STEPFUNCTIONS_TEMPLATE_PATH"] = 'templates/step_functions_resources.stack.yaml'
    conf["STATE_MACHINE_RESOURCE_FRAGMENT"] = 'templates/fragments/statemachine.json'
    conf["FRAGMENTS_DIR_PATH"] = 'templates/fragments'

    conf["TEMPLATES"] = [
        conf["PARENT_TEMPLATE_PATH"],
        conf["LAMBDA_TEMPLATE_PATH"],
        conf["NETWORK_TEMPLATE_PATH"],
        conf["BATCH_TEMPLATE_PATH"],
        conf["STEPFUNCTIONS_TEMPLATE_PATH"]]

    conf["TEMPLATE_LABEL_PATH_MAP"] = {
        'launch': conf["PARENT_TEMPLATE_PATH"],
        'lambda': conf["LAMBDA_TEMPLATE_PATH"],
        'network': conf["NETWORK_TEMPLATE_PATH"],
        'batch': conf["BATCH_TEMPLATE_PATH"],
        'sfn': conf["STEPFUNCTIONS_TEMPLATE_PATH"]
    }

    conf["LAMBDA_CFN_PARAM_TEMPLATE_URL"] = "LambdaTemplateURL"
    conf["NETWORK_CFN_PARAM_TEMPLATE_URL"] = "NetworkTemplateURL"
    conf["BATCH_CFN_PARAM_TEMPLATE_URL"] = "BatchTemplateURL"
    conf["STEP_FUNCTIONS_PARAM_TEMPLATE_URL"] = "StepFunctionsTemplateURL"

    conf["LAMBDA_CFN_ARGMT_TEMPLATE_URL"] = "https://s3.amazonaws.com/{}/lambda_resources.stack.yaml".format(conf["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"])
    conf["NETWORK_CFN_ARGMT_TEMPLATE_URL"] = "https://s3.amazonaws.com/{}/network_resources.stack.yaml".format(conf["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"])
    conf["BATCH_CFN_ARGMT_TEMPLATE_URL"] = "https://s3.amazonaws.com/{}/batch_resources.stack.yaml".format(conf["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"])
    conf["STEP_FUNCTIONS_ARGMT_TEMPLATE_URL"] = "https://s3.amazonaws.com/{}/step_functions_resources.stack.yaml".format(conf["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"])

    #
    # Batch config
    #


    conf["PIPELINE_CMD_TOOL_PARAM_FILE"] = "tool_parameter_{}_{}.yaml".format(conf["OME"], conf["BUILD"])
    conf["PIPELINE_CMD_TOOL_PARAM_LOCAL_PATH"] = "./docker"
    conf["PIPELINE_CMD_TOOL_PARAM_S3_BUCKET"] = user_settings["PARAM_BUCKET"]
    conf["PIPELINE_CMD_TOOL_PARAM_S3_KEY_PREFIX"] = user_settings["PARAM_PREFIX"]

    conf["SENTIEON_S3_BUCKET"] = user_settings["SENTIEON_BUCKET"]
    conf["SENTIEON_S3_KEY_PREFIX"] = user_settings["SENTIEON_PREFIX"]
    conf["SENTIEON_PACKAGE_LOCAL_PATH"] = user_settings["SENTIEON_PACKAGE_PATH"]
    conf["SENTIEON_PACKAGE_NAME"] = user_settings["SENTIEON_PACKAGE_NAME"]
    conf["SENTIEON_LICENSE_LOCAL_PATH"] = user_settings["SENTIEON_LICENSE_PATH"]
    conf["SENTIEON_LICENSE_FILE_NAME"] = user_settings["SENTIEON_LICENSE_NAME"]


    #
    # Job Def dimension
    #
    conf["MEMS"] = {
        "test": {
            "bwa_mem" : "15000",
            "sort_sam" : "30000",
            "mark_dups" : "30000",
            "index_bam" : "30000",
            "base_recal_table" : "30000",
            "base_recal" : "30000",
            "sentieon_haplotyper" : "15000",
            "sentieon_genotyper" : "30000",
            "vqsr_snp_model" : "120000",
            "vqsr_snp_apply" : "120000",
            "vqsr_indel_model" : "120000",
            "vqsr_indel_apply" : "120000"
        },
        "prod": {
            "bwa_mem" : "66000",
            "sort_sam" : "56000",
            "mark_dups" : "56000",
            "index_bam" : "56000",
            "base_recal_table" : "117000",
            "base_recal" : "117000",
            "sentieon_haplotyper" : "66000",
            "sentieon_genotyper" : "132000",
            "vqsr_snp_model" : "132000",
            "vqsr_snp_apply" : "132000",
            "vqsr_indel_model" : "132000",
            "vqsr_indel_apply" : "132000"
        }
    }
    conf["VCPUS"] = {
        "test": {
            "bwa_mem" : "8",
            "sort_sam" : "4",
            "mark_dups" : "4",
            "index_bam" : "4",
            "base_recal_table" : "4",
            "base_recal" : "4",
            "sentieon_haplotyper" : "8",
            "sentieon_genotyper" : "4",
            "vqsr_snp_model" : "16",
            "vqsr_snp_apply" : "16",
            "vqsr_indel_model" : "16",
            "vqsr_indel_apply" : "16"
        },
        "prod": {
            "bwa_mem" : "36",
            "sort_sam" : "8",
            "mark_dups" : "8",
            "index_bam" : "8",
            "base_recal_table" : "16",
            "base_recal" : "16",
            "sentieon_haplotyper" : "36",
            "sentieon_genotyper" : "72",
            "vqsr_snp_model" : "72",
            "vqsr_snp_apply" : "72",
            "vqsr_indel_model" : "72",
            "vqsr_indel_apply" : "72"
        }
    }



    #
    # Google Cloud params
    #
    conf["GCP_CREDS"] = user_settings["GCP_CREDS_FILE"]
    conf["CLUSTER_NAME"] = (conf["STACK_NAME"]).lower()
    conf["PROJECT_ID"] = user_settings["PROJECT_ID"]
    conf["ZONE"] = user_settings["ZONE"]


    conf["HDOF_LAMBDA_MODULE_NAME"] = 'handoff'
    conf["HDOF_LAMBDA_SCRIPT_FILE"] = 'lambda/{module_name}.py'.format(module_name=conf["HDOF_LAMBDA_MODULE_NAME"])
    conf["HDOF_LAMBDA_REQUIREMENTS_FILE"] = 'lambda/handoff_requirements.txt'
    conf["HDOF_LAMBDA_BUILD_DIR"] = '.handoff_lambda_build'
    conf["HDOF_LAMBDA_DEPLOYMENT_ZIP"] = 'handoff.deployable'
    conf["HDOF_LAMBDA_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["HDOF_LAMBDA_DEPLOY_KEY"] = 'handoff.deployable.zip'

    conf["HDOF_POLLER_LAMBDA_MODULE_NAME"] = 'handoff_poller'
    conf["HDOF_POLLER_LAMBDA_SCRIPT_FILE"] = 'lambda/{module_name}.py'.format(module_name=conf["HDOF_POLLER_LAMBDA_MODULE_NAME"])
    conf["HDOF_POLLER_LAMBDA_REQUIREMENTS_FILE"] = 'lambda/handoff_poller_requirements.txt'
    conf["HDOF_POLLER_LAMBDA_BUILD_DIR"] = '.handoff_poller_lambda_build'
    conf["HDOF_POLLER_LAMBDA_DEPLOYMENT_ZIP"] = 'handoff_poller.deployable'
    conf["HDOF_POLLER_LAMBDA_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["HDOF_POLLER_LAMBDA_DEPLOY_KEY"] = 'handoff_poller.deployable.zip'

    conf["DPROC_CREATE_POLLER_LAMBDA_MODULE_NAME"] = 'dataproc_create_polling'
    conf["DPROC_CREATE_POLLER_LAMBDA_SCRIPT_FILE"] = 'lambda/{module_name}.py'.format(module_name=conf["DPROC_CREATE_POLLER_LAMBDA_MODULE_NAME"])
    conf["DPROC_CREATE_POLLER_LAMBDA_REQUIREMENTS_FILE"] = 'lambda/dataproc_create_polling_requirements.txt'
    conf["DPROC_CREATE_POLLER_LAMBDA_BUILD_DIR"] = '.dataproc_create_polling_lambda_build'
    conf["DPROC_CREATE_POLLER_LAMBDA_DEPLOYMENT_ZIP"] = 'dataproc_create_polling.deployable'
    conf["DPROC_CREATE_POLLER_LAMBDA_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["DPROC_CREATE_POLLER_LAMBDA_DEPLOY_KEY"] = 'dataproc_create_polling.deployable.zip'

    conf["DPROC_SUBMIT_POLLER_LAMBDA_MODULE_NAME"] = 'dataproc_submit_polling'
    conf["DPROC_SUBMIT_POLLER_LAMBDA_SCRIPT_FILE"] = 'lambda/{module_name}.py'.format(module_name=conf["DPROC_SUBMIT_POLLER_LAMBDA_MODULE_NAME"])
    conf["DPROC_SUBMIT_POLLER_LAMBDA_REQUIREMENTS_FILE"] = 'lambda/dataproc_submit_polling_requirements.txt'
    conf["DPROC_SUBMIT_POLLER_LAMBDA_BUILD_DIR"] = '.dataproc_submit_polling_lambda_build'
    conf["DPROC_SUBMIT_POLLER_LAMBDA_DEPLOYMENT_ZIP"] = 'dataproc_submit_polling.deployable'
    conf["DPROC_SUBMIT_POLLER_LAMBDA_DEPLOY_BUCKET"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["DPROC_SUBMIT_POLLER_LAMBDA_DEPLOY_KEY"] = 'dataproc_submit_polling.deployable.zip'


    #
    # Lambda Cloudformation (Parameter, Argument) pairs
    #

    conf["CLOUDSPAN_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'CloudspanLambdaFuncS3BucketName'
    conf["CLOUDSPAN_CFN_PARAM_LAMBDA_KEY_NAME"] = 'CloudspanLambdaFuncS3KeyName'
    conf["CLOUDSPAN_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'CloudspanLambdaFuncModuleName'

    conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["LAMBDA_DEPLOY_KEY"]
    conf["CLOUDSPAN_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["LAMBDA_MODULE_NAME"]

    conf["HDOF_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME"] = 'HandoffPollerLambdaFuncS3BucketName'
    conf["HDOF_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME"] = user_settings["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"]
    conf["HDOF_POLLER_CFN_PARAM_LAMBDA_KEY_NAME"] = 'HandoffPollerLambdaFuncS3KeyName'
    conf["HDOF_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME"] = conf["HDOF_POLLER_LAMBDA_DEPLOY_KEY"]
    conf["HDOF_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME"] = 'HandoffPollerLambdaFuncModuleName'
    conf["HDOF_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME"] = conf["HDOF_POLLER_LAMBDA_MODULE_NAME"]

    conf["RKSTR8_PKG_CFN_PARAM_BUCKET_NAME"] = 'Rkstr8PkgBucketName'
    conf["RKSTR8_PKG_CFN_ARGMT_BUCKET_NAME"] = conf["RKSTR8_PKG_REMOTE_BUCKET"]
    conf["RKSTR8_PKG_CFN_PARAM_KEY_NAME"] = 'Rkstr8PkgKeyName'
    conf["RKSTR8_PKG_CFN_ARGMT_KEY_NAME"] = conf["RKSTR8_PKG_REMOTE_KEY"]


    #
    # Google Cloud params
    #

    conf["CLOUD_TRANSFER_OUTBUCKET"] = user_settings["CLOUD_TRANSFER_OUTBUCKET"]
    conf["GCP_CREDS"] = user_settings["GCP_CREDS_FILE"]
    conf["GIAB_BUCKET"] = "gs://pipeline-assets"

    conf["MASTER_TYPE"] = "n1-highmem-8"
    conf["WORKER_TYPE"] = "n1-standard-8"
    conf["NUM_MASTERS"] = 1
    conf["NUM_WORKERS"] = 10
    conf["NUM_PREEMPT"] = 20

    conf["MASTER_BOOT_DISK_SIZE"] = 100
    conf["WORKER_BOOT_DISK_SIZE"] = 40
    conf["PREEMPT_BOOT_DISK_SIZE"] = 40

    conf["MASTER_NUM_SSDS"] = 0
    conf["WORKER_NUM_SSDS"] = 0
    conf["PREEMPT_NUM_SSDS"] = 0

    conf["HAIL_SCRIPT_BUCKET"] = "pipeline-validation/hail-validation-test"
    conf["HAIL_SCRIPT_KEY"] = "hail-validation.py"
    conf["SPARK_VERSION"] = "2.2.0"
    conf["HAIL_VERSION"] = "devel"

    conf["STACK_LAUNCH_TIMEOUT_MINUTES"] = 10

    conf["HAIL_HASH"] = "6f866e159085"
    

    return conf
