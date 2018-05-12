KMS_ADMIN_USER_ARN = "arn:aws:iam::249640142434:user/Lindsay"
KMS_END_USER_ARN = "arn:aws:iam::249640142434:user/Lindsay"
RESOURCE_CFN_TMPL_DEPLOY_BUCKET = "CFNTemplate"
GPCE_VPC_ID = "vpc-2a9fa853"
GPCE_SUBNET_AZ1_CIDR_BLOCK = "172.31.196.0/20" 
#GPCE_INSTANCE_TYPES  = "r4.4xlarge, c5.9xlarge, c5.18xlarge"
GPCE_INSTANCE_TYPES = "r4.xlarge, c5.2xlarge, c5.4xlarge, r4.4xlarge"
CONTAINER_NAMES = ["bwa_mem", 
					"sort_sam", 
					"mark_dups", 
					"index_bam", 
					"base_recal_table", 
					"base_recal",
					"sentieon_haplotyper",
					"sentieon_genotyper",
					"vqsr_snp_model",
					"vqsr_snp_apply",
					"vqsr_indel_model",
					"vqsr_indel_apply"]
GPCE_SSH_KEY_PAIR = "LindsayKey"
REF = "hg38"
# SAMPLES = ["594"]
# COHORT_PREFIX = "multisample-wgs"
#INPUT = "s3://pipeline-validation/wgs-fq/"
#OUTPUT = "s3://pipeline-validation/wgs-full-pipeline-test/"
SAMPLES = ['D680TT', 'F680TT']
COHORT_PREFIX = "multisample-small"
INPUT = "s3://pipeline-validation/smallfq/"
OUTPUT = "s3://pipeline-validation/small-data-runs/"
STACK_NAME = "LindsayTTRun"
REPO_LOCATION = "/Users/lindsayliang/Documents/aws-sfn-tutorial"
param_file = "tool_parameter.yaml"
param_path = "./docker"
param_bucket = "wgs-pipeline-param-jsons"
param_prefix = ""
sentieon_license_bucket = "wgs-pipeline-param-jsons"
sentieon_license_prefix = ""
sentieon_license_path = "./"
sentieon_license_name = "UCSF_Sanders_eval.lic"

TEST_COHORT_PREFIX = "SSC_chr17_02_28_sentieon"
MODE = "test"
FASTQ_SUFFIX = ""
TEST_INPUT = "s3://pipeline-validation/sfn-test/"
REF_BUCKET_URI = "s3://GRCh38-references/"
vqsr_test_data_uri_prefix = "s3://pipeline-validation/sfn-test/"
vqsr_test_cohort_key = "SSC_chr17_02_28_sentieon"