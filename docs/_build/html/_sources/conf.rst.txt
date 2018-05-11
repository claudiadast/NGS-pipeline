.. _sec-conf:

========================
Configuring the Pipeline
========================

.. _runyaml:

Your Pipeline Run
-----------------
Before running the pipeline, some information must be provided to the pipeline.
In the current build of the pipeline, you, the user, will have to modify the 
<run.yaml> file located in the root of the project.  It looks something like
the following:

.. code-block:: yaml

	---
	STACK_NAME : ""
	RESOURCE_CFN_TMPL_DEPLOY_BUCKET : ""

	GCP_CREDS_FILE : ""
	CLOUD_TRANSFER_OUTBUCKET : ""
	PROJECT_ID : ""
	ZONE : ""

	GPCE_SSH_KEY_PAIR : ""

	INPUT : ""
	OUTPUT : ""
	REF_BUCKET_URI : ""
	SAMPLES :
	  - ""
	  - ""
	COHORT_PREFIX : ""

	BUILD : ""
	OME : ""
	MODE : ""

	TARGET : ""
	TARGET_FILE_LOCAL_PATH : ""
	TARGET_FILE_S3_KEY_PREFIX : ""
	TARGET_FILE_S3_BUCKET : ""

	SENTIEON_PACKAGE_PATH : ""
	SENTIEON_PACKAGE_NAME: ""
	SENTIEON_LICENSE_PATH : ""
	SENTIEON_BUCKET : ""
	SENTIEON_PREFIX : ""

	PARAM_FILE : ""
	PARAM_PATH : ""
	PARAM_BUCKET : ""
	PARAM_PREFIX : ""

	POLL_TIME : ""

Each field must have a value for the pipeline to run.  Listed below are
descriptions for each of the fields.  For more information about the yaml
file format, see the `official yaml website`_.

* *STACK_NAME*
	- The name of the AWS Cloudformation stack associated with the pipeline run.
* *RESOURCE_CFN_TMPL_DEPLOY_BUCKET*
	- The AWS S3 bucket where the AWS Lambda functions and other resources will be uploaded. 
* *GCP_CREDS_FILE*
	- The full path to the credentials file for GCP for the validation step of the pipeline. 
* *CLOUD_TRANSFER_OUTBUCKET*
	- The GCS Bucket to upload the final VCF from AWS S3.
* *GCP_CREDS_FILE*
	- The absolute path to the local copy of the user's GCP credentials json file.
* *CLUSTER_NAME*
	- The name of the Dataproc cluster.
* *CLOUD_TRANSFER_OUTBUCKET*
	- The GCS URI where the final VQSR'd VCF from AWS will be uploaded to for post processing.
* *PROJECT_ID*
	- The ID for a google cloud (billable) project.
* *ZONE*
	- The availability zone on GCP where results should be stored.
* *GCPE_SSH_KEY_PAIR*
	- The AWS EC2 Key Pair to associate with the Batch Compute Environment.
	- With this the user will be able to SSH into any instance in the cluster.
* *INPUT*
	- The AWS S3 URI where the fastq for all SAMPLES are locate.
	- (eg. s3://analysis/fastqs/)
* *OUTPUT*
	- The AWS S3 URI where the output should be uploaded.
	- (eg. s3://analysis/results/)
* *REF_BUCKET_URI*
	- The AWS S3 URI where the reference files are located (see :ref:`refs`).
* *SAMPLES*
	- A yaml list of the sample names that are present in INPUT.
* *COHORT_PREFIX*
	- The name given to the vcf produced at the joint genotyping step
	- (eg family5 will produce family5.gt.vcf).
* *BUILD*
	- The build of the human reference to be used; either GRCh37 or GRCh38.
* *OME*
	- Either wgs or wes; dictates whether the pipeline should run whole genome or targeted/exome analysis.
* *MODE*
	- Set to prod, unless in development, then use test.
* *TARGET*
	- The BED file which describes the intervals to be used if OME is set to wes.
* *TARGET_FILE_LOCAL_PATH*
	- The relative path to TARGET from the root of the project directory.
* *TARGET_FILE_S3_KEY_PREFIX*
	- The S3 prefix for where TARGET should be uploaded.
* *TARGET_FILE_S3_BUCKET*
	- The S3 bucket for where TARGET should be uploaded.
* *SENTIEON_PACKAGE_NAME*
	- The name of the tar.gz Sentieon software package.
	- (eg. sentieon-genomics-201711.01.tar.gz) 
* *SENTIEON_PACKAGE_PATH*
	- The relative path to SENTIEON_PACKAGE_NAME from the root of the project directory. 
* *SENTIEON_LICENSE_NAME*
	- The name of the Sentieon license file. 
* *SENTIEON_LICENSE_PATH*
	- The relative path to SENTIEON_LICENSE from the root of the project directory. 
* *SENTIEON_BUCKET*
	- The S3 bucket for where the Sentieon software package and license should be uploaded.
* *SENTIEON_PREFIX*
	- The S3 prefix for where the Sentieon software package and license should be uploaded. 
* *PARAM_BUCKET*
	- The S3 bucket for where the tool_parameter yaml file should be uploaded. 
* *PARAM_PREFIX*
	- The S3 prefix for where the tool_parameter yaml file should be uploaded. 
* *POLL_TIME*
	- The number of seconds the Step Functions state machine should wait before polling for completed jobs (see :ref:`infrastructure`) 

Next Steps
----------

Once run.yaml has been properly filled out, the pipeline is ready to be run. Please see :ref:`run` to continue.

.. _official yaml website: http://yaml.org
