# Welcome to the Sanders Lab NGS-pipeline!

The following NGS pipeline was developed by the UCSF Psychiatry Bioinformatics Core (PsychCore) 
team to call variants on large cohorts of human sequencing data. The current build can run 
on either whole genome or whole exome/targeted sequencing data. This build can also call variants
based on either reference genome GRCh37 (hg19) or GRCh38 (hg39). Please note that this pipeline is
still being developed.  

What follows is a brief quick-start guide to running the pipeline. Full documentation 
can be found on readthedocs here: http://ngs-pipeline.readthedocs.io/en/latest/index.html

# Getting Started

Before running the pipeline, you'll need the following:

  - An AWS account
  - A Sentieon License
  - A python 3.6 envrionment configured with our project dependencies (we recommend Conda!)

If you need to set up any of the above, please see the docs; once you 
have these in place, you can execute the command at the bottom to run
the pipeline.

## Creating and Setting up your Amazon Web Services (AWS) Account

To create an AWS account, you can follow the instructions at ([AWS](https://aws.amazon.com)). 
The pipeline's infrastructure is made up of several
AWS Services (see [Pipeline infrastructure](http://ngs-pipeline.readthedocs.io/en/latest/overview.html#pipeline-infrastructure)).

With a newly created AWS account there are certain **limits** imposed that would restrict runs to 5 samples. 
To scale up to larger datasets, you can increase these **limits** by following the instructions at [Limits](https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Limits:) page under the [EC2 dashboard](https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Home:) in the
AWS console. Note that this may take some time to process, so it should be done early.

You will also need at least one **SSH KeyPair** created in your account in order to run a pipeline (link to instrutions).
The SSH KeyPair allows you to remotely connect to the machines that carry out the variant calling, whether you like it or not.

In addition to an **SSH KeyPair**, you will need to set up cloud storage locations for input and output of the pipeline.
In AWS, this storage service is called **S3**. In this service, data is held in containers, called buckets, you will need at least one.
Additional details on creating buckets can be [found here](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-bucket.html).

## Creating a Google Cloud Platform (GCP) Account (Optional)

Note that this is only required for running the Validation pipeline (see [GCP documentation](https://cloud.google.com))

## Download and Upload Reference Files to S3

The pipeline performs many operations which require several reference
files. (Eg. the human reference genome fasta and its indexes). These
must be uploaded to AWS S3 before the pipeline can be run. The standard
reference files are provided by the Broad Institute's [GATK Resource
Bundle](https://software.broadinstitute.org/gatk/download/bundle). Currently, the pipeline supports two builds of the human
reference genome - [GRCh37](https://software.broadinstitute.org/gatk/download/bundle) (hg19) and [GRCh38](https://console.cloud.google.com/storage/browser/genomics-public-data/resources/broad/hg38/v0) (hg38). GRCh37 files
are located on the Broad Institute's ftp site while GRCH38 is hosted on
Google Cloud Storage.

In order to upload the reference files to AWS S3, you'll need to install
the AWS Command Line Interface - please see [AWS CLI Installation](https://docs.aws.amazon.com/cli/latest/userguide/installing.html).
For uploading files onto S3, please see the [AWS S3 documentation](https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html).

## Obtain a Sentieon License File

Currently, the pipeline utilises only [Sentieon](https://www.sentieon.com) in its haplotyping
and joint genotyping steps. Thus, in order to use the pipeline you must
first contact Sentieon and obtain a license. They also offer a free
[trial](https://www.sentieon.com/home/free-trial/).

## Install Conda and your Dev Environment

In order to run the pipeline, you'll need to install [Conda](https://conda.io/miniconda.html).

  - If you have python 2.7 installed currently, pick that installer.
  - If you have python 3.6 installed currently, pick that.
  - Run the installer. The defaults should be fine.

Then, create a python 3.6 environment: :

    $ conda create -n psy-ngs python=3.6

Activate the newly created environment: (you may need to start a new
terminal session) :

    $ source activate psy-ngs

You can verify that the environment has activated by checking the python
version (if it is different than your base) :

    $ python --version

You should also see the environment name prepended to your shell promp

# Running the Pipeline

    $ python rkstr8_driver.py -p <pipeline-name> [ -a access_key_id ] [ -s secret_access_key ]

# Additional Information

By default, the pipeline makes use of the following instance types:

  - c5.9xlarge, c5.18xlarge, r4.2xlarge, r4.4xlarge.

The pricing specification for each of the AWS EC2 instance types can be
found on the [AWS Instance Pricing page](https://aws.amazon.com/ec2/pricing/on-demand/).
