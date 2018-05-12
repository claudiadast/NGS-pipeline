# Use GATK 3.8-0 as parent image
FROM broadinstitute/gatk3:3.8-0

# Set the working directory in container to /combineGVCFs
WORKDIR /vqsr_indel_apply

# Install python, pip, and aws cli
# Need to install sudo first to remove prompts of [Y/n]
RUN apt-get update && apt-get install -y sudo && rm -rf /var/lib/apt/lists/* \
	&& sudo apt-get -y update \
	&& sudo apt-get -y install python-pip python-dev build-essential \
	&& sudo apt-get install -y groff \
	&& sudo pip install --upgrade pip \
	&& sudo pip install --upgrade virtualenv \
	&& sudo pip install awscli --upgrade --user \
	&& pip install boto3 \
	&& pip install fasteners

# Input and Output dirs will be mounted with docker run...somehow

# Add combineGVCF.py script from local to container
ADD . /vqsr_indel_apply

# Run combineGVCFs to test. Need to reference the jar file w absolute path
# Use CMD when you don't want code to execute during build.  
# Only one CMD/Dockerfile.
RUN cd /vqsr_indel_apply

CMD bash /vqsr_indel_apply/vqsr_indel_apply_setup.sh
