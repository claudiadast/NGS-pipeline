# Sention recommends Ubuntu-14.04 or higher.  
FROM ubuntu:14.04

# Set the working directory in container
WORKDIR /sentieon_genotyper

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
	&& pip install fasteners \
	&& sudo apt-get install zlib1g-dev

# Install Sentieon
RUN export SENTIEON_PYTHON=/usr/bin/ \
	&& export SENTIEON_LICENSE=/sentieon_genotyper/LICENSE_DIR/UCSF_Sanders_eval.lic

ADD . /sentieon_genotyper

CMD /sentieon_genotyper/sentieon_genotyper_setup.sh