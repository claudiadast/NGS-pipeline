#!/bin/bash
export PATH=$PATH:~/.local/bin/
export SENTIEON_PYTHON=/usr/bin/
export SENTIEON_LICENSE=/sentieon_haplotyper/localDir/UCSF_Sanders_eval.lic
export PATH=$PATH:/sentieon_haplotyper/localDir/sentieon-genomics-201711.01/bin/

source ./SDK.sh 
program_setup_run sentieon_haplotyper
