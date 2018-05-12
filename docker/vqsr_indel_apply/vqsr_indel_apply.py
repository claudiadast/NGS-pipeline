import SDK
import os
from datetime import datetime

#Check that all req env variables are set
env_vars = ["prefix", "param_uri", "ref_uri", "in_uri", "out_uri"]

if not set(env_vars).issubset(set(os.environ)):
	print "Environment not configured properly."
	raise ValueError("The required env variables are: {}".format(", ".join(env_vars)))

prefix = os.environ["prefix"]
param_uri = os.environ["param_uri"]
param_file = param_uri.split("/")[-1]
ref_uri = os.environ["ref_uri"]
in_uri = os.environ["in_uri"]
out_uri = os.environ["out_uri"]
build = os.environ["build"]
if build == "GRCh38":
	ref_files = ["Homo_sapiens_assembly38.fasta", 
		"Homo_sapiens_assembly38.fasta.fai",
		"Homo_sapiens_assembly38.dict"]
elif build == "GRCh37":
	ref_files = ["human_g1k_v37.fasta", 
		"human_g1k_v37.fasta.fai",
		"human_g1k_v37.dict"]
else:
	raise ValueError("Unrecognized build - environment variable mode must be GRCh38 or GRCh37")
vcf = "{}.gt.snp.recal.vcf".format(prefix)
idx = "{}.gt.snp.recal.vcf.idx".format(prefix)
recal_file = "{}.gt.snp.indel.recal.model".format(prefix)
tranches_file = "{}.gt.snp.indel.tranches".format(prefix)
in_files = [vcf, idx, recal_file, tranches_file]

start_time = datetime.now()
print "VQSR INDEL APPLY for {} was started at {}.".format(prefix, str(start_time))

task = SDK.Task(
	step="vqsr_indel_apply",  
	prefix=prefix, 
	in_files=in_files, 
	ref_files=ref_files,
	param_file=param_file,
	param_uri=param_uri,
	ref_uri=ref_uri,
	in_uri=in_uri,
	out_uri=out_uri)
if not set(in_files).issubset(set(os.listdir("."))):
	task.download_files("INPUT")
if not set(ref_files).issubset(set(os.listdir("."))):
	task.download_files("REF")
if param_file not in os.listdir("."):
	task.download_files("PARAMS")
task.build_cmd()
task.run_cmd()
task.upload_results()
task.cleanup()

end_time = datetime.now()
print "VQSR INDEL APPLY for {} ended at {}.".format(prefix, str(end_time))
total_time = end_time - start_time
print "Total time for VQSR INDEL APPLY was {}.".format(str(total_time))
