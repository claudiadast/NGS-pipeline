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
ome = os.environ["ome"]
if build == "GRCh38":
	ref_files = ["Homo_sapiens_assembly38.fasta", "Homo_sapiens_assembly38.fasta.fai", 
		"Homo_sapiens_assembly38.dict",
		"Mills_and_1000G_gold_standard.indels.hg38.vcf.gz", 
		"Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi",
		"Homo_sapiens_assembly38.dbsnp138.vcf", 
		"Homo_sapiens_assembly38.dbsnp138.vcf.idx",
		"Homo_sapiens_assembly38.known_indels.vcf.gz", 
		"Homo_sapiens_assembly38.known_indels.vcf.gz.tbi"]
elif build == "GRCh37":
	ref_files = ["human_g1k_v37.fasta", "human_g1k_v37.fasta.fai",
		"human_g1k_v37.dict",
		"Mills_and_1000G_gold_standard.indels.b37.vcf",
		"dbsnp_138.b37.vcf",
		"1000G_phase1.snps.high_confidence.b37.vcf",
		"xgen-exome-research-panel-targets-cols1-2-noChr-merged.bed"]
else:
	raise ValueError("Unrecognized build - environment variable build must be GRCh38 or GRCh37")
if ome == "wes":
	target_file = os.environ["target_file"]
	ref_files.append(target_file)
else:
	target_file = None
bam = "{}.sorted.deduped.bam".format(prefix)
bai = "{}.sorted.deduped.bam.bai".format(prefix)
in_files = [bam, bai]
threads = os.environ['threads']

start_time = datetime.now()
print "BASE RECAL TABLE for {} was started at {}.".format(prefix, str(start_time))

task = SDK.Task(
	step="base_recal_table",  
	prefix=prefix, 
	threads=threads,
	in_files=in_files, 
	ref_files=ref_files,
	param_file=param_file,
	param_uri=param_uri,
	ref_uri=ref_uri,
	in_uri=in_uri,
	out_uri=out_uri,
	target_file=target_file)
if not set(in_files).issubset(set(os.listdir("."))):
	task.download_files("INPUT")
if not set(ref_files).issubset(set(os.listdir("."))):
	task.download_files("REF")
if param_file not in os.listdir("."):
	task.download_files("PARAMS")
if ome == "wes" and target_file not in os.listdir("."):
	task.download_files("TARGET")
task.build_cmd()
task.run_cmd()
task.upload_results()
task.cleanup()

end_time = datetime.now()
print "BASE RECAL TABLE for {} was ended at {}.".format(prefix, str(end_time))
total_time = end_time - start_time
print "Total time for BASE RECAL TABLE was {}.".format(str(total_time))
