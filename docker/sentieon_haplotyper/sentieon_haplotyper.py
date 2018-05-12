import SDK
import os
from datetime import datetime

#Check that all req env variables are set
env_vars = ["prefix", "param_uri", "ref_uri", "in_uri", "out_uri", "license_uri"]

if not set(env_vars).issubset(set(os.environ)):
	print "Environment not configured properly."
	raise ValueError("The required env variables are: {}".format(", ".join(env_vars)))
	
prefix = os.environ["prefix"]
param_uri = os.environ["param_uri"]
param_file = param_uri.split("/")[-1]
ref_uri = os.environ["ref_uri"]
in_uri = os.environ["in_uri"]
out_uri = os.environ["out_uri"]
sentieon_pkg = os.environ["sentieon_pkg"]
license_uri = os.environ["license_uri"]
license_file = license_uri.split("/")[-1]
build = os.environ["build"]
ome = os.environ["ome"]
if build == "GRCh38":
	ref_files = ["Homo_sapiens_assembly38.fasta", 
		"Homo_sapiens_assembly38.fasta.fai",
		"Homo_sapiens_assembly38.dbsnp138.vcf",
		"Homo_sapiens_assembly38.dbsnp138.vcf.idx"]
elif build == "GRCh37":
	ref_files = ["human_g1k_v37.fasta", 
		"human_g1k_v37.fasta.fai",
		"dbsnp_138.b37.vcf",
		"dbsnp_138.b37.vcf.idx"]
else:
	raise ValueError("Unrecognized build - environment variable build must be GRCh38 or GRCh37")
if ome == "wes":
	target_file = os.environ["target_file"]
	ref_files.append(target_file)
else:
	target_file = None
bam = "{}.sorted.deduped.recalibrated.bam".format(prefix)
bai = "{}.sorted.deduped.recalibrated.bai".format(prefix)
in_files = [bam, bai]
threads = os.environ['threads']

start_time = datetime.now()
print "Sentieon's HAPLOTYPER for {} was started at {}.".format(prefix, str(start_time))

task = SDK.Task(
	step="haplotyper",  
	prefix=prefix, 
	threads=threads,
	in_files=in_files, 
	ref_files=ref_files,
	param_file=param_file,
	sentieon_pkg=sentieon_pkg,
	license_file=license_file,
	license_uri=license_uri,
	param_uri=param_uri,
	ref_uri=ref_uri,
	in_uri=in_uri,
	out_uri=out_uri, 
	target_file=target_file)
if not set(in_files).issubset(set(os.listdir("."))):
	task.download_files("INPUT")
if not set(ref_files).issubset(set(os.listdir("."))):
	task.download_files("REF")
if license_file not in os.listdir("."):
	task.download_files("SENTIEON")
if param_file not in os.listdir("."):
	task.download_files("PARAMS")
if ome == "wes" and target_file not in os.listdir("."):
	task.download_files("TARGET")
task.build_cmd()
task.run_cmd()
task.upload_results()
task.cleanup()

end_time = datetime.now()
print "Sentieon's HAPLOTYPER for {} ended at {}.".format(prefix, str(end_time))
total_time = end_time - start_time
print "Total time for Sentieon's HAPLOTYPER was {}.".format(str(total_time))
