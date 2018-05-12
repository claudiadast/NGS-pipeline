import SDK
import os
from datetime import datetime

#Check that all req env variables are set
env_vars = ["prefix", "param_uri", "ref_uri", "in_uri", "out_uri"]

if not set(env_vars).issubset(set(os.environ)):
	print "Environment not configured properly."
	raise ValueError("The required env variables are: {}".format(", ".join(env_vars)))

if "suffix" in os.environ:
	suffix = "{}.fastq.gz".format(os.environ["suffix"])
else:
	suffix = ".fastq.gz"
prefix = os.environ["prefix"]
param_uri = os.environ["param_uri"]
param_file = param_uri.split("/")[-1]
ref_uri = os.environ["ref_uri"]
in_uri = os.environ["in_uri"]
out_uri = os.environ["out_uri"]
build = os.environ["build"]
if build == "GRCh38":
	ref_files = ["Homo_sapiens_assembly38.fasta", 
		"Homo_sapiens_assembly38.fasta.64.amb", 
		"Homo_sapiens_assembly38.fasta.64.ann",
		"Homo_sapiens_assembly38.fasta.64.bwt", 
		"Homo_sapiens_assembly38.fasta.64.pac",
		"Homo_sapiens_assembly38.fasta.64.sa", 
		"Homo_sapiens_assembly38.fasta.64.alt"]
elif build == "GRCh37":
	ref_files = ["human_g1k_v37.fasta",
		"human_g1k_v37.fasta.amb",
		"human_g1k_v37.fasta.ann",
		"human_g1k_v37.fasta.bwt",
		"human_g1k_v37.fasta.fai",
		"human_g1k_v37.fasta.pac",
		"human_g1k_v37.fasta.sa"]
else: 
	raise ValueError("Unrecognized build - environment variable mode must be GRCh38 or GRCh37")
R1 = "{}_R1{}".format(prefix, suffix)
R2 = "{}_R2{}".format(prefix, suffix)
in_files = [R1, R2]
threads = os.environ['threads']

start_time = datetime.now()
print "BWA MEM for {} was started at {}.".format(prefix, str(start_time))

task = SDK.Task(
	step="bwa_mem",  
	prefix=prefix, 
	threads=threads,
	in_files=in_files, 
	ref_files=ref_files,
	param_file=param_file,
	param_uri=param_uri,
	ref_uri=ref_uri,
	in_uri=in_uri,
	out_uri=out_uri)
dir_contents = os.listdir(".")

print "Current dir contents: {}".format(str(dir_contents))
if not set(in_files).issubset(set(dir_contents)):
	task.download_files("INPUT")
if not set(ref_files).issubset(set(dir_contents)):
	task.download_files("REF")
if param_file not in dir_contents:
	task.download_files("PARAMS")
task.build_cmd()
task.run_cmd()
task.upload_results()
task.cleanup()

end_time = datetime.now()
print "BWA MEM for {} ended at {}.".format(prefix, str(end_time))
total_time = end_time - start_time
print "Total time for BWA MEM was {}.".format(str(total_time))
