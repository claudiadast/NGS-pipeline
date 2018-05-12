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
ref_files = []
in_files = ["{}.sorted.deduped.bam".format(prefix)]

start_time = datetime.now()
print "INDEX BAM for {} was started at {}.".format(prefix, str(start_time))

task = SDK.Task(
	step="index_bam",  
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
print "INDEX BAM for {} was ended at {}.".format(prefix, str(end_time))
total_time = end_time - start_time
print "Total time for INDEX BAM was {}.".format(str(total_time))
