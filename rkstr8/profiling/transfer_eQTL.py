'''
Makes a compute environment, job defs, etc to submit a large number of dummy jobs

'''
import boto3
import yaml
import time
import cmd
import sys
import os
import datetime
import uuid
import json
import string
from sqlitedict import SqliteDict
from pprint import pprint
from botocore.exceptions import ClientError
from threading import Thread
from argparse import ArgumentParser
from rkstr8.cloud.batch import BatchJobListStatusPoller

GVCF_GROUP = 0

batch_client = boto3.client('batch')

s3 = boto3.resource('s3')

psychcore_s3_resource = boto3.resource(
	's3',
	'aws_access_key_id'='AKIAIMGOJCQUTJZDEGZA',
	'aws_secret_access_key'='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'
	)

sandbox_s3_resource = boto3.resource(
	's3',
	'aws_access_key_id'='AKIAJHIIU2S6REARTSWA',
	'aws_secret_access_key'='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'
	)


psychcore_s3_client = boto3.client(
	's3',
	'aws_access_key_id'='AKIAIMGOJCQUTJZDEGZA',
	'aws_secret_access_key'='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'
	)

sandbox_s3_client = boto3.client(
	's3',
	'aws_access_key_id'='AKIAJHIIU2S6REARTSWA',
	'aws_secret_access_key'='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'
	)

asg_client = boto3.client('autoscaling')
ec2_client = boto3.client('ec2')

db_file = './trials.db.sqlite'

def unique_trial_name(base='batch-trial'):
	return '-'.join((base, str(uuid.uuid4())[:4]))

instance_cost_per_hour = {
	'm4.large': (0.1, 2),
	'm4.xlarge': (0.2, 4),
	'm4.2xlarge': (0.4, 8),
	'm4.4xlarge': (0.8, 16),
	'm4.10xlarge': (2, 40),
	'm4.16xlarge': (3.2, 64),
	'c4.large': (0.1, 2),
	'c4.xlarge': (0.19, 4),
	'c4.2xlarge': (0.40, 8),
	'c4.4xlarge': (0.80, 16),
	'c4.8xlarge': (1.6, 36),
	'r4.large': (0.13, 2),
	'r4.xlarge': (0.27, 4),
	'r4.2xlarge': (0.53, 8),
	'r4.4xlarge': (1.1, 16),
	'r4.8xlarge': (2.1, 32),
	'r4.16xlarge': (4.3, 64)

}

def createCompEnv(name, instances, minCPU, desiredvCpus, maxCPU, ami):
	try:
		response = batch_client.create_compute_environment(
			computeEnvironmentName=name,
			type="MANAGED",
			state="ENABLED",
			computeResources={
				"type" : "EC2",
				"desiredvCpus" : desiredvCpus,
				"minvCpus" : minCPU,
				"maxvCpus" : maxCPU,
				"instanceTypes" : instances,
				"imageId" : ami,
				"subnets" : [
					"subnet-78c18974", 
					"subnet-45f80d7a", 
					"subnet-1bafbf53",
					"subnet-85df88df",
					"subnet-a2885cc6",
					"subnet-f46538d8"],
				"securityGroupIds" : [
					"sg-54d99524"],
				"ec2KeyPair" : "LindsayKey",
				"instanceRole" : "arn:aws:iam::249640142434:instance-profile/ec2ContainerHolding",
			},
			serviceRole="arn:aws:iam::249640142434:role/service-role/AWSBatchServiceRole"
		)
		# time.sleep(40)
		return response
	except ClientError as e:
		if e.response['Error']['Message'] == "Object already exists":
			print("[ WARN ] Compute environment " + name + " already created!")
			pass

def createQ(name, compEnv, priority):
	try:
		response = batch_client.create_job_queue(
			jobQueueName=name,
			state="ENABLED",
			priority=priority,
			computeEnvironmentOrder=[
				# Can have multipe compute environments - scheduler
				# attempts to schedule from order 1, 2,...n
				{
					"order" : 1,
					"computeEnvironment" : compEnv
				}
			]
		)
		# time.sleep(30)
		return response
	except ClientError as e:
		if e.response['Error']['Message'] == "Object already exists":
			print("[ WARN ] Queue " + name + " already created!")
			pass

def defJob(name, container, vcpus, mem, cmd, containerVol, ec2Mount, attempts):
	try:
		response = batch_client.register_job_definition(
			jobDefinitionName=name,
			type="container",
			containerProperties={
				"image" : container,
				"vcpus" : vcpus,
				"memory" : mem,
				"command" : cmd,
				"jobRoleArn" : "arn:aws:iam::249640142434:role/AmazonEC2ContainerServiceTaskRole",
				"volumes" : [
					{
						"host" : {
							"sourcePath" : ec2Mount
						},
						"name" : "localDir"
					}],
				"mountPoints" : [
					{
						"containerPath" : containerVol,
						"readOnly" : False,
						"sourceVolume" : "localDir",
					}],
				"readonlyRootFilesystem" : False,
				'ulimits': [
			            {
			                'hardLimit': 90000,
			                'name': 'nofile',
			                'softLimit': 90000
			            },
			        ],
				"privileged" : False 
			},
			retryStrategy={
				"attempts" : attempts
			}
		)
		return response
	except ClientError as e:
		if e.response['Error']['Message'] == "Object already exists":
			print ("[ WARN ] Job definition " + name + " already created!")
			pass

def submitJob(name, queue, jobDef, dependsOn=False, containerOverrides=None):
	if dependsOn and not containerOverrides:
		return batch_client.submit_job(
		jobName=name,
		jobQueue=queue,
		dependsOn=dependsOn,
		jobDefinition=jobDef)
	elif not dependsOn and not containerOverrides:
		return batch_client.submit_job(
		jobName=name,
		jobQueue=queue,
		jobDefinition=jobDef)
	elif containerOverrides and not dependsOn:
		return batch_client.submit_job(
		jobName=name,
		jobQueue=queue,
		jobDefinition=jobDef,
		containerOverrides = containerOverrides)
	elif containerOverrides and dependsOn:
		return batch_client.submit_job(
		jobName=name,
		jobQueue=queue,
		dependsOn=dependsOn,
		jobDefinition=jobDef,
		containerOverrides = containerOverrides)

def wait_on_batch_resources_created(boto3_batch_fn, kw_args, response_key):
	success_set = set([('ENABLED', 'VALID')])
	creating_set = set(['CREATING'])
	creating_and_valid_set = set(['CREATING', 'VALID'])

	print(success_set)
	print(creating_set)
	print(creating_and_valid_set)

	while True:
		batch_response = boto3_batch_fn(**kw_args)

		print(batch_response)

		states_statuses = [(resource_config['state'], resource_config['status']) for resource_config in batch_response[response_key]]
		print(states_statuses)
		states_statuses_set = set(states_statuses)

		status_set = set([state_status[1] for state_status in states_statuses])

		if states_statuses_set == success_set:
			return True
		elif status_set == creating_set or status_set == creating_and_valid_set:
			time.sleep(1)
		else:
			print(status_set)
			print(states_statuses)
			raise Exception("Failed to create CE!")

def s3_obj_exists(bucket, key):
	try:
	    s3.Object(bucket, key).load()
	except botocore.exceptions.ClientError as e:
	    if e.response['Error']['Code'] == "404":
	        # The object does not exist.
	        return False
	    else:
	        print(e)
	else:
		return True

class AutoScalingGroupPoller(Thread):
	def __init__(self, asg_prefix):
		Thread.__init__(self)
		self.asg_prefix = asg_prefix
		# self.instances is a dict which contains instance_type : {instance_1 : (start, end), instance_2 : (start, end)}
		self.instances = {}
	
	def costOfAsg(self, costDict):
		print("****CALCULATING COST - INSTANCE TIMES:")
		pprint(self.instances)
		total_cost = 0
		instance_active_time = {}
		for instance_type in self.instances:
			instance_type_time = 0
			for instance in self.instances[instance_type]:
				instance_type_time += self.instances[instance_type][instance][1] - self.instances[instance_type][instance][0]
			# don't need to know how many instances of this type, only how many total hours
			# note: the instance_type_time is in seconds
			total_cost += costDict[instance_type][0] * (instance_type_time / 3600)
			print("TOTAL COST:", total_cost)
		return total_cost

	def run(self):
		asg_alive = False
		trial_asg = None
		while not asg_alive:
			all_asgs = asg_client.describe_auto_scaling_groups()
			for asg in all_asgs['AutoScalingGroups']:
				if asg['AutoScalingGroupName'].startswith(self.asg_prefix) and len(asg['Instances']) > 0:
					trial_asg_name = asg["AutoScalingGroupName"]
					asg_alive = True
			# Hit max retries: 4 for DescribeAutoScallingGroups - add sleep to reduce retries?
			time.sleep(5)

		while asg_alive:
			trial_asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[trial_asg_name])['AutoScalingGroups'][0]
			if trial_asg['Instances'] != []:
				# Get only instance ids
				trial_instance_ids = [instance_description['InstanceId'] for instance_description in trial_asg['Instances']]
				for instance_description in trial_asg['Instances']:
					instance_id = instance_description['InstanceId']
					instance_type_description = ec2_client.describe_instance_attribute(Attribute='instanceType', InstanceId=instance_id)
					instance_type = instance_type_description["InstanceType"]["Value"]
					if instance_type not in self.instances:
						# get time when instance was spun into asg
						start = time.time()
						self.instances[instance_type] = {instance_id : (start, 0)}
					elif instance_id not in self.instances[instance_type]:
						self.instances[instance_type][instance_id] = (start, 0)
				# can't assume that all instaces are in asg until asg dies, need to check if all are stil alive
				for instance_type in self.instances:
					for instance_in_asg in self.instances[instance_type]:
						if instance_in_asg not in trial_instance_ids:
							# an instance we've been keeping track of has scaled out
							end_time = time.time()
							self.instances[instance_type][instance_in_asg][1] = end_time

			else:
				# Need some way of detecting when all instances in asg are scaled out
				# This if will only be entered if asg_alive is True, aka when at least
				# one instance had been previously scaled in
				print("NO INSTANCES IN ASG")
				asg_alive = False
				# Get end time of asg for instance time lengths
			time.sleep(5)
		total_cost = self.costOfAsg(instance_cost_per_hour)

		 #Write cost analysis for all instances per trial to a file

		instance_cost_analysis = self.asg_prefix + "_instance_cost_analysis.json"
		for instance_type in self.instances:
			self.instances[instance_type] = list(self.instances[instance_type]) 
		
		with open(instance_cost_analysis, 'w') as f_writable:
			self.instances['queue_total_cost'] = total_cost
			json.dump(self.instances, f_writable)

		pprint(self.instances)

class StateChangePoller(Thread):

	def __init__(self, time_info_uri, jobIds_and_intervals, intervals, prefix, q):
		Thread.__init__(self)
		self.time_info_uri= time_info_uri
		self.jobIds_and_intervals = jobIds_and_intervals
		self.intervals = intervals
		self.jobIds = [pair[0] for pair in jobIds_and_intervals]
		self.jobDescriptionDict = {}
		self.q = q
		#self.instance_types = "_".join(q.split("_")[1:])
		self.prefix = prefix

	def merge_container_job_times(self, jobDescriptionDict):
		jobIds_and_intervals = self.jobIds_and_intervals
		sourceBucket = self.time_info_uri.split('/')[2]
		object_prefix = self.time_info_uri.split('/')[3:]

		for jobID_interval in jobIds_and_intervals:
			jobId = jobID_interval[0]
			interval = jobID_interval[1].split(".")[0]

			# print("JOBID", jobId)
			# print("INTERVAL", interval)

			container_time_info_name = "{}_{}_container_time_info.json".format(self.q, interval)

			if len(object_prefix) <= 1:
				if s3_obj_exists(sourceBucket, container_time_info_name):
					s3.Bucket(sourceBucket).download_file(container_time_info_name, container_time_info_name)

			else:
				new_object_prefix= object_prefix[:-1]
				new_object_prefix.append(container_time_info_name)
				if s3_obj_exists(sourceBucket, "/".join(new_object_prefix)):
					s3.Bucket(sourceBucket).download_file("/".join(new_object_prefix), container_time_info_name)

			job_state_change = jobDescriptionDict[jobId]

			pprint(job_state_change)

			with open(container_time_info_name, "r") as f:
				containerTimesDict = json.load(f)
				pprint(containerTimesDict)

				for state in job_state_change:
					containerTimesDict[state]= {
													"start": job_state_change[state][0],
													"end": job_state_change[state][1],
													"elapsed": job_state_change[state][2]
												}


			profile_file_name = container_time_info_name.split("_time")[0] + "_state_change_time_info.json"

			with open(profile_file_name, 'w') as f_writable:
				json.dump(containerTimesDict, f_writable)

			pprint(containerTimesDict)

	def run(self):
		jobIds = self.jobIds
		jobIds_and_intervals = self.jobIds_and_intervals
		jobDescriptionDict = self.jobDescriptionDict
		q = self.q
		intervals = self.intervals
		complete = False
		prev_states = {job_id:None for job_id in jobIds}

		while not complete:
			# a list of job descriptions for all jobs
			# get all job statuses for this loop iteration
			for sublist_of_jobs in BatchJobListStatusPoller.split_list(jobIds, 100):
				partial_job_descriptions = batch_client.describe_jobs(jobs=sublist_of_jobs)
				for job_description in partial_job_descriptions['jobs']:
					curr_state = job_description["status"]
					jobId = job_description["jobId"]
					if prev_states[jobId] == None:
						# In the submitted state
						print("{} WAS JUST SUBMITTED".format(jobId))
						jobDescriptionDict[jobId] =  {
							"SUBMITTED" : [0,0,0],
							"PENDING" : [0,0,0],
							"RUNNABLE" : [0,0,0],
							"STARTING" : [0,0,0],
							"RUNNING" : [0,0,0],
							"SUCCEEDED" : [0,0,0],
							"FAILED" : [0,0,0]}
						jobDescriptionDict[jobId]["SUBMITTED"][0] = time.time()
						prev_states[jobId] = "SUBMITTED"
					elif job_description['status'] != prev_states[jobId]:
						print("STATE HAS CHANGED FROM {} TO {} IN JOB {}".format(prev_states[jobId], job_description['status'], jobId) )
						# start time
						jobDescriptionDict[jobId][curr_state][0] = time.time()
						# end time
						jobDescriptionDict[jobId][prev_states[jobId]][1] = time.time()
						prev_states[jobId] = curr_state
					elif (job_description['status'] == "SUCCEEDED" or job_description['status'] == "FAILED") \
						and job_description['status'] == prev_states[jobId]:
						continue
			if set(prev_states.values()) == set(['SUCCEEDED']) or \
				set(prev_states.values()) == set(['FAILED']) or \
				set(prev_states.values()) == set(['SUCCEEDED', 'FAILED']):
				print("COMPLETE")
				complete = True

		for job in jobDescriptionDict:
			for state in jobDescriptionDict[job]:
				jobDescriptionDict[job][state][2] = jobDescriptionDict[job][state][1] - jobDescriptionDict[job][state][0]

		self.merge_container_job_times(jobDescriptionDict=jobDescriptionDict)

def merge_jobs_from_queue_w_cost(time_json_objs, cost_json_obj):
		'''
		Merges the container and state change time info dictionaries for all 
		jobs in a queue and adds the cost for all the jobs in the queue to run.
		'''
		# Calculate total cost
		q_cost_time = {}
		result_file = "cost_time_analysis_{}.json".format("".join(time_json_objs[0].split("-q")[0:2]))
		for time_json_obj in time_json_objs:
			with open(time_json_obj, "r") as t:
				job_interval = "_".join(time_json_obj.split("_")[3:5])
				q_cost_time[job_interval] = json.load(t)
		with open(cost_json_obj) as c:
			q_cost_time["cost"] = json.load(c)["queue_total_cost"]

		with open(result_file, "w") as wf:
			json.dump(q_cost_time, wf)

def interval_filenames(bucket='test-references', prefix='intervals_GRCh37_150000bp/4_'):
	"""
	Returns a list of s3 object key suffixes at specified bucket/prefix, possibly empty.
	"""
	paginator = s3_client.get_paginator('list_objects')
	page_iterator = paginator.paginate(
		Bucket=bucket,
		Delimiter='/',
		Prefix=prefix
	)

	interval_key_suffixes = []

	for page in page_iterator:
		for s3_obj in page['Contents']:
			key = s3_obj['Key']
			suffix = key.split('/')[-1] if '/' in key else key
			interval_key_suffixes.append(suffix)

	return interval_key_suffixes

def fastq_filenames(bucket, prefix, account):
	"""
	Returns a list of s3 object key suffixes at specified bucket/prefix, possibly empty.
	"""
	if account = "psychcore":
		paginator = psychcore_s3_client.get_paginator('list_objects')
	if account = "sandbox":
		paginator = sandbox_s3_client.get_paginator('list_objects')
	page_iterator = paginator.paginate(
		Bucket=bucket,
		Delimiter='/',
		Prefix=prefix
	)

	fastq_key_suffixes = []

	for page in page_iterator:
		for s3_obj in page['Contents']:
			key = s3_obj['Key']
			suffix = key.split('/')[-1] if '/' in key else key
			fastq_key_suffixes.append(suffix)

	return fastq_key_suffixes

def create_resources(user_config, do_create_job_def=False):
	"""
	[
		{
			'trial': trial,
			'compute_environment': {
				'name': ...
			},
			'queue': {
				'name': ...,
				'arn': ...
			}
		}
	}
	"""

		# TODO: wait on job_def create...
		# But docs don't show what the job_def_status values are...

	trial_names = [trial["trial_name"] for trial in user_config["trials"]]
	compute_environment_names = ['-'.join((trial_name, 'ce')) for trial_name in trial_names]
	queue_names = ['-'.join((trial_name, 'q')) for trial_name in trial_names]

	trial_resource_configs = []
	for trials_names in zip(user_config['trials'], compute_environment_names, queue_names):
		trial = trials_names[0]
		compute_environment_name = trials_names[1]
		queue_name = trials_names[2]

		trial_resource_configs.append(
			{
				'trial': trial,
				'compute_environment': {
					'name': compute_environment_name
				},
				'queue': {
					'name': queue_name,
					'arn': None
				}
			}
		)

	if do_create_job_def:
		for config in trial_resource_configs:
			trial = config['trial']
			print('Creating job def...')
			job_def_name = "haplotypegenotype_psychcore_{}".format(trial["trial_name"])
			trial['job_def'] = job_def_name
			response = defJob(
				job_def_name,
				user_config["container"],
				trial["job_specs"]["vcpus"],
				trial["job_specs"]["mem"],
				["bash", "/haplotype_genotype/haplotype_genotype_setup.sh"],
				"/haplotype_genotype/localDir",
				"/mnt/data/localDir",
				2
			)
		pprint(response)

	some_create_failed = False

	creating_ces = []
	for trial_config in trial_resource_configs:
		pprint(trial_config)
		trial = trial_config['trial']
		pprint(trial)
		print('Creating resources for trial {}...'.format(trial['trial_name']))
		print('Creating compute env...')
		response = createCompEnv(
			name=trial_config['compute_environment']['name'],
			instances=trial['instance_types'],
			minCPU=trial['vcpu_specs'][0],
			desiredvCpus=trial['vcpu_specs'][1],
			maxCPU=trial['vcpu_specs'][2],
			# ami='ami-a2dd82d8'
			ami=trial['ami']
		)
		if response:
			pprint(response)
			creating_ces.append(trial_config['compute_environment']['name'])
		else:
			some_create_failed = True
			print('Failed to create ce.')

	print('Waiting on compute envs...')
	wait_on_batch_resources_created(
		boto3_batch_fn=batch_client.describe_compute_environments,
		kw_args={'computeEnvironments': creating_ces},
		response_key='computeEnvironments'
	)

	creating_qs = []
	for trial_config in trial_resource_configs:
		print('Creating Qs...')
		# Note: queue name can't have "." in it
		response = createQ(
			name=trial_config['queue']['name'] + "_" + "_".join([str(i) for i in trial_config['trial']['instance_types']]).replace(".", "_"),
			compEnv=trial_config['compute_environment']['name'],
			priority=10
		)
		if response:
			pprint(response)
			trial_config['queue']['arn'] = response['jobQueueArn']
			creating_qs.append(response['jobQueueArn'])
		else:
			some_create_failed = True
			print('Failed to create q.')

	print('Waiting on Qs...')
	wait_on_batch_resources_created(
		boto3_batch_fn=batch_client.describe_job_queues,
		kw_args={'jobQueues': creating_qs},
		response_key='jobQueues'
	)

	return some_create_failed, trial_resource_configs

def submit_bam_variant_scatter_gather(trial_resource_configs, user_config):

	for trial_config in trial_resource_configs:
		trial = trial_config['trial']
		job_def = trial['job_def']
		num_intervals = trial['num_intervals']
		intervalFiles_chr4 = interval_filenames(bucket='test-references', prefix='intervals_GRCh37_150000bp/4_')
		intervalFiles_chr5 = interval_filenames(bucket='test-references', prefix='intervals_GRCh37_150000bp/5_')
		intervalFiles= intervalFiles_chr4 + intervalFiles_chr5
		q = trial_config['queue']['name'] + "_" + "_".join([str(i) for i in trial_config['trial']['instance_types']]).replace(".", "_")
		# Just define base_recal_def here:
		#def defJob(name, container, vcpus, mem, cmd, containerVol, ec2Mount, attempts)
		base_recal_def = defJob("bam_scatter",
						"lindsayliang/base_recal",
						14, 30000, ["bash", "/base_recal/base_recal_setup.sh"],
						"/base_recal/localDir", "/mnt/data/localDir", 2)


		jobs_per_queue = []

		# 1. Submit base_recal job for single sample to create and push interval-bams to s3
		
		base_recal_job = submitJob("bam_scatter_" + trial["prefix"], q, "bam_scatter", 
									containerOverrides={"environment": [
										  { "name":"inBucket", "value":"s3://pipeline-validation/bam-variant-scatter/exome/" },
										  # Use a different env variable so we can still run GATK's base_recal
										  { "name":"bamBucket", "value":"s3://pipeline-validation/bam-variant-scatter/exome/" },
										  { "name":"intervalBucket", "value":"s3://test-references/intervals_GRCh37_150000bp/" },
										  { "name":"splitBamBucket", "value":"s3://pipeline-validation/bam-variant-scatter/exome/split-bams/" },
										  { "name":"numWorkers", "value":"28" },
										  { "name":"prefix", "value":trial["prefix"]}]})

		# 2. Submit haplotype_genotype job for each of the interval-bams generated in step 1
		# Assumption:  A fixed number of interval-bams were written in a single location
		# Eg. There's an interval-bam for each GRCh37 interval present
		# Todo 
		for i in range(0, len(intervalFiles)):
			interval = intervalFiles[i].split(".")[0]
			interval_bam_name = "{}_{}".format(trial["prefix"], interval)

			haplogeno_job = submitJob("haplogeno_" + str(i), q, job_def,
									  containerOverrides={"environment": [
										  { "name":"inBucket", "value":"s3://pipeline-validation/bam-variant-scatter/exome/split-bams/" },
										  { "name":"outBucket", "value":"s3://pipeline-validation/bam-variant-scatter/exome/haplo-geno-output/" },
										  { "name":"refBucket", "value":"s3://test-references/" },
										  { "name":"timeBucket", "value":user_config["time-info-uri"] },
										  { "name":"intervalBucket", "value":"s3://test-references/intervals_GRCh37/" },
										  { "name":"bufferVCFBucket", "value":"s3://test-references/exome-buffers/" },
										  { "name":"intervalFile", "value":intervalFiles[i] },
										  # interal bam name given as prefix for bam download from s3
										  { "name":"prefix", "value":interval_bam_name },
										  { "name":"trial", "value":q }]},
										  dependsOn=[{"jobId":base_recal_job["jobId"]}])

			jobs_per_queue.append((haplogeno_job["jobId"], intervalFiles[i]))

def submit_jobs_profiled(trial_resource_configs, user_config):

	#stateChangeDict = {}

	state_change_pollers = []
	asg_pollers = []

	for trial_config in trial_resource_configs:
		trial = trial_config['trial']
		job_def = trial['job_def']
		num_intervals = trial['num_intervals']
		intervalFiles = interval_filenames()[:num_intervals]
		q = trial_config['queue']['name'] + "_" + "_".join([str(i) for i in trial_config['trial']['instance_types']]).replace(".", "_")

		jobs_per_queue = []

		for i in range(0, len(intervalFiles)):
			haplogeno_job = submitJob("haplogeno_" + str(i), q, job_def,
									  containerOverrides={"environment": [
										  { "name":"inBucket", "value":user_config["in-bucket-uri"] },
										  { "name":"outBucket", "value":user_config["out-bucket-uri"] },
										  { "name":"refBucket", "value":"s3://test-references/" },
										  { "name":"timeBucket", "value":user_config["time-info-uri"] },
										  { "name":"intervalBucket", "value":"s3://test-references/intervals_GRCh37/" },
										  { "name":"bufferVCFBucket", "value":user_config["buffer-vcf-uri"] },
										  { "name":"intervalFile", "value":intervalFiles[i] },
										  { "name":"prefix", "value":trial["prefix"] },
										  { "name":"trial", "value":q }]})
			jobs_per_queue.append((haplogeno_job["jobId"], intervalFiles[i]))
		
		asg_prefix = q.split("-q")[0]
		print("ASG_PREFIX", asg_prefix)
		
		getStateChangesManyJobs = StateChangePoller(
			time_info_uri = user_config["time-info-uri"],
			jobIds_and_intervals=jobs_per_queue,
			intervals=intervalFiles[:],
			prefix="HG001.21",
			q=q
		)
		getASGCost = AutoScalingGroupPoller(asg_prefix=asg_prefix)

		state_change_pollers.append(getStateChangesManyJobs)
		asg_pollers.append(getASGCost)

		print('Starting threads {} and {}'.format(state_change_pollers, asg_pollers))
		getStateChangesManyJobs.start()
		getASGCost.start()
		

		print('Joining on {}'.format(state_change_pollers))
		print('Joining on {}'.format(asg_pollers))
		_ = [res for res in map(Thread.join, state_change_pollers)]
		__ = [res for res in map(Thread.join, asg_pollers)]
		print('Joined.')

		# Merging time info jsons and cost json
		time_files = []
		for intervalFile in intervalFiles:
			interval = intervalFile.split(".")[0]
			file_name = "{}_{}_container_state_change_time_info.json".format(q, interval)
			if os.path.exists(file_name):
				time_files.append(file_name)
		
		cost_file = "{}_instance_cost_analysis.json".format(asg_prefix)

		#print("TIME FILES:", str(time_files))
		#print("COSTFILE", cost_file)

		merge_jobs_from_queue_w_cost(time_files, cost_file)

def poll_and_submit_bwa(trial_resource_configs, user_config):
	for trial_config in trial_resource_configs:
			trial = trial_config['trial']
			q = trial_config['queue']['name'] + "_" + "_".join([str(i) for i in trial_config['trial']['instance_types']]).replace(".", "_")

	bwa_def = defJob("bwa_mem", q, 
		"lindsayliang/bwa_mem",
		36, 60000, ["bash", "/bwa_mem/bwa_mem_setup.sh"],
		"/bwa_mem/localDir", "/mnt/data/localDir/", 2)

	#TEST BWA CONTAINER
	bwaJob = submitJob(
		name="bwa_test",
		queue=q,
		jobDef=bwa_def,
		containerOverrides={"environment": [
						  { "name":"inBucket", "value":"s3://pipeline-validation/smallfq/" },
						  { "name":"outBucket", "value":"s3://pipeline-validation/smallfq/one-off/" },
						  { "name":"refBucket", "value":"s3://test-references/" },
						  { "name":"R1", "value":"D680TT_R1.fastq.gz" },
						  { "name":"R2", "value":"D680TT_R2.fastq.gz" },
						  { "name":"prefix", "value":"D680TT"}]})

	#avail_fastqs is a dict : {sample : (True, True)}
	# avail_fastqs = {}
	# already_submitted_fastqs = set()
	# while True:
	# 	curr_sest_bucket_contents = fastq_filenames(
	# 		bucket='sestan-lab-share', 
	# 		prefix='eQTL_sestan-lab-wgs/BK1704031_R3_Batch1/', 
	# 		account='psychcore')
	# 	for file in curr_sest_bucket_contents:
	# 		sample_name = file.split("_R")[0]
	# 		R = file.split("_R")[1][0]
	# 		if sample_name not in avail_fastqs:
	# 			# Neither R1 or R2 of this sample were previously available
	# 			if R = "1":
	# 				avail_fastqs[sample_name] = (True, False)
	# 			elif R = "2":
	# 				avail_fastqs[sample_name] = (False, True)
	# 		elif avail_fastqs[sample_name] == (True, True):
	# 			# Both R1 and R2 are currently available
	# 			if sample_name not in already_submitted_fastqs:
	# 				already_submitted_fastqs.add(sample_name)
					
	# 				# Download from psychcore
	# 				sourceBucket = 'sestan-lab-share'
	# 				R1_fastq_name_clean = '{}_R1.fastq.gz'.format(sample_name)
	# 				R2_fastq_name_clean = '{}_R2.fastq.gz'.format(sample_name)

	# 				obj = 'eQTL_sestan-lab-wgs/BK1704031_R3_Batch1/{}'.format(file)
	# 				psychcore_s3_resource.Bucket('sestan-lab-share').download_file(obj, R1_fastq_name_clean)
	# 				psychcore_s3_resource.Bucket('sestan-lab-share').download_file(obj, R2_fastq_name_clean)

	# 				# Upload to sandbox
	# 				dest_bucket = 'brain-eQTL'
	# 				R1_dest_obj_prefix = "/fastqs/{}".format(R1_fastq_name_clean)
	# 				R2_dest_obj_prefix = "/fastqs/{}".format(R2_fastq_name_clean)

	# 				sandbox_s3_resource.Bucket(dest_bucket).upload_file(dest_obj_prefix, R1_fastq_name_clean)
	# 				sandbox_s3_resource.Bucket(dest_bucket).upload_file(dest_obj_prefix, R2_fastq_name_clean)

	# 				# Submit job
	# 				#def submitJob(name, queue, jobDef, dependsOn=False, containerOverrides=None)
					
	# 				bwaJob = submitJob(
	# 					name="bwa_{}".format(sample_name),
	# 					queue=q,
	# 					jobDef=bwa_def,
	# 					containerOverrides={"environment": [
	# 									  { "name":"inBucket", "value":"s3://brain-eQTL/fastqs/" },
	# 									  { "name":"outBucket", "value":"s3://brain-eQTL/sam/" },
	# 									  { "name":"refBucket", "value":"s3://test-references/" },
	# 									  { "name":"R1", "value":R1_fastq_name_clean },
	# 									  { "name":"R2", "value":R2_fastq_name_clean },
	# 									  { "name":"prefix", "value":sample_name}]})
	# 			else:
	# 				continue
	# 		else:
	# 			# Only one of R1 and R2 is available
	# 			if R = "1":
	# 				avail_fastqs[sample_name][0] = True
	# 			elif R = "2":
	# 				avail_fastqs[sample_name][1] = True

	# 	time.sleep(30)





class DeleteResourceLoop(cmd.Cmd):

	def __init__(self, db_file):
		super().__init__()
		self.db_file = db_file

	def key_list(self, run_dict):
		keys = list(run_dict.keys())
		keys.sort()
		return keys

	def do_l(self, line):
		with SqliteDict(db_file) as run_dict:
			keys = self.key_list(run_dict)
			for i, run_key in enumerate(keys):
				print("{}: {}".format(i, run_key))

	def do_d(self, number):
		number = int(number)
		with SqliteDict(db_file) as run_dict:
			num_runs = len(run_dict)
			if number > -1 and number < num_runs:
				keys = self.key_list(run_dict)
				key_to_describe = keys[number]
				trial_resource_configs = run_dict[key_to_describe]
				self.describe_resources(trial_resource_configs)

	def describe_resources(self, trial_configs):
		for trial_config in trial_configs:
			queue_arn = trial_config['queue']['arn']
			queue_name = trial_config['queue']['name']
			ce_name = trial_config['compute_environment']['name']

			describe_ce_response = batch_client.describe_compute_environments(computeEnvironments=[ce_name])
			if len(describe_ce_response['computeEnvironments']) > 0:
				print("{}: ({},{})".format(
					ce_name,
					describe_ce_response['computeEnvironments'][0]['status'],
					describe_ce_response['computeEnvironments'][0]['state']
				))
			else:
				print("{}: Not found".format(ce_name))

			describe_queue_response = batch_client.describe_job_queues(jobQueues=[queue_arn])
			if len(describe_queue_response['jobQueues']) > 0:
				print("{}: ({},{})".format(
					queue_name,
					describe_queue_response['jobQueues'][0]['status'],
					describe_queue_response['jobQueues'][0]['state']
				))
			else:
				print("{}: Not found".format(queue_name))

	def do_del(self, number):
		number = int(number)
		if number > -1:
			with SqliteDict(db_file) as run_dict:
				num_runs = len(run_dict)
				if number > -1 and number < num_runs:
					keys = list(run_dict.keys())
					keys.sort()
					key_to_describe = keys[number]
					trial_resource_configs = run_dict[key_to_describe]
					print('Deleting...')
					self.delete_resources(trial_resource_configs)

	def delete_resources(self, trial_configs):
		for trial_config in trial_configs:
			queue_arn = trial_config['queue']['arn']
			queue_name = trial_config['queue']['name']
			ce_name = trial_config['compute_environment']['name']

			print('Disabling q: {}'.format(queue_name))

			# first check if exists...
			response = batch_client.describe_job_queues(
				jobQueues=[queue_arn]
			)
			alter_q = False
			if response:
				if len(response['jobQueues']) > 0:
					status = response['jobQueues'][0]['status']
					alter_q = status == 'VALID' or status == 'INVALID'

			if alter_q:
				response = batch_client.update_job_queue(
					jobQueue=queue_arn,
					state='DISABLED'
				)

				if response:
					disabled = False
					while not disabled:
						response = batch_client.describe_job_queues(
							jobQueues=[queue_arn]
						)
						if response:
							#pprint(response)
							if len(response['jobQueues']) > 0:
								if response['jobQueues'][0]['state'] == 'DISABLED' and response['jobQueues'][0]['status'] == 'VALID':
									disabled = True
						time.sleep(1)

				print('Deleting q: {}'.format(queue_name))
				response = batch_client.delete_job_queue(
					jobQueue=queue_arn
				)

				if response:
					deleted = False
					while not deleted:
						response = batch_client.describe_job_queues(
							jobQueues=[queue_arn]
						)
						if response:
							#pprint(response)
							if len(response['jobQueues']) > 0:
								if response['jobQueues'][0]['status'] == 'DELETED':
									deleted = True
							else:
								deleted = True
						time.sleep(1)

			print('Deleting: {}'.format(ce_name))
			print('Disabling q: {}'.format(queue_name))

			# first check if exists...
			response = batch_client.describe_compute_environments(
				computeEnvironments=[ce_name]
			)
			alter_ce = False
			if response:
				if len(response['computeEnvironments']) > 0:
					# only alter if it is stationary
					status = response['computeEnvironments'][0]['status']
					alter_ce = status == 'VALID' or status == 'INVALID'

			if alter_ce:
				response = batch_client.update_compute_environment(
					computeEnvironment=ce_name,
					state='DISABLED'
				)

				if response:
					disabled = False
					while not disabled:
						response = batch_client.describe_compute_environments(
							computeEnvironments=[ce_name]
						)
						if response:
							#pprint(response)
							if len(response['computeEnvironments']) > 0:
								if response['computeEnvironments'][0]['state'] == 'DISABLED' and response['computeEnvironments'][0]['status'] == 'VALID':
									disabled = True
						time.sleep(1)

				print('Deleting ce: {}'.format(ce_name))
				response = batch_client.delete_compute_environment(
					computeEnvironment=ce_name
				)

				if response:
					deleted = False
					while not deleted:
						response = batch_client.describe_compute_environments(
							computeEnvironments=[ce_name]
						)
						if response:
							#pprint(response)
							if len(response['computeEnvironments']) > 0:
								if response['computeEnvironments'][0]['status'] == 'DELETED':
									deleted = True
							else:
								deleted = True
						time.sleep(1)

	def do_EOF(self, line):
		return True

def delete_resources_manager():
	DeleteResourceLoop(db_file=db_file).cmdloop()

def persist_resource_configs(db_file, trial_resource_configs):
	"""
	{
		'<run_date>': trial_resource_config
	}
	Args:
	    trial_resource_configs:

	Returns:

	"""
	now = datetime.datetime.now()
	run_key = 'Run {}-{}-{} {}:{}'.format(
		now.year,
		now.month,
		now.day,
		now.hour,
		now.minute
	)
	with SqliteDict(db_file) as run_dict:
		run_dict[run_key] = trial_resource_configs
		run_dict.commit(blocking=True)

def get_configuration(config):
	with open(config, "r") as y:
		config_dict = yaml.load(y)
		pprint(config_dict)
		for trial in config_dict['trials']:
			trial['trial_name'] = unique_trial_name()
	return config_dict

def parse_args():
	parser = ArgumentParser()
	parser.add_argument('-c', '--config', type=str, required=True)
	parser.add_argument('-m', '--manager', action='store_true', required=False)
	parser.add_argument('-j', '--register-job-def', action='store_true', required=False)

	args = parser.parse_args()
	return args.manager, args.register_job_def, args.config

def main_profiler(do_manager, register_job_def, config):

	if do_manager:
		delete_resources_manager()
		sys.exit(0)
	else:
		configuration = get_configuration(config)
		print('Creating Resources...')
		err, trial_resource_configs = create_resources(configuration, register_job_def)

		print('Persisting...')
		persist_resource_configs(
			db_file=db_file,
			trial_resource_configs=trial_resource_configs
		)

		if not err:
			print('Submitting jobs...')
			#submit_jobs_profiled(trial_resource_configs, configuration)
			#submit_bam_variant_scatter_gather(trial_resource_configs, configuration)
			poll_and_submit_bwa(trial_resource_configs, configuration)


if __name__ == '__main__':
	do_manager, register_job_def, config = parse_args()
	main_profiler(
		do_manager,
		register_job_def,
		config
	)

