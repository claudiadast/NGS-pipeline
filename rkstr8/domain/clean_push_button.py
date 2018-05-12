from rkstr8.cloud.lambda_ import Lambda
from rkstr8.cloud.s3 import S3Upload
from rkstr8.cloud.stepfunctions import StepFunctionsMachine
from rkstr8.cloud.cloudformation import StackLauncher, Stack, TemplateProcessor, CloudFormationTemplate
from rkstr8.domain.pipelines import MultiSampleWGSRefactoredNested
from rkstr8.cloud import json_serial, TemplateValidationException, TimeoutException
import rkstr8.conf as conf

import logging
import shutil
import uuid
import json
from pprint import pprint
from string import Template
from pathlib import Path
import tempfile
import os
import yaml

#
# Configure the logger for this module
#
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s [%(levelname)s]: %(message)s')

#
# Mapping of end-user pipeline names to PipelineSpecification implementation classes
#
pipelines = {
    'germline_wgs': MultiSampleWGSRefactoredNested,
}


def pipeline_for(pipeline_name, conf):
    """
    Use the pipelines map to return a pipeline object given a name.
    :param pipeline_name: Str
    :return: pipeline: PipelineSpecification subclass
    """
    try:
        pipeline_cls = pipelines[pipeline_name]
    except KeyError:
        raise Exception('No pipeline named: {}. Try {}.'.format(
            pipeline_name, list(pipelines.keys())
        ))
    else:
        return pipeline_cls(conf)


def render_templates(pipeline, conf):
    """
    Render the partial CloudFormation templates into valid templates

    :return: None
    """
    logging.debug('Building StateMachine from Pipeline spec...')

    #
    # Invoke SFN StateMachine definition to get rkstr8.cloud.stepfunctions_mx.StateMachine
    # Also get associated StateMachine resource substitution dictionary
    #
    state_machine = pipeline.build_machine()
    state_machine_dict = state_machine.build()
    state_machine_json = json.dumps(state_machine_dict, indent=2, sort_keys=False)
    substitutions_dict = pipeline.build_substitutions()

    logging.debug('Rendering templates...')

    #
    # Read in Template fragments for CFN StateMachine and JobDefinitions
    #

    # StateMachine CFN Resource from json fragment. Add StateMachine definition and substitutions.

    with open('templates/fragments/statemachine.json') as frag_fh:
        state_machine_resource_tmpl = frag_fh.read().replace('\n', '')
        state_machine_resource = json.loads(state_machine_resource_tmpl)
        # Set the DefinitionString to the machine json and its substitutions
        state_machine_resource['Properties']['DefinitionString']['Fn::Sub'] = [state_machine_json, substitutions_dict]

    # StateMachine CFN Resource from json fragment. Add StateMachine definition and substitutions.

    job_def_templates_dir = Path('templates/fragments')
    job_def_templates = dict()
    for job_def_templ in job_def_templates_dir.glob('job_def_template.json'):
        with open(str(job_def_templ)) as frag_fh:
            key = job_def_templ.name.rstrip('.json')
            job_def_templates[key] = Template(frag_fh.read().replace('\n', ''))

    job_def_names = []
    job_defs = []

    for job in conf["CONTAINER_NAMES"]:
        #job_def_template_key = container_name_job_def_template[job]
        job_def_template_key = "job_def_template.json"
        job_def_resource_tmpl = job_def_templates[job_def_template_key]
        job_def_names.append('{}JobDef'.format(job.replace("_","")))
        job_uid = job + str(uuid.uuid4())[0:4]
        job_def_resource_str = job_def_resource_tmpl.substitute(job=job, job_uid=job_uid)
        job_def = json.loads(job_def_resource_str)
        pprint(job_def)
        job_defs.append(job_def)

    #
    # Render Resource definitions onto Base template to create effective (final) CFN Template
    #

    templates = ['templates/rkstr8.base.yaml']

    # Define the template rendering pipeline
    rendered_templates = TemplateProcessor(templates)\
        .from_yaml(as_path=True)\
        .add_resource(conf["CFN_FSA_LOGICAL_RESOURCE_ID"], state_machine_resource)\
        .add_resources(job_def_names, job_defs)\
        .to_yaml()

    # Run the template rendering pipeline, leaving results in list
    final_template = list(rendered_templates)[0]

    with open("templates/rkstr8.rendered.test.yaml", "w") as w:
        w.write(final_template)

    return {
        'launch': final_template
    }


def render_templates_nested(pipeline, conf):

    # TODO: Render the Statemachine and it's substitutions onto the SFN stack
    # TODO: Update nested pipeline to use Fn::ImportValue in substitutions
    # TODO: Render the JobDefs onto the Batch stack
    # TODO: Decide what this returns to the caller
    #           i. create_resources requires the parent template as string, so that needs to be returned
    #          ii. stage_assets_nested will require the rendered templates as strings for validation, so return those
    #         iii. could just return ALL the templates as strings, in dict. <---

    logging.debug('Building StateMachine from Pipeline spec...')

    #
    # Invoke SFN StateMachine definition to get rkstr8.cloud.stepfunctions_mx.StateMachine
    # Also get associated StateMachine resource substitution dictionary
    #

    state_machine = pipeline.build_machine()
    state_machine_dict = state_machine.build()
    state_machine_json = json.dumps(state_machine_dict, indent=2, sort_keys=False)
    substitutions_dict = pipeline.build_substitutions()

    logging.debug('Rendering templates...')

    #
    # Read in Template fragments for CFN StateMachine and JobDefinitions
    #

    # StateMachine CFN Resource from json fragment. Add StateMachine definition and substitutions.

    with open('templates/fragments/statemachine.json') as frag_fh:
        state_machine_resource_tmpl = frag_fh.read().replace('\n', '')
        state_machine_resource = json.loads(state_machine_resource_tmpl)
        # Set the DefinitionString to the machine json and its substitutions
        state_machine_resource['Properties']['DefinitionString']['Fn::Sub'] = [state_machine_json, substitutions_dict]

    sfn_template_base = ['templates/step_functions_resources.stack.yaml']

    # Define the template rendering pipeline
    sfn_template_rendered = TemplateProcessor(sfn_template_base) \
        .from_yaml(as_path=True) \
        .add_resource(conf["CFN_FSA_LOGICAL_RESOURCE_ID"], state_machine_resource)\
        .to_yaml()

    # Run the template rendering pipeline, leaving results in list
    sfn_template = list(sfn_template_rendered)[0]

    #
    # Build job defs from fragments and render onto Batch base template
    #

    job_def_templates_dir = Path('templates/fragments')
    job_def_templates = dict()
    for job_def_templ in job_def_templates_dir.glob('job_def_template.json'):
        with open(str(job_def_templ)) as frag_fh:
            key = job_def_templ.name.rstrip('.json')
            job_def_templates[key] = Template(frag_fh.read().replace('\n', ''))

    job_def_names = []
    job_defs = []

    for job in conf["CONTAINER_NAMES"]:
        job_def_template_key = "job_def_template"
        job_def_resource_tmpl = job_def_templates[job_def_template_key]
        job_def_names.append('{}JobDef'.format(job.replace("_","")))
        job_uid = job + str(uuid.uuid4())[0:4]
        job_def_resource_str = job_def_resource_tmpl.substitute(job=job, job_uid=job_uid)
        job_def = json.loads(job_def_resource_str)
        pprint(job_def)
        job_defs.append(job_def)

    #
    # Render Resource definitions onto Base template to create effective (final) CFN Template
    #

    batch_template_base = ['templates/batch_resources.stack.yaml']

    # Define the template rendering pipeline
    batch_template_rendered = TemplateProcessor(batch_template_base) \
        .from_yaml(as_path=True) \
        .add_resources(job_def_names, job_defs) \
        .to_yaml()

    # Run the template rendering pipeline, leaving results in list
    batch_template = list(batch_template_rendered)[0]

    static_templates = {
        'launch': conf["PARENT_TEMPLATE_PATH"],
        'lambda': conf["LAMBDA_TEMPLATE_PATH"],
        'network': conf["NETWORK_TEMPLATE_PATH"]
    }

    final_template_strings = {
        'batch': batch_template,
        'sfn': sfn_template
    }

    for name, static_template in static_templates.items():
        with open(static_template, 'r') as fh:
            final_template_strings[name] = fh.read()

    return final_template_strings


def stage_assets_nested(final_template_strings, conf):

    for label, final_template in final_template_strings.items():

        pprint_yaml = yaml.dump(
            yaml.load(final_template)
        )
        print(label)
        print('----------------------------------------------')
        print(pprint_yaml)
        print('----------------------------------------------')

        cfn_template = CloudFormationTemplate(final_template)

        if not cfn_template.validate():
            raise Exception('{} failed to validate.'.format(label))

        if label != 'launch':
            # TODO: Make _check_upload support strings, so we don't needlessly have to write out to files
            final_template_tmp_file = tempfile.NamedTemporaryFile(delete=False)
            final_template_tmp_file.write(final_template.encode(encoding='utf-8'))
            final_template_tmp_file_path = final_template_tmp_file.name
            final_template_tmp_file.close()

            _check_upload(
                local_path=final_template_tmp_file_path,
                bucket_name=conf["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"],
                key_name=Path(conf["TEMPLATE_LABEL_PATH_MAP"][label]).name
            )

            os.unlink(final_template_tmp_file_path)
            if os.path.exists(final_template_tmp_file_path):
                raise Exception('Probably failed to delete {}'.format(final_template_tmp_file_path))

    logging.debug('Uploading tool param jsons...')

    #
    # Batch assets staging
    #

    # get all the json files from the dir using pathlib
    tool_param_jsons = Path(conf["PIPELINE_CMD_TOOL_PARAM_LOCAL_PATH"])
    for tool_param_json in list(tool_param_jsons.glob('*.json')):
        _check_upload(
            local_path=str(tool_param_json),
            bucket_name=conf["PIPELINE_CMD_TOOL_PARAM_S3_BUCKET"],
            key_name='/'.join((conf["PIPELINE_CMD_TOOL_PARAM_S3_KEY_PREFIX"], tool_param_json.name))
        )

    #
    # Uploading license file
    #

    sentieon_license_dir = conf["SENTIEON_LICENSE_LOCAL_PATH"]
    sentieon_license_file = conf["SENTIEON_LICENSE_FILE_NAME"]
    sentieon_license_path = Path(sentieon_license_dir).joinpath(sentieon_license_file)

    _check_upload(
        local_path=str(sentieon_license_path),
        bucket_name=conf["SENTIEON_LICENSE_S3_BUCKET"],
        key_name='/'.join((conf["SENTIEON_LICENSE_S3_KEY_PREFIX"], sentieon_license_file))
    )

    #
    # Uploading parameter yaml config
    #

    param_file_dir = conf["PIPELINE_CMD_TOOL_PARAM_LOCAL_PATH"]
    param_file = conf["PIPELINE_CMD_TOOL_PARAM_FILE"]
    param_path = Path(param_file_dir).joinpath(param_file)

    _check_upload(
        local_path=str(param_path),
        bucket_name=conf["PIPELINE_CMD_TOOL_PARAM_S3_BUCKET"],
        key_name='/'.join((conf["PIPELINE_CMD_TOOL_PARAM_S3_KEY_PREFIX"], param_file))
    )

    #
    # Lambda asset building and staging
    #

    logging.debug('Building and Uploading Lambdas...')

    lambdas = [
        {
            'build_dir': conf["LAMBDA_BUILD_DIR"],
            'script_file': conf["LAMBDA_SCRIPT_FILE"],
            'requirements_file': conf["LAMBDA_REQUIREMENTS_FILE"],
            'deployment_zip': conf["LAMBDA_DEPLOYMENT_ZIP"],
            'deployment_bucket': conf["LAMBDA_DEPLOY_BUCKET"],
            'deployment_key': conf["LAMBDA_DEPLOY_KEY"]
        },
        {
            'build_dir': conf["ALIGN_LAMBDA_BUILD_DIR"],
            'script_file': conf["ALIGN_LAMBDA_SCRIPT_FILE"],
            'requirements_file': conf["ALIGN_LAMBDA_REQUIREMENTS_FILE"],
            'deployment_zip': conf["ALIGN_LAMBDA_DEPLOYMENT_ZIP"],
            'deployment_bucket': conf["ALIGN_LAMBDA_DEPLOY_BUCKET"],
            'deployment_key': conf["ALIGN_LAMBDA_DEPLOY_KEY"]
        },
        {
            'build_dir': conf["HAPLO_LAMBDA_BUILD_DIR"],
            'script_file': conf["HAPLO_LAMBDA_SCRIPT_FILE"],
            'requirements_file': conf["HAPLO_LAMBDA_REQUIREMENTS_FILE"],
            'deployment_zip': conf["HAPLO_LAMBDA_DEPLOYMENT_ZIP"],
            'deployment_bucket': conf["HAPLO_LAMBDA_DEPLOY_BUCKET"],
            'deployment_key': conf["HAPLO_LAMBDA_DEPLOY_KEY"]
        },
        {
            'build_dir': conf["GENO_LAMBDA_BUILD_DIR"],
            'script_file': conf["GENO_LAMBDA_SCRIPT_FILE"],
            'requirements_file': conf["GENO_LAMBDA_REQUIREMENTS_FILE"],
            'deployment_zip': conf["GENO_LAMBDA_DEPLOYMENT_ZIP"],
            'deployment_bucket': conf["GENO_LAMBDA_DEPLOY_BUCKET"],
            'deployment_key': conf["GENO_LAMBDA_DEPLOY_KEY"]
        },
        {
            'build_dir': conf["VQSR_LAMBDA_BUILD_DIR"],
            'script_file': conf["VQSR_LAMBDA_SCRIPT_FILE"],
            'requirements_file': conf["VQSR_LAMBDA_REQUIREMENTS_FILE"],
            'deployment_zip': conf["VQSR_LAMBDA_DEPLOYMENT_ZIP"],
            'deployment_bucket': conf["VQSR_LAMBDA_DEPLOY_BUCKET"],
            'deployment_key': conf["VQSR_LAMBDA_DEPLOY_KEY"]
        },
        {
            'build_dir': conf["ALIGN_POLLER_LAMBDA_BUILD_DIR"],
            'script_file': conf["ALIGN_POLLER_LAMBDA_SCRIPT_FILE"],
            'requirements_file': conf["ALIGN_POLLER_LAMBDA_REQUIREMENTS_FILE"],
            'deployment_zip': conf["ALIGN_POLLER_LAMBDA_DEPLOYMENT_ZIP"],
            'deployment_bucket': conf["ALIGN_POLLER_LAMBDA_DEPLOY_BUCKET"],
            'deployment_key': conf["ALIGN_POLLER_LAMBDA_DEPLOY_KEY"]
        }
    ]

    for lambda_cfg in lambdas:
        builder = Lambda.DeploymentBuilder(**lambda_cfg)
        builder.create_deployment_package()
        builder.upload_deployment_package()
        builder.tear_down()

    #
    # Activities assets building and staging
    #

    logging.debug('Uploading activities assets...')

    activities_assets = [
        {
            'local_path': conf["ACT_AND_HANDOFF_ANSIBLE_PLAYBOOK_LOCAL_PATH"],
            'bucket_name': conf["ACT_AND_HANDOFF_ANSIBLE_PLAYBOOK_REMOTE_BUCKET"],
            'key_name': conf["ACT_AND_HANDOFF_ANSIBLE_PLAYBOOK_REMOTE_KEY"]
        },
        {
            'local_path': conf["ACT_AND_HANDOFF_DAEMON_SCRIPT_LOCAL_PATH"],
            'bucket_name': conf["ACT_AND_HANDOFF_DAEMON_SCRIPT_REMOTE_BUCKET"],
            'key_name': conf["ACT_AND_HANDOFF_DAEMON_SCRIPT_REMOTE_KEY"]
        },
        {
            'local_path': conf["ACT_AND_HANDOFF_DAEMON_REQUIREMENTS_LOCAL_PATH"],
            'bucket_name': conf["ACT_AND_HANDOFF_DAEMON_REQUIREMENTS_REMOTE_BUCKET"],
            'key_name': conf["ACT_AND_HANDOFF_DAEMON_REQUIREMENTS_REMOTE_KEY"]
        },
    ]

    for activities_cfg in activities_assets:
        _check_upload(**activities_cfg)

    logging.debug('Zipping and uploading rkstr8 pkg to s3...')

    shutil.make_archive(conf["RKSTR8_PKG_ARCHIVE_LOCAL_PATH_PREFIX"], 'zip', conf["RKSTR8_PKG_LOCAL_PATH"])

    _check_upload(
        local_path=conf["RKSTR8_PKG_ARCHIVE_LOCAL_PATH"],
        bucket_name=conf["RKSTR8_PKG_REMOTE_BUCKET"],
        key_name=conf["RKSTR8_PKG_REMOTE_KEY"]
    )


def create_resources(pipeline, rendered_templates, conf):

    launchable_template = rendered_templates['launch']

    launcher = StackLauncher(
        template_string=launchable_template
    )

    if launcher.validate_template():
        launcher.upload_template()
    else:
        raise TemplateValidationException()

    template_parameters = pipeline.build_template_params()
    launcher.check_params(template_parameters)

    response = launcher.create(
        template_url='/'.join((
            'https://s3.amazonaws.com',
            conf["RESOURCE_CFN_TMPL_DEPLOY_BUCKET"],
            conf["RESOURCE_CFN_TMPL_DEPLOY_KEY"]
        )),
        parameters=template_parameters,
        timeout=10,
        capabilities=['CAPABILITY_IAM'],
        tags=[
            {
                'Key': 'Name',
                'Value': launcher.stack_name
            }
        ]
    )

    logging.debug('create_stack response: ')
    logging.debug(json.dumps(response, indent=4, sort_keys=False, default=json_serial))

    if not launcher.wait_for_stack():
        raise TimeoutException('Failed waiting for stack to create.')

    return Stack.from_stack_name(stack_name=launcher.stack_name).stack


def run(pipeline, stack, conf):
    """
    Builds the pipeline input and executes the pipeline as a StepFunctions State Machine.
    :return:
    """
    # To execute StateMachine, need
    #
    # 1. the machine arn (from stack)
    # 2. an execution name
    # 3. the PipelineSpec input as json

    stack_with_machine = Stack.from_stack_name(
            stack_name=stack.Resource("StepFunctionResourcesStack").physical_resource_id.split("/")[1]
    ).stack

    print('Stack with machine: {}'.format(stack_with_machine.stack_name))

    machine_arn = stack_with_machine.Resource(conf["CFN_FSA_LOGICAL_RESOURCE_ID"]).physical_resource_id
    execution_name = '-'.join(('NestedExecution', str(uuid.uuid4())[:4]))
    machine_input_json = json.dumps(pipeline.build_input(stack))

    print(machine_input_json)

    execution_arn = StepFunctionsMachine.start(
        machine_arn=machine_arn,
        input=machine_input_json,
        exec_name=execution_name
    )

    logging.info('execution_arn: {}'.format(execution_arn))


def _check_upload(local_path, bucket_name, key_name):
    S3Upload.upload_file(
        local_path=local_path,
        bucket_name=bucket_name,
        key_name=key_name
    )

    if not S3Upload.object_exists(
            bucket_name=bucket_name,
            key_name=key_name
    ):
        raise ValueError('Unable to find {} in S3 after upload.'.format(key_name))
