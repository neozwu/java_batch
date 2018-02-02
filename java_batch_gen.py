import os
import sys
import glob
import json
import fnmatch
import argparse
import subprocess
from artman import cli
from artman.config.proto.config_pb2 import Artifact, Config
from google.protobuf import json_format
import yaml
import shutil

APIS = [
    # shared package
    'core',
    'appengine',
    'iam',
    # gapic API
    'bigquerydatatransfer',
    'bigtable',
    'bigtableadmin',
    'container',
    'dataproc_v1',
    'datastore',
    'dialogflow_v2beta1_java',
    'dlp_v2beta1',
    'dlp_v2beta2',
    'errorreporting',
    'firestore',
    'language_v1',
    'language_v1beta2',
    'logging',
    'longrunning',
    'monitoring',
    'pubsub',
    'oslogin_v1',
    'spanner',
    'spanner_admin_database',
    'spanner_admin_instance',
    'speech_v1',
    'speech_v1beta1',
    'cloudtrace_v1',
    'cloudtrace_v2',
    'videointelligence_v1beta1',
    'videointelligence_v1beta2',
    'videointelligence_v1',
    'vision_v1',
    'vision_v1p1beta1',
]

PROTO_EXCLUSION = ['longrunning']
GRPC_EXCLUSION = ['appengine', 'longrunning']
COPY_EXCLUSION = ['dlp_v2beta2']

def get_artman_yaml(googleapis_repo):
  artman_yaml_files = []
  for root, dir_names, file_names in os.walk(
      os.path.join(googleapis_repo, 'google')):
    for fname in fnmatch.filter(file_names, 'artman_*.yaml'):
      artman_yaml_files.append(os.path.join(root, fname))
  return artman_yaml_files


def filter_exclusion_list(artman_yaml_files, exclusion_list):
  return [
      yaml for yaml in artman_yaml_files
      if os.path.basename(yaml) not in exclusion_list
  ]


def api_to_yaml_mapping(artman_yaml_files):
  mappings = dict(
      zip([
          os.path.basename(yaml_file)[7:-5] for yaml_file in artman_yaml_files
      ], artman_yaml_files))
  return mappings


def get_task_type(artman_yaml_file):
  artman_file_content = open(artman_yaml_file, 'r').read()
  if 'JAVA_GAPIC' in artman_file_content or 'java_gapic' in artman_file_content:
    return 'JAVA_GAPIC'
  if 'java_grpc' in artman_file_content and 'java_proto' in artman_file_content:
    return 'JAVA_GRPC'


def run_batch(api,
              artman_yaml,
              root_dir,
              staging_repo,
              docker_mode,
              g3artman_mode,
              dry_run=False):
  task_type = get_task_type(artman_yaml)
  artman_yaml = _get_config_path_relative_to_googleapis(artman_yaml)
  if task_type == 'JAVA_GAPIC':
    return _run_java_gapic(api, artman_yaml, root_dir, staging_repo,
                           docker_mode, g3artman_mode, dry_run)
  elif task_type == 'JAVA_GRPC':
    return _run_java_grpc(api, artman_yaml, root_dir, staging_repo, docker_mode,
                          g3artman_mode, dry_run)
  else:
    return False


def _run_java_gapic(api, artman_yaml, root_dir, local_staging_repo, docker_mode,
                    g3artman_mode, dry_run):
  cmd = ('%s %s --config %s --root-dir %s publish --local-repo-dir %s '
         '--dry-run --target staging java_gapic') % (
             'artman' if not g3artman_mode else 'g3artman', '--local'
             if not docker_mode else '', artman_yaml, root_dir,
             local_staging_repo)
  print('JAVA_BATCH>  running: %s' % cmd)
  if not dry_run:
    try:
      subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError:
      return False
  return True


def _run_java_grpc(api, artman_yaml, root_dir, local_staging_repo, docker_mode,
                   g3artman_mode, dry_run):
  java_proto_cmd = (
      '%s %s --config %s --root-dir %s publish --local-repo-dir %s '
      '--dry-run --target staging java_proto') % (
          'artman' if not g3artman_mode else 'g3artman', '--local'
          if not docker_mode else '', artman_yaml, root_dir, local_staging_repo)

  java_grpc_cmd = (
      'artman %s --config %s --root-dir %s publish --local-repo-dir %s '
      '--dry-run --target staging java_grpc') % ('--local'
                                                 if not docker_mode else '',
                                                 artman_yaml, root_dir,
                                                 local_staging_repo)

  if api not in PROTO_EXCLUSION:
    print('JAVA_BATCH> running: %s' % java_proto_cmd)
    if not dry_run:
      try:
        subprocess.check_output(java_proto_cmd, shell=True)
      except subprocess.CalledProcessError:
        return False

  if api not in GRPC_EXCLUSION:
    print('JAVA_BATCH> running: %s' % java_grpc_cmd)
    if not dry_run:
      try:
        subprocess.check_output(java_grpc_cmd, shell=True)
      except subprocess.CalledProcessError:
        return False
  return True


def _parse_args(*args):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--root-dir',
      type=str,
      default='',
      help=
      'Googleapis repo directory. You need to either specify this argument or specify googleapis in artman user config yaml'
  )

  parser.add_argument(
      '--local-repo-dir',
      type=str,
      default='../api-client-staging',
      help=
      'api-client-staging repo directory. Default to \'../api-client-staging\'')

  parser.add_argument(
      '--gcj-repo-dir',
      type=str,
      default='',
      help=
      'google-cloud-java repo directory. If this argument is set, batch script will run copy task only')

  parser.add_argument(
      '--api-list',
      type=str,
      default='',
      help=
      'A list of comma-separated API names that batch script will generate. Default to run all APIs'
  )

  parser.add_argument(
      '--exclude',
      type=str,
      default='',
      help='A list of comma-separated API names that batch script will exclude.'
  )

  parser.add_argument('--user-config', default='~/.artman/config.yaml')

  parser.add_mutually_exclusive_group(required=False)
  parser.add_argument(
      '--docker-mode',
      dest='docker_mode',
      action='store_true',
      help='Run artman in docker mode. This is default behavior.')

  parser.add_argument(
      '--local-mode',
      dest='docker_mode',
      action='store_false',
      help='Run artman in local mode.')

  parser.add_argument(
      '--g3artman',
      dest='g3artman_mode',
      action='store_true',
      help='Not supported yet. Run g3artman instead of artman.')

  parser.set_defaults(g3artman=False)

  parser.add_argument(
      '--dry-run',
      dest='dryrun_mode',
      action='store_true',
      help=
      'Dry run mode. Batch script prints out artman command without running them. Default to false.'
  )

  parser.set_defaults(dryrun_mode=False)

  flags = parser.parse_args(args)

  user_config = cli.main.read_user_config(flags)

  if not flags.root_dir:
    if 'googleapis' in user_config['local_paths']:
      flags.root_dir = user_config['local_paths']['googleapis']
    elif 'reporoot' in user_config['local_paths']:
      flags.root_dir = os.path.join(user_config['local_paths']['reporoot'],
                                    'googleapis')
    else:
      print('JAVA_BATCH> fatal error: `--root_dir` or '
            '`googleapis` field in artman user config must be specified.')
      sys.exit(1)

  flags.root_dir = os.path.expanduser(flags.root_dir)
  return flags


def _get_config_path_relative_to_googleapis(path):
  return path[path.find('googleapis/') + 11:]


def remove_proto_exclusion(flags, mapping):
  for api in PROTO_EXCLUSION:
    artman_yaml = mapping[api]
    proto_dir = _get_staging_dir(artman_yaml, 'java_gapic', 'proto')
    if proto_dir:
      print('JAVA_BATCH> deleting: ' + proto_dir)
      shutil.rmtree(proto_dir)


def remove_grpc_exclusion(flags, mapping):
  for api in GRPC_EXCLUSION:
    artman_yaml = mapping[api]
    grpc_dir = _get_staging_dir(artman_yaml, 'java_gapic', 'grpc')
    if grpc_dir:
      print('JAVA_BATCH> deleting: ' + grpc_dir)
      shutil.rmtree(grpc_dir)


def _get_staging_dir(artman_yaml, artifact_name, dir_mapping_name):
  artman_config = _get_artman_config(artman_yaml)
  for artifact in artman_config.artifacts:
    if artifact.name == artifact_name:
      for publish_target in artifact.publish_targets:
        if publish_target.name == 'staging':
          for dir_mapping in publish_target.directory_mappings:
            if dir_mapping.name == dir_mapping_name:
              return dir_mapping.dest
  return None


def _get_artman_config(artman_yaml):
  config_pb = Config()
  with open(artman_yaml, 'r') as f:
    artman_config_json_string = json.dumps(yaml.load(f))
  json_format.Parse(artman_config_json_string, config_pb)
  return config_pb


def main(*args):
  if not args:
    args = sys.argv[1:]
  flags = _parse_args(*args)
  mapping = api_to_yaml_mapping(get_artman_yaml(flags.root_dir))
  print(mapping)

  if flags.gcj_repo_dir:
    pass
  else:
    if flags.exclude:
      for api in flags.exclude.split(','):
        APIS.remove(api)

    if flags.api_list:
      for api in flags.api_list.split(','):
        run_batch(api, mapping[api], flags.root_dir, flags.local_repo_dir,
                  flags.docker_mode, flags.g3artman_mode, flags.dryrun_mode)
    else:
      for api in APIS:
        run_batch(api, mapping[api], flags.root_dir, flags.local_repo_dir,
                  flags.docker_mode, flags.g3artman_mode, flags.dryrun_mode)

    remove_proto_exclusion(flags, mapping)
    remove_grpc_exclusion(flags, mapping)

if __name__ == '__main__':
  main()
