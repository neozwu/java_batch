import os
import sys
import glob
import fnmatch
import argparse
import subprocess
from artman import cli

exclusion_list = [
    'artman_streetview_publish.yaml',
    'artman_clouddebugger.yaml',
    'artman_cloudbuild.yaml',
    'artman_remoteworkers.yaml',
    'artman_containeranalysis',
    'artman_cloudresourcemanager.yaml',
    'artman_cloudiot.yaml',
    'artman_functions.yaml',
]


def get_artman_api_yaml(googleapis_repo):
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
          os.path.basename(yaml_file)[7:-5]
          for yaml_file in artman_yaml_files
      ], artman_yaml_files))
  return mappings


def get_task_type(artman_yaml_file):
  artman_file_content = open(artman_yaml_file, 'r').read()
  if 'JAVA_GAPIC' in artman_file_content or 'java_gapic' in artman_file_content:
    return 'JAVA_GAPIC'
  if 'java_grpc' in artman_file_content and 'java_proto' in artman_file_content:
    return 'JAVA_GRPC'


def run_batch(artman_yaml_file,
              root_dir,
              staging_repo,
              docker_mode,
              g3artman_mode,
              dry_run=False):
  task_type = get_task_type(artman_yaml_file)
  artman_yaml_file = _get_config_path_relative_to_googleapis(
      artman_yaml_file)
  if task_type == 'JAVA_GAPIC':
    return _run_java_gapic(artman_yaml_file, root_dir, staging_repo,
                           docker_mode, g3artman_mode, dry_run)
  elif task_type == 'JAVA_GRPC':
    return _run_java_grpc(artman_yaml_file, root_dir, staging_repo,
                          docker_mode, g3artman_mode, dry_run)
  else:
    return False


def _run_java_gapic(artman_yaml_file, root_dir, local_staging_repo,
                    docker_mode, g3artman_mode, dry_run):
  cmd = ('%s %s --config %s --root-dir %s publish --local-repo-dir %s '
         '--dry-run --target staging java_gapic') % (
             'artman' if not g3artman_mode else 'g3artman', '--local'
             if not docker_mode else '', artman_yaml_file, root_dir,
             local_staging_repo)
  print('JAVA_BATCH:  running: %s' % cmd)
  if not dry_run:
    try:
      subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError:
      return False
  return True


def _run_java_grpc(artman_yaml_file, root_dir, local_staging_repo, docker_mode,
                   g3artman_mode, dry_run):
  java_proto_cmd = (
      '%s %s --config %s --root-dir %s publish --local-repo-dir %s '
      '--dry-run --target staging java_proto') % (
          'artman' if not g3artman_mode else 'g3artman', '--local'
          if not docker_mode else '', artman_yaml_file, root_dir,
          local_staging_repo)

  java_grpc_cmd = (
      'artman %s --config %s --root-dir %s publish --local-repo-dir %s '
      '--dry-run --target staging java_grpc') % ('--local'
                                                 if not docker_mode else '',
                                                 artman_yaml_file, root_dir,
                                                 local_staging_repo)

  print('JAVA_BATCH: running: %s' % java_proto_cmd)
  if not dry_run:
    try:
      subprocess.check_output(java_proto_cmd, shell=True)
    except subprocess.CalledProcessError:
      return False

  print('JAVA_BATCH: running: %s' % java_grpc_cmd)
  if not dry_run:
    try:
      subprocess.check_output(java_grpc_cmd, shell=True)
    except subprocess.CalledProcessError:
      return False
  return True


def _parse_args(*args):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--root_dir',
      type=str,
      default='',
      help=
      'Googleapis repo directory. You need to either specify this argument or specify googleapis in artman user config yaml'
  )

  parser.add_argument(
      '--staging_repo',
      type=str,
      default='../api-client-staging',
      help=
      'api-client-staging repo directory. Default to \'../api-client-staging\''
  )

  parser.add_argument(
      '--api_list',
      type=str,
      default='',
      help=
      'A list of comma-separated API names that batch script will generate. Default to run all APIs'
  )

  parser.add_argument('--user-config', default='~/.artman/config.yaml')

  parser.add_mutually_exclusive_group(required=False)
  parser.add_argument(
      '--docker_mode',
      dest='docker_mode',
      action='store_true',
      help='Run artman in docker mode. This is default behavior.')

  parser.add_argument(
      '--local_mode',
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
      '--dryrun',
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
      flags.root_dir = os.path.join(
          user_config['local_paths']['reporoot'], 'googleapis')
    else:
      print(
          'JAVA_BATCH: fatal error: `--root_dir` or '
          '`googleapis` field in artman user config must be specified.')
      sys.exit(1)

  flags.root_dir = os.path.expanduser(flags.root_dir)
  return flags


def _get_config_path_relative_to_googleapis(path):
  return path[path.find('googleapis/') + 11:]


def main(*args):
  if not args:
    args = sys.argv[1:]
  flags = _parse_args(*args)
  artman_yamls = filter_exclusion_list(
      get_artman_api_yaml(flags.root_dir), exclusion_list)

  if flags.api_list:
    mappings = api_to_yaml_mapping(artman_yamls)
    artman_yamls = []
    for api in flags.api_list.split(','):
      artman_yamls.append(mappings[api])

  for artman_yaml in artman_yamls:
    run_batch(artman_yaml, flags.root_dir, flags.staging_repo,
              flags.docker_mode, flags.g3artman_mode, flags.dryrun_mode)


if __name__ == '__main__':
  main()
