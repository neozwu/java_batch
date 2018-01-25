import glob
import os
import sys
import fnmatch
import argparse
import subprocess
from artman import cli

blacklist = ['artman_streetview_publish.yaml']


def get_artman_api_yaml(googleapis_repo):
    artman_yaml_files = []
    for root, dir_names, file_names in os.walk(
            os.path.join(googleapis_repo, 'google')):
        for fname in fnmatch.filter(file_names, 'artman_*.yaml'):
            artman_yaml_files.append(os.path.join(root, fname))
    return artman_yaml_files


def get_config_path_relative_to_googleapis(path):
    return path[path.find('googleapis/') + 11:]


def filter_blacklist(artman_yaml_files, blacklist=[]):
    return [
        yaml for yaml in artman_yaml_files
        if os.path.basename(yaml) not in blacklist
    ]


def get_api_to_yaml_mapping(artman_yaml_files):
    mappings = dict(
        zip([
            os.path.basename(yaml_file)[7:] for yaml_file in artman_yaml_files
        ], artman_yaml_files))
    return mappings


def get_task_type(artman_yaml_file):
    artman_file_content = open(artman_yaml_file, 'r').read()
    if 'JAVA_GAPIC' in artman_file_content or 'java_gapic' in artman_file_content:
        return 'JAVA_GAPIC'
    if 'java_grpc' in artman_file_content and 'java_proto' in artman_file_content:
        return 'JAVA_GRPC'


def run_artman_yaml(artman_yaml_file, is_docker_mode, root_dir, staging_repo):
    task_type = get_task_type(artman_yaml_file)
    artman_yaml_file = get_config_path_relative_to_googleapis(artman_yaml_file)
    if task_type == 'JAVA_GAPIC':
        return run_java_gapic(artman_yaml_file, is_docker_mode, root_dir,
                              staging_repo)
    elif task_type == 'JAVA_GRPC':
        return run_java_grpc(artman_yaml_file, is_docker_mode, root_dir,
                             staging_repo)
    else:
        return False


def run_java_gapic(artman_yaml_file, is_docker_mode, root_dir,
                   local_staging_repo):
    cmd = "artman %s --config %s --root-dir %s publish --local-repo-dir %s --dry-run --target staging java_gapic" % (
        "--local" if not is_docker_mode else "", artman_yaml_file, root_dir,
        local_staging_repo)
    print("running: %s" % cmd)
    try:
        subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError:
        return False
    return True


def run_java_grpc(artman_yaml_file, is_docker_mode, root_dir,
                  local_staging_repo):
    cmd = "artman %s --config %s --root-dir %s publish --local-repo-dir %s --dry-run --target staging java_grpc" % (
        "--local" if not is_docker_mode else "", artman_yaml_file, root_dir,
        local_staging_repo)
    print("running: %s" % cmd)
    try:
        subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError:
        return False
    return True


def parse_args(*args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--root_dir',
        type=str,
        default='',
        help='Googleapis repo directory. Default to \'./\'')

    parser.add_argument(
        '--staging_repo',
        type=str,
        default='../api-client-staging',
        help=
        'api-client-staging repo directory. Default to \'../api-client-staging\''
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

    flags = parser.parse_args(args)

    user_config = cli.main.read_user_config(flags)
    if not flags.root_dir:
        if user_config['local_paths']['googleapis']:
            flags.root_dir = user_config['local_paths']['googleapis']
        else:
            print(
                "Fatal error: `--root_dir` must be specified, or you will have to specify the `googleapis` field in artman user config."
            )
            sys.exit(1)
    flags.root_dir = os.path.expanduser(flags.root_dir)
    return flags


def main(*args):
    if not args:
        args = sys.argv[1:]
    flags = parse_args(*args)
    print(flags)
    artman_yamls = filter_blacklist(
        get_artman_api_yaml(flags.root_dir), blacklist)
    for artman_yaml in artman_yamls:
        print(artman_yaml)
        run_artman_yaml(artman_yaml, flags.docker_mode, flags.root_dir,
                        flags.staging_repo)


if __name__ == '__main__':
    main()
