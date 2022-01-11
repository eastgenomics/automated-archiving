"""
Automated-archiving

This script will check for projs and directories within staging52/53
which has not been active for the past X months (inactive). It will then
send a Slack notification to notify the will-be-archived files

The second run of the script will start the archiving process previously
noted to-be-archive files. It skips files tagged with 'no-archive'

"""

import os
import sys
import requests
import json
import dxpy as dx
from dotenv import load_dotenv
import pickle
import collections
import datetime as dt
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from helper import get_logger

log = get_logger("main log")


load_dotenv()

PROJECT_52 = os.environ['PROJECT_52']
PROJECT_53 = os.environ['PROJECT_53']
MONTH = int(os.environ['AUTOMATED_MONTH'])
ARCHIVE_PICKLE_PATH = os.environ['AUTOMATED_ARCHIVE_PICKLE_PATH']
ARCHIVED_TXT_PATH = os.environ['AUTOMATED_ARCHIVED_TXT_PATH']


def post_message_to_slack(channel, index, data, error='', alert=False):
    """
    Request function for slack web api for:
    (1) send alert msg when dxpy auth failed
    (2) send to-be-archived notification

    Inputs:
        channel: e.g. egg-alerts, egg-logs
        index: index for which proj in lists (below)
        data: list of projs / dirs to be archived
        error: (optional) (required only when dxpy auth failed) dxpy error msg
        alert: (optional) (required only when dxpy auth failed) Boolean

    """

    http = requests.Session()
    retries = Retry(total=3, backoff_factor=1, method_whitelist=['POST'])
    http.mount("https://", HTTPAdapter(max_retries=retries))

    lists = [
        'proj_list',
        'folders52',
        'folders53',
        'special_notify',
        'error'
        ]

    log.info(f'Posting data for: {lists[index]}')

    today = dt.date.today().strftime("%d/%m/%Y")
    text_data = '\n'.join(sorted(data))

    messages = [
        f':file_folder: {today} *Projects which will be archived:*',
        f':file_folder: {today} *Directories in `staging52` to be archived:*',
        f':file_folder: {today} *Directories in `staging53` to be archived:*',
        (f':bangbang: {today} *Inactive projects or directories to '
            'be archived unless re-tag `no-archive`:*')
        ]

    log.info(f'Sending POST request to channel: #{channel}')

    try:
        if alert:
            error_msg = (
                "automated-archiving: Error with dxpy token! Error code: \n"
                f"`{error.error_message()}`"
                )

            response = http.post(
                'https://slack.com/api/chat.postMessage', {
                    'token': os.environ['SLACK_TOKEN'],
                    'channel': f'U02HPRQ9X7Z',
                    'text': error_msg
                }).json()

            if response['ok']:
                log.info(f'POST request to channel #{channel} successful')
                return
            else:
                # slack api request failed
                error_code = response['error']
                log.error(f'Slack API error to #{channel}')
                log.error(f'Error Code From Slack: {error_code}')
        else:
            response = http.post(
                'https://slack.com/api/chat.postMessage', {
                    'token': os.environ['SLACK_TOKEN'],
                    'channel': f'U02HPRQ9X7Z',
                    'attachments': json.dumps([{
                        "pretext": messages[index],
                        "text": text_data}])
                }).json()

            if response['ok']:
                log.info(f'POST request to channel #{channel} successful')
                return
            else:
                # slack api request failed
                error_code = response['error']
                log.error(f'Slack API error to #{channel}')
                log.error(f'Error Code From Slack: {error_code}')

    except Exception as e:
        # endpoint request fail from server
        log.error(f'Error sending POST request to channel #{channel}')
        log.error(e)


def read_or_new_pickle(path):
    """
    Read stored pickle memory for the script
    Using defaultdict() automatically create new dict.key()

    Input:
        Path to store the pickle

    Returns:
        dict: the stored pickle dict
    """
    if os.path.isfile(path):
        with open(path, 'rb') as f:
            pickle_dict = pickle.load(f)
    else:
        pickle_dict = collections.defaultdict(list)
        with open(path, 'wb') as f:
            pickle.dump(pickle_dict, f)

    return pickle_dict


def older_than(month, modified_epoch):
    """
    Determine if a modified epoch date is older than X month

    Inputs:
        X month, proj modified date (epoch)

    Returns:
        Boolean
    """

    modified = modified_epoch / 1000.0
    date = dt.datetime.fromtimestamp(modified)

    return date + relativedelta(months=+month) < dt.datetime.today()


def check_dir(dir, month):
    """
    Function to check if project (002) for that directory
    exist. e.g. For 210407_A01295_0010_AHWL5GDRXX
    it looks for 002_210407_A01295_0010_AHWL5GDRXX

    If the 002 exist, we check if the 002 has been inactive
    for the last X month. If yes, return True.

    Inputs:
        directory, X month

    Returns:
        Boolean:
        True if its 002 has not been active for X month
        False if no 002 returned / 002 been active in past X month
    """

    result = list(
        dx.find_projects(
            dir,
            name_mode='regexp',
            describe=True))

    if not result:
        return False

    modified_epoch = result[0]['describe']['modified']

    if older_than(month, modified_epoch):
        return True
    else:
        return False


def dx_login():
    """
    DNANexus login check function

    Returns:
        None
    """

    DX_SECURITY_CONTEXT = {
        "auth_token_type": "Bearer",
        "auth_token": os.environ['DNANEXUS_TOKEN']
        }

    dx.set_security_context(DX_SECURITY_CONTEXT)

    try:
        log.info('Checking DNANexus login')
        dx.api.system_whoami()
        log.info('DNANexus login successful')

    except Exception as e:
        log.error('Error with DNANexus login')
        log.error(f'Error message from DNANexus{e.error_message()}')

        post_message_to_slack(
            'egg-alerts',
            4,
            [],
            error=e,
            alert=True
            )

        log.info('End of script')
        sys.exit()


def remove_proj_tag(proj):
    """
    Function to remove tag 'no-archive' for project

    When a project has been tagged 'no-archive' but
    has not been modified for X months
    the tag will be removed and the project will be
    notified to be archived

    This only applies to project-id
    Not directories in Staging52 & 53

    Input:
        project-id

    Returns:
        project-id of tag-removed project
    """

    response = dx.api.project_remove_tags(
        proj, input_params={'tags': ['no-archive']})
    return response['id']


def find_projs_and_notify():

    log.info('Running Code A')

    dx_login()

    archive_pickle = read_or_new_pickle(ARCHIVE_PICKLE_PATH)

    # special notify include those projs / directories in staging52/53
    # which has been tagged 'no-archive' before but has not been modified
    # for X months. It will be listed under its own column in Slack msg
    # to make it more visible
    special_notify = []

    # Get all 002 and 003 projects
    projects_dict = dict()

    projects = dx.search.find_projects(
        name='00[2,3].*',
        name_mode='regexp',
        billed_to='org-emee_1',
        describe=True
        )

    # put all projects into a dict
    projects_dict.update({proj['id']: proj for proj in list(projects)})

    # sieve the dict to include only old-enough projs
    old_enough_projects_dict = {
        k: v for k, v in projects_dict.items() if older_than(
            MONTH, v['describe']['modified'])}

    excluded_list = [PROJECT_52, PROJECT_53]

    # exclude projs (staging52/53) and archived projs
    old_enough_projects_dict = {
        k: v for k, v in old_enough_projects_dict.items()
        if k not in excluded_list and k not in archive_pickle['archived']
    }

    log.info(f'No. of old enough projects: {len(old_enough_projects_dict)}')

    staging52 = dx.DXProject(PROJECT_52)
    staging53 = dx.DXProject(PROJECT_53)

    # get all folders in staging52
    all_folders_in_52 = staging52.list_folder(only='folders')['folders']
    directories_in_52 = [
        (file.lstrip('/').lstrip('/processed'), PROJECT_52, '52', file)
        for file in all_folders_in_52 if file != '/processed']
    directories_in_52_processed = [
        (file.lstrip('/').lstrip('/processed'), PROJECT_52, '52', file)
        for file in staging52.list_folder(
            '/processed', only='folders')['folders']]

    # get all folders in staging53
    excluded_directories = ['/MVZ_upload', '/Reports', '/dx_describe']
    all_folders_in_53 = staging53.list_folder(only='folders')['folders']
    directories_in_53 = [
        (file.lstrip('/').lstrip('/processed'), PROJECT_53, '53', file)
        for file in all_folders_in_53 if file not in excluded_directories]

    # all_52_dirs = directories_in_52 + directories_in_52_processed
    all_directories = \
        directories_in_52 + directories_in_52_processed + directories_in_53
    archived_dirs = \
        archive_pickle['archived_52'] + archive_pickle['archived_53']

    all_directories = [
        dir for dir in all_directories if dir[3] not in archived_dirs]

    log.info(f'Processing {len(all_directories)} directories in staging52/53')

    old_enough_directories = [
        file for file in all_directories if check_dir(file[0], MONTH)]

    log.info(f'No. of old enough directories: {len(old_enough_directories)}')

    # get proj-id of each projs
    if old_enough_projects_dict:
        log.info('Saving project-id to pickle')

        for k, v in old_enough_projects_dict.items():
            if 'no-archive' in [tag.lower() for tag in v['describe']['tags']]:
                id = remove_proj_tag(k)
                log.info(f'REMOVE_TAG: {id}')

                special_notify.append(v['describe']['name'])
                archive_pickle['to_be_archived'].append(v['id'])

            else:
                archive_pickle['to_be_archived'].append(v['id'])

    # sieve through each directory in staging52/53
    if old_enough_directories:
        log.info('Saving directories')

        for _, proj, file_num, original_dir in old_enough_directories:
            if original_dir in archive_pickle['skipped']:
                print('found it')

                log.info(f'REMOVE_TAG: {original_dir} in skipped')
                files = list(dx.find_data_objects(
                    project=proj,
                    folder=original_dir,
                    tags=['no-archive']
                ))

                log.info(f'REMOVE_TAG: removing tag for {len(files)} files')
                for file in files:
                    dx.api.file_remove_tags(
                        file['id'],
                        input_params={
                            'tags': ['no-archive'],
                            'project': proj})

                special_notify.append(f'{original_dir} in staging{file_num}')
                archive_pickle['skipped'].remove(original_dir)

            archive_pickle[f'staging_{file_num}'].append(original_dir)

    # get everything ready for slack notification
    proj_list = [
        p['describe']['name'] for p in old_enough_projects_dict.values()]
    folders52 = archive_pickle['staging_52']
    folders53 = archive_pickle['staging_53']

    lists = [
          list(set(proj_list)),
          list(set(folders52)),
          list(set(folders53)),
          list(set(special_notify))
        ]

    # send slack notification if there's old-enough dir
    for index, data in enumerate(lists):
        if data:
            post_message_to_slack(
                channel='egg-alerts',
                index=index,
                data=data
                )

    # save dict
    with open(ARCHIVE_PICKLE_PATH, 'wb') as f:
        pickle.dump(archive_pickle, f)

    log.info('End of Code A')


def archive_skip_function(dir, proj, archive_dict, temp_dict, num):
    folders = list(dx.find_data_objects(
        project=proj,
        folder=dir,
        tags=['no-archive']
        ))

    if folders:
        log.info(f'Skipped {dir} in staging{num}')
        archive_dict['skipped'].append(dir)
    else:
        log.info(f'archiving staging{num}: {dir}')
        # dx.api.project_archive(
        #     proj, input_params={'folder': dir})
        archive_dict[f'archived_{num}'].append(dir)
        temp_dict['archived'].append(f'{proj}:{dir}')


def archiving_function():

    log.info('Running Code B')

    dx_login()

    # get previously saved dict in pickle
    archive_pickle = read_or_new_pickle(ARCHIVE_PICKLE_PATH)

    list_of_projs = archive_pickle['to_be_archived']
    list_of_dirs_52 = archive_pickle['staging_52']
    list_of_dirs_53 = archive_pickle['staging_53']

    temp_archived = collections.defaultdict(list)

    # do the archiving
    if list_of_projs:
        for id in list_of_projs:
            project = dx.DXProject(id)
            proj_desc = project.describe()
            proj_name = proj_desc['name']

            # check if proj been tagged with 'no-archive'
            if 'no-archive' in proj_desc['tags']:
                log.info(f'Skipped {proj_name}')
                continue
            else:
                log.info(f'archiving {id}')
                # dx.api.project_archive(proj)
                archive_pickle['archived'].append(id)
                temp_archived['archived'].append(id)

    if list_of_dirs_52:
        for dir in list_of_dirs_52:
            archive_skip_function(
                dir, PROJECT_52, archive_pickle, temp_archived, '52')

    if list_of_dirs_53:
        for dir in list_of_dirs_53:
            archive_skip_function(
                dir, PROJECT_53, archive_pickle, temp_archived, '53')

    # generate archiving log file
    if os.path.isfile(ARCHIVED_TXT_PATH):
        with open(ARCHIVED_TXT_PATH, 'a') as f:
            for line in temp_archived['archived']:
                f.write('\n' + line)
    else:
        with open(ARCHIVED_TXT_PATH, 'w') as f:
            f.write('\n'.join(temp_archived['archived']))

    # empty pickle
    log.info('Clearing pickle file')

    archive_pickle['to_be_archived'] = []
    archive_pickle['staging_52'] = []
    archive_pickle['staging_53'] = []

    # save dict
    with open(ARCHIVE_PICKLE_PATH, 'wb') as f:
        pickle.dump(archive_pickle, f)

    log.info('End of Code B')

    find_projs_and_notify()


def main():

    archive_pickle = read_or_new_pickle(ARCHIVE_PICKLE_PATH)
    to_be_archived = archive_pickle['to_be_archived']
    staging52 = archive_pickle['staging_52']
    staging53 = archive_pickle['staging_53']

    if to_be_archived or staging52 or staging53:
        archiving_function()
    else:
        find_projs_and_notify()

    log.info('End of script.')


if __name__ == "__main__":
    main()
