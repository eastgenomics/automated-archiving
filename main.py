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

# for sending helpdesk email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

from helper import get_logger

log = get_logger("main log")


load_dotenv()

PROJECT_52 = os.environ['PROJECT_52']
PROJECT_53 = os.environ['PROJECT_53']
MONTH = int(os.environ['AUTOMATED_MONTH'])
ARCHIVE_PICKLE_PATH = os.environ['AUTOMATED_ARCHIVE_PICKLE_PATH']
ARCHIVED_TXT_PATH = os.environ['AUTOMATED_ARCHIVED_TXT_PATH']
SLACK_TOKEN = os.environ['SLACK_TOKEN']
SERVER = os.environ['ANSIBLE_SERVER']
PORT = os.environ['ANSIBLE_PORT']
SENDER = os.environ['ANSIBLE_SENDER']
RECEIVERS = os.environ['TEST_RECEIVERS']


def post_message_to_slack(channel, index, data, error='', alert=False):
    """
    Request function for slack web api for:
    (1) send alert msg when dxpy auth failed (alert=True)
    (2) send to-be-archived notification (default alert=False)

    Inputs:
        channel: e.g. egg-alerts, egg-logs
        index: index for which proj in lists (below)
        data: list of projs / dirs to be archived
        error: (optional) (required only when dxpy auth failed) dxpy error msg
        alert: (optional) (required only when dxpy auth failed) Boolean

    Return:
        None
    """

    http = requests.Session()
    retries = Retry(total=5, backoff_factor=10, method_whitelist=['POST'])
    http.mount("https://", HTTPAdapter(max_retries=retries))

    receivers = RECEIVERS.split(',') if ',' in RECEIVERS else [RECEIVERS]

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
            # only for dxpy auth failure alert msg
            error_msg = (
                "automated-archiving: Error with dxpy token! Error code: \n"
                f"`{error.error_message()}`"
                )

            response = http.post(
                'https://slack.com/api/chat.postMessage', {
                    'token': SLACK_TOKEN,
                    'channel': f'#{channel}',
                    'text': error_msg
                }).json()

        else:
            # default notification
            response = http.post(
                'https://slack.com/api/chat.postMessage', {
                    'token': SLACK_TOKEN,
                    'channel': f'#{channel}',
                    'attachments': json.dumps([{
                        "pretext": messages[index],
                        "text": text_data}])
                }).json()

        if response['ok']:
            log.info(f'POST request to channel #{channel} successful')
        else:
            # slack api request failed
            error_code = response['error']
            log.error(f'Slack API error to #{channel}')
            log.error(f'Error Code From Slack: {error_code}')

            send_mail(
                SENDER,
                receivers,
                'Automated Archiving Slack API Token Error',
                'Error with Automated Archiving Slack API Token'
                )
            log.info('End of script')
            sys.exit()

    except Exception as e:
        # endpoint request fail from server
        log.error(f'Error sending POST request to channel #{channel}')
        log.error(e)

        send_mail(
            SENDER,
            receivers,
            'Automated Archiving Slack Post Request Failed (Server Error)',
            'Error with Automated Archiving post request to Slack'
            )
        log.info('End of script')
        sys.exit()


def send_mail(send_from, send_to, subject, text):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    try:
        smtp = smtplib.SMTP(SERVER, PORT)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.close()
        log.info('Server help email SENT')

    except Exception as e:
        log.error('Server error email FAILED')


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


def older_than(month, modified_epoch) -> bool:
    """
    Determine if a modified epoch date is older than X month

    Inputs:
        X month, proj modified date (epoch)

    Returns (Boolean):
        True if haven't been modified in last X month
        False if have been modified in last X month
    """

    modified = modified_epoch / 1000.0
    date = dt.datetime.fromtimestamp(modified)

    return date + relativedelta(months=+month) < dt.datetime.today()


def check_dir(dir, month):
    """
    Function to check if project (002) for that directory
    exist. e.g. For 210407_A01295_0010_AHWL5GDRXX
    it looks for 002_210407_A01295_0010_AHWL5GDRXX project

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


def get_all_old_enough_projs(month, archive_dict):
    """
    Get all 002 and 003 projects which are not modified
    in the last X months. Exclude projects: staging 52 and
    staging 53 as they will be processed separately + exclude projects
    which had been archived.

    Input:
        month: duration of inactivity in the last x month
        archive_dict: the archive pickle to remember what file to be archived

    Returns (dict):
        dictionary of key (proj-id) and
        value (describe JSON from dxpy for the proj)

    """

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
            month, v['describe']['modified'])}

    excluded_list = [PROJECT_52, PROJECT_53]

    # exclude projs (staging52/53) and archived projs
    old_enough_projects_dict = {
        k: v for k, v in old_enough_projects_dict.items()
        if k not in excluded_list and k not in archive_dict['archived']
    }

    return old_enough_projects_dict


def get_all_dirs(archive_dict, proj_52, proj_53) -> list:
    """
    Function to get all directories in staging52 and 53
    Exclude those which had been archived
    Combine both list and return

    Inputs:
        archive_dict: archive pickle to get previously archived directories
        proj_52: proj-id of staging52
        proj_53: proj-id of staging53

    Return:
        List of tuple for ease of processing later on
        Tuple content:
        1. dir name (e.g. 210407_A01295_0010_AHWL5GDRXX) for 002 query
        2. proj-id (staging52 or 53) for 002 query
        3. string '52' or '53'
        4. original directory path (e.g. /210407_A01295_0010_AHWL5GDRXX)
    """

    staging52 = dx.DXProject(proj_52)
    staging53 = dx.DXProject(proj_53)

    # get all folders in staging52
    all_folders_in_52 = staging52.list_folder(only='folders')['folders']
    directories_in_52 = [
        (file.lstrip('/').lstrip('/processed'), proj_52, '52', file)
        for file in all_folders_in_52 if file != '/processed']
    directories_in_52_processed = [
        (file.lstrip('/').lstrip('/processed'), proj_52, '52', file)
        for file in staging52.list_folder(
            '/processed', only='folders')['folders']]

    # get all folders in staging53
    excluded_directories = ['/MVZ_upload', '/Reports', '/dx_describe']
    all_folders_in_53 = staging53.list_folder(only='folders')['folders']
    directories_in_53 = [
        (file.lstrip('/').lstrip('/processed'), proj_53, '53', file)
        for file in all_folders_in_53 if file not in excluded_directories]

    # combine both directories
    all_directories = \
        directories_in_52 + directories_in_52_processed + directories_in_53

    # remove dirs which had been archived
    archived_dirs = \
        archive_dict['archived_52'] + archive_dict['archived_53']

    all_directories = [
        dir for dir in all_directories if dir[3] not in archived_dirs]

    return all_directories


def archive_skip_function(dir, proj, archive_dict, temp_dict, num):
    """
    Function to archive directories in staging52 / 53.

    If there is 'never-archive', return

    If there is 'no-archive' tag in any file within the directory,
    the dir will be skipped, folder is remembered in archive_pickle['skipped']

    If there is no tag in any files, directory will be archived.

    Input:
        dir: directory in staging52/53
        proj: either staging52 / 53
        archive_dict: the archive pickle for remembering skipped and
                        archived files
        temp_dict: temporary dictionary for slack notification later
        num: either 52 / 53

    Returns:
        None
    """

    never_archive = list(dx.find_data_objects(
        project=proj,
        folder=dir,
        tags=['never-archive']
        ))

    if never_archive:
        log.info(f'NEVER_ARCHIVE {dir} in staging{num}')
        return

    folders = list(dx.find_data_objects(
        project=proj,
        folder=dir,
        tags=['no-archive']
        ))

    if folders:
        log.info(f'SKIPPED {dir} in staging{num}')
    else:
        log.info(f'archiving staging{num}: {dir}')
        # dx.api.project_archive(
        #     proj, input_params={'folder': dir})
        archive_dict[f'archived_{num}'].append(dir)
        temp_dict['archived'].append(f'{proj}:{dir}')


def find_projs_and_notify(archive_pickle):
    """
    Function to find projs or directories in staging52/53
    which has not been modified in the last X months (inactive)
    and send Slack notification about it.
    """

    log.info('Start finding projs and notify')

    dx_login()

    # special notify include those projs / directories in staging52/53
    # which has been tagged 'no-archive' before but has not been modified
    # for X months. It will be listed under its own column in Slack msg
    # to make it more visible
    special_notify = []
    to_be_archived_list = []

    # get all old enough projects
    old_enough_projects_dict = get_all_old_enough_projs(MONTH, archive_pickle)

    log.info(f'No. of old enough projects: {len(old_enough_projects_dict)}')

    # get all directories
    all_directories = get_all_dirs(archive_pickle, PROJECT_52, PROJECT_53)

    log.info(f'Processing {len(all_directories)} directories in staging52/53')

    # check if directories have 002 projs made and 002 has not been modified
    # in the last X month
    old_enough_directories = [
        file for file in all_directories if check_dir(file[0], MONTH)]

    log.info(f'No. of old enough directories: {len(old_enough_directories)}')

    # get proj-id of each projs
    if old_enough_projects_dict:
        log.info('Saving project-id to pickle')

        for k, v in old_enough_projects_dict.items():
            tags = [tag.lower() for tag in v['describe']['tags']]

            # if 'never-archive', move on
            # if 'no-archive', remove and put it in
            # to-be-archived list & special notify
            # for Slack notification

            if 'never-archive' in tags:
                log.info(f'NEVER_ARCHIVE: {k}')
                continue
            elif 'no-archive' in tags:
                id = remove_proj_tag(k)
                log.info(f'REMOVE_TAG: {id}')

                special_notify.append(v['describe']['name'])
                archive_pickle['to_be_archived'].append(v['id'])
                to_be_archived_list.append(v['describe']['name'])

            else:
                archive_pickle['to_be_archived'].append(v['id'])
                to_be_archived_list.append(v['describe']['name'])

    # sieve through each directory in staging52/53
    if old_enough_directories:
        log.info('Saving directories to pickle')

        for _, proj, file_num, original_dir in old_enough_directories:
            # if there's 'never-archive' tag in any file, continue
            never_archive = list(dx.find_data_objects(
                project=proj,
                folder=original_dir,
                tags=['never-archive']
            ))

            if never_archive:
                log.info(f'NEVER_ARCHIVE: {original_dir} in staging{file_num}')
                continue

            # check for 'no-archive' tag in any files
            files = list(dx.find_data_objects(
                project=proj,
                folder=original_dir,
                tags=['no-archive'],
                describe=True
            ))

            if not files:
                archive_pickle[f'staging_{file_num}'].append(original_dir)
            else:
                # check if files are active in the last X month
                # if no, remove tag and list for special notify
                # if yes, continue
                if any([older_than(
                        MONTH, f['describe']['modified']) for f in files]):

                    log.info(
                        f'REMOVE_TAG: removing tag for {len(files)} files')
                    for file in files:
                        dx.api.file_remove_tags(
                            file['id'],
                            input_params={
                                'tags': ['no-archive'],
                                'project': proj})
                    special_notify.append(
                        f'{original_dir} in staging{file_num}')
                    archive_pickle[f'staging_{file_num}'].append(original_dir)
                else:
                    log.info(f'SKIPPED: {original_dir} in staging{file_num}')
                    continue

    # get everything ready for slack notification
    proj_list = to_be_archived_list
    folders52 = archive_pickle['staging_52']
    folders53 = archive_pickle['staging_53']

    lists = [
          list(set(proj_list)),
          list(set(folders52)),
          list(set(folders53)),
          list(set(special_notify))
        ]

    # send slack notification if there's old-enough dir / projs
    for index, data in enumerate(lists):
        if data:
            post_message_to_slack(
                channel='egg-alerts',
                index=index,
                data=data
                )

    # save dict
    log.info('Writing into pickle file')
    with open(ARCHIVE_PICKLE_PATH, 'wb') as f:
        pickle.dump(archive_pickle, f)

    log.info('End of finding projs and notify')


def archiving_function(archive_pickle):
    """
    Function to check previously listed projs and dirs
    which have not been modified (inactive) in the last X months
    and do the archiving.

    Skip projs tagged 'no-archive' or any directory with one file within
    tagged with 'no-archive'

    """

    log.info('Start archiving')

    dx_login()

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
            if 'never-archive' in proj_desc['tags']:
                log.info(f'NEVER_ARCHIVE {proj_name}')
                continue
            elif 'no-archive' in proj_desc['tags']:
                log.info(f'SKIPPED {proj_name}')
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
    log.info('Writing into pickle file')
    with open(ARCHIVE_PICKLE_PATH, 'wb') as f:
        pickle.dump(archive_pickle, f)

    log.info('End of archiving')

    find_projs_and_notify(archive_pickle)


def main():

    archive_pickle = read_or_new_pickle(ARCHIVE_PICKLE_PATH)
    to_be_archived = archive_pickle['to_be_archived']
    staging52 = archive_pickle['staging_52']
    staging53 = archive_pickle['staging_53']

    if to_be_archived or staging52 or staging53:
        archiving_function(archive_pickle)
    else:
        find_projs_and_notify(archive_pickle)

    log.info('End of script.')


if __name__ == "__main__":
    main()
