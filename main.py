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
import pickle
import collections
import datetime as dt
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from member.members import MEMBER_LIST
from dotenv import load_dotenv

# for sending helpdesk email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

from helper import get_logger

log = get_logger("main log")
load_dotenv()

SLACK_TOKEN = os.environ['SLACK_TOKEN']
DNANEXUS_TOKEN = os.environ['DNANEXUS_TOKEN']

PROJECT_52 = os.environ['PROJECT_52']
PROJECT_53 = os.environ['PROJECT_53']
MONTH2 = int(os.environ['AUTOMATED_MONTH_002'])
MONTH3 = int(os.environ['AUTOMATED_MONTH_003'])
TAR_MONTH = int(os.environ['TAR_MONTH'])
ARCHIVE_MODIFIED_MONTH = int(os.environ['ARCHIVE_MODIFIED_MONTH'])
ARCHIVE_PICKLE_PATH = os.environ['AUTOMATED_ARCHIVE_PICKLE_PATH']
ARCHIVED_TXT_PATH = os.environ['AUTOMATED_ARCHIVED_TXT_PATH']
URL_PREFIX = 'https://platform.dnanexus.com/panx/projects'

SERVER = os.environ['ANSIBLE_SERVER']
PORT = os.environ['ANSIBLE_PORT']
SENDER = os.environ['ANSIBLE_SENDER']
RECEIVERS = os.environ['ANSIBLE_RECEIVERS']


def messages(purpose, today, day=None, error_msg=None):
    """
    Function to return the right message for the give purpose

    Inputs:
        purpose: decide on which message to return (etc, 002_proj, alert..)
        today: today's date to display on Slack message
        day: tuple of dates (vary) depending on purpose
        error_msg: error message from if purpose == alert (dxpy fail)

    Return:
        string of message
    """

    messages = {
        '002_proj':
        (
            f':bangbang: {today} *002 projects to be archived:*'
            '\n_Please tag `no-archive` or `never-archive`_'
            f'\n*Archive date: {day[0]}*'
        ),
        '003_proj':
        (
            f':bangbang: {today} *003 projects to be archived:*'
            '\n_Please tag `no-archive` or `never-archive`_'
            f'\n*Archive date: {day[0]}*'
        ),
        'staging_52':
        (
            f':bangbang: {today} *Directories in `staging52` to be archived:*'
            '\n_Please tag `no-archive` or `never-archive`_'
            f'\n*Archive date: {day[0]}*'
        ),
        'special_notify':
        (
            f':warning: {today} *Inactive project or directory to be archived*'
            '\n_unless re-tag `no-archive` or `never-archive`_'
            f'\n*Archive date: {day[0]}*'
        ),
        'no_archive':
        (
            f':male-detective: {today} *Projects or directory'
            ' tagged with `no-archive`:*'
            '\n_just for your information_'
        ),
        'never_archive':
        (
            f':female-detective: {today} *Projects or directory'
            ' tagged with `never-archive`:*'
            '\n_just for your information_'
        ),
        'archived':
        (
            ':closed_book: *Projects or directory archived:*'
        ),
        'countdown':
        (
            f'automated-archiving: '
            f'{day[0]} day till archiving on {day[1]}'
        ),
        'alert':
        (
            "automated-archiving: Error with dxpy token! Error code:\n"
            f"`{error_msg}`"
        ),
        'tar_notify':
        (
            'automated-tar-notify: '
            '`tar.gz` files not modified in the last 3 month'
            f'\nEarliest Date: {day[0]} -- Latest Date: {day[1]}'
            '\n_Please find complete list of file-id below:_'
        )
    }

    # just in case
    if purpose not in messages.keys():
        return None

    return messages[purpose]


def post_message_to_slack(
        channel,
        purpose,
        data=None,
        error=None,
        day=(None, None)
        ) -> None:
    """
    Request function for slack web api for:
    (1) send alert msg when dxpy auth failed (alert=True)
    (2) send to-be-archived notification (default alert=False)

    Inputs:
        channel: e.g. egg-alerts, egg-logs
        purpose: this decide what message to send
        data: list of projs / dirs to be archived
        error: (optional) (required only when dxpy auth failed) dxpy error msg
        day: (optional) tuple of (day till next date, next run date) depend
        on purpose

    Return:
        None
    """

    http = requests.Session()
    retries = Retry(total=5, backoff_factor=10, method_whitelist=['POST'])
    http.mount("https://", HTTPAdapter(max_retries=retries))

    receivers = RECEIVERS.split(',') if ',' in RECEIVERS else [RECEIVERS]

    log.info(f'Posting data for: {purpose}')

    today = dt.date.today().strftime("%d/%m/%Y")
    message = messages(purpose, today, day, error)

    log.info(f'Sending POST request to channel: #{channel}')

    try:
        if purpose in ['alert', 'countdown']:
            response = http.post(
                'https://slack.com/api/chat.postMessage', {
                    'token': SLACK_TOKEN,
                    'channel': f'#{channel}',
                    'text': message
                }).json()
        elif purpose == 'tar_notify':

            with open('tar.txt', 'w') as f:
                for line in data:
                    txt = ' '.join(line)
                    f.write(f'{txt}\n')

            tar_file = {
                'file': ('tar.txt', open('tar.txt', 'rb'), 'txt')
                }
            response = http.post(
                'https://slack.com/api/files.upload',
                params={
                    'token': SLACK_TOKEN,
                    'channels': f'#{channel}',
                    'initial_comment': message,
                    'filename': 'tar.txt',
                    'filetype': 'txt'
                },
                files=tar_file
                ).json()
        else:
            # default notification
            text_data = '\n'.join(data)

            response = http.post(
                'https://slack.com/api/chat.postMessage', {
                    'token': SLACK_TOKEN,
                    'channel': f'#{channel}',
                    'attachments': json.dumps([{
                        "pretext": message,
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


def send_mail(send_from, send_to, subject, text) -> None:
    """
    Function to send email to helpdesk

    Inputs:
        send_from: BioinformaticsTeamGeneticsLab@addenbrookes.nhs.uk
        send_to: list of emails
        subject: message subject
        text: message body

    Return:
        None
    """
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


def read_or_new_pickle(path) -> dict:
    """
    Read stored pickle memory for the script
    Using defaultdict() automatically create new dict.key()

    Input:
        Path to store the pickle (memory)

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


def check_dir(dir, month) -> bool:
    """
    Function to check if project (002/003) for that directory
    exist. e.g. For 210407_A01295_0010_AHWL5GDRXX
    it will find 002_210407_A01295_0010_AHWL5GDRXX project

    If the 002/003 exist, we check if the proj has been inactive
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


def dx_login() -> None:
    """
    DNANexus login check function.
    If fail, send Slack notification

    Returns:
        None
    """

    DX_SECURITY_CONTEXT = {
        "auth_token_type": "Bearer",
        "auth_token": DNANEXUS_TOKEN
        }

    dx.set_security_context(DX_SECURITY_CONTEXT)

    try:
        log.info('Checking DNANexus login')
        dx.api.system_whoami()
        log.info('DNANexus login successful')

    except Exception as err:
        error_msg = err.error_message()
        log.error('Error with DNANexus login')
        log.error(f'Error message from DNANexus: {error_msg}')

        post_message_to_slack(
            'egg-alerts',
            'alert',
            error=error_msg,
            )

        log.info('End of script')
        sys.exit()


def remove_proj_tag(proj) -> None:
    """
    Function to remove tag 'no-archive' for project

    When a project has been tagged 'no-archive' but
    has not been modified for X months
    the tag will be removed and the project will be
    notified to be archived

    This only applies to project-id
    Not directories in Staging52

    Input:
        project-id

    Returns:
        project-id of tag-removed project
    """

    log.info(f'REMOVE_TAG: {proj}')
    dx.api.project_remove_tags(
        proj, input_params={'tags': ['no-archive']})


def get_all_projs():
    """
    Get all 002 and 003 projects

    Returns:
        2 dict for 002 and 003 projs
    """
    # Get all 002 and 003 projects
    projects_dict_002 = dict()
    projects_dict_003 = dict()

    projects002 = list(dx.search.find_projects(
        name='^002.*',
        name_mode='regexp',
        billed_to='org-emee_1',
        describe=True
        ))

    projects003 = list(dx.search.find_projects(
        name='^003.*',
        name_mode='regexp',
        billed_to='org-emee_1',
        describe=True
        ))

    # put all projects into a dict
    projects_dict_002.update({proj['id']: proj for proj in projects002})
    projects_dict_003.update({proj['id']: proj for proj in projects003})

    return projects_dict_002, projects_dict_003


def get_all_old_enough_projs(month2, month3, archive_dict) -> dict:
    """
    Get all 002 and 003 projects which are not modified
    in the last X months. Exclude projects: staging 52
    as they will be processed separately + exclude projects
    which had been archived.

    Input:
        month2: duration of inactivity in the last x month for 002
        month3: duration of inactivity in the last x month for 003
        archive_dict: the archive pickle to remember what file to be archived

    Returns (dict):
        dictionary of key (proj-id) and
        value (describe JSON from dxpy for the proj)

    """

    # Get all 002 and 003 projects
    projects_dict_002, projects_dict_003 = get_all_projs()

    # sieve the dict to include only old-enough projs
    old_enough_projects_dict_002 = {
        k: v for k, v in projects_dict_002.items() if older_than(
            month2, v['describe']['modified'])}
    old_enough_projects_dict_003 = {
        k: v for k, v in projects_dict_003.items() if older_than(
            month3, v['describe']['modified'])}

    old_enough_projects_dict = {
        **old_enough_projects_dict_002, **old_enough_projects_dict_003}

    excluded_list = [PROJECT_52, PROJECT_53]

    # exclude projs (staging52/53) and archived projs
    old_enough_projects_dict = {
        k: v for k, v in old_enough_projects_dict.items()
        if k not in excluded_list and k not in archive_dict['archived']
    }

    return old_enough_projects_dict


def get_all_dirs(archive_dict, proj_52) -> list:
    """
    Function to get all directories in staging52
    Exclude those which had been archived

    Inputs:
        archive_dict: archive pickle to get previously archived directories
        proj_52: proj-id of staging52

    Return:
        List of tuple for ease of processing later on
        Tuple content:
        1. dir name (e.g. 210407_A01295_0010_AHWL5GDRXX) for 002 query
        2. original directory path (e.g. /210407_A01295_0010_AHWL5GDRXX/)
    """

    staging52 = dx.DXProject(proj_52)

    # get all folders in staging52
    all_folders_in_52 = staging52.list_folder(only='folders')['folders']
    directories_in_52 = [
        (file.lstrip('/').lstrip('/processed'), file)
        for file in all_folders_in_52 if file != '/processed']
    directories_in_52_processed = [
        (file.lstrip('/').lstrip('/processed'), file)
        for file in staging52.list_folder(
            '/processed', only='folders')['folders']]

    # combine both directories
    all_directories = directories_in_52 + directories_in_52_processed

    # remove dirs which had been archived
    archived_dirs = archive_dict['archived_52']
    all_directories = [
        dir for dir in all_directories if dir[1] not in archived_dirs]

    return all_directories


def archive_skip_function(dir, proj, archive_dict, temp_dict) -> None:
    """
    Function to archive directories in staging52

    If there is 'never-archive', return

    If recently modified, return

    If there is 'no-archive' tag in any file within the directory,
    the dir will be skipped

    If there is no tag in any files, directory will be archived.

    Input:
        dir: directory in staging52
        proj: staging52 project id
        archive_dict: the archive pickle for remembering skipped and
                        archived files
        temp_dict: temporary dictionary for slack notification later

    Returns:
        None
    """

    never_archive = list(dx.find_data_objects(
        project=proj,
        folder=dir,
        tags=['never-archive']
        ))

    if never_archive:
        log.info(f'NEVER_ARCHIVE: {dir} in staging52')
        return

    # 2 * 4 week = 8 weeks
    num_weeks = ARCHIVE_MODIFIED_MONTH * 4

    # check if there's any files modified in the last num_weeks
    recent_modified = list(dx.find_data_objects(
        project=proj,
        folder=dir,
        modified_after=f'-{num_weeks}w'
    ))

    if recent_modified:
        log.info(f'RECENTLY MODIFIED: {dir} in staging52')
        return

    folders = list(dx.find_data_objects(
        project=proj,
        folder=dir,
        tags=['no-archive']
        ))

    if folders:
        log.info(f'SKIPPED: {dir} in staging52')
        return
    else:
        log.info(f'ARCHIVING staging52: {dir}')
        res = dx.api.project_archive(
            proj, input_params={'folder': dir})
        if res['count'] != 0:
            archive_dict['archived_52'].append(dir)
            temp_dict['archived'].append(f'{proj}:{dir}')


def get_tag_status(proj_52):
    """
    Function to get the latest tag status in staging52 and projects

    Input:
        proj_52: staging52 project-id

    Returns:
        2 list of proj & directories in staging52 tagged with either
        no-archive and never-archive
    """

    no_archive_list = []
    never_archive_list = []

    # check no-archive tag in staging52
    temp_no_archive = list(
        dx.find_data_objects(
            project=proj_52, tag='no-archive', describe=True))
    # check never-archive tag in staging52
    temp_never_archive = list(
        dx.find_data_objects(
            project=proj_52, tag='never-archive', describe=True))

    # get the directory name
    temp_no_archive = [
        t['describe']['folder'].lstrip('/') for t in temp_no_archive]
    temp_never_archive = [
        t['describe']['folder'].lstrip('/') for t in temp_never_archive]

    # clean the directory path and append to list
    for dir in temp_no_archive:
        temp = dir.split('/')
        if 'processed' in dir:
            no_archive_list.append(f'/{temp[0]}/{temp[1]} in `staging52`')
        else:
            no_archive_list.append(f'{temp[0]} in `staging52`')

    for dir in temp_never_archive:
        temp = dir.split('/')
        if 'processed' in dir:
            never_archive_list.append(f'/{temp[0]}/{temp[1]} in `staging52`')
        else:
            never_archive_list.append(f'{temp[0]} in `staging52`')

    # check no-archive & never-archive in projects
    # Get all 002 and 003 projects
    projects_dict_002, projects_dict_003 = get_all_projs()

    # get proj tagged with no-archive or never-archive for notify later
    agg_dict = {
        **projects_dict_002, **projects_dict_003}

    proj_no_archive = [
        proj['describe']['name'] for proj in agg_dict.values() if
        'no-archive' in proj['describe']['tags']]
    proj_never_archive = [
        proj['describe']['name'] for proj in agg_dict.values() if
        'never-archive' in proj['describe']['tags']]

    no_archive_list += proj_no_archive
    never_archive_list += proj_never_archive

    return no_archive_list, never_archive_list


def find_projs_and_notify(archive_pickle, today):
    """
    Function to find projs or directories in staging52
    which has not been modified in the last X months (inactive)
    and send Slack notification.

    Inputs:
        archive_pickle: to remember to-be-archived files
        today: today's date to get next_archiving date + include
        in Slack notification

    Return:
        None
    """

    log.info('Start finding projs and notify')

    # special notify include those projs / directories in staging52
    # which has been tagged 'no-archive' before but has not been modified
    # for X months. It will be listed under its own column in Slack msg
    # to make it more visible
    special_notify_list = []
    to_be_archived_list = collections.defaultdict(list)
    to_be_archived_dir = []

    # get all old enough projects
    old_enough_projects_dict = get_all_old_enough_projs(
        MONTH2, MONTH3, archive_pickle)

    log.info(f'No. of old enough projects: {len(old_enough_projects_dict)}')

    # get all directories
    all_directories = get_all_dirs(archive_pickle, PROJECT_52)

    log.info(f'Processing {len(all_directories)} directories in staging52')

    # check if directories have 002 projs made and 002 has not been modified
    # in the last X month
    old_enough_directories = [
        file for file in all_directories if check_dir(file[0], MONTH2)]

    log.info(f'No. of old enough directories: {len(old_enough_directories)}')

    # get proj-id of each projs
    if old_enough_projects_dict:
        log.info('Processing projects')

        for k, v in old_enough_projects_dict.items():
            tags = [tag.lower() for tag in v['describe']['tags']]

            # if 'never-archive', move on
            # if 'no-archive', remove and put it in
            # to-be-archived list & special notify
            # for Slack notification

            proj_name = v['describe']['name']
            trimmed_id = k.lstrip('project-')
            created_by = v['describe']['createdBy']['user']

            if 'never-archive' in tags:
                log.info(f'NEVER_ARCHIVE: {k}')
                continue
            elif 'no-archive' in tags:
                remove_proj_tag(k)

                special_notify_list.append(proj_name)
                archive_pickle['to_be_archived'].append(k)

                if proj_name.startswith('002'):
                    to_be_archived_list['002'].append(
                        f'<{URL_PREFIX}/{trimmed_id}/|{proj_name}>')
                else:
                    to_be_archived_list['003'].append({
                        'user': created_by,
                        'link': f'<{URL_PREFIX}/{trimmed_id}/|{proj_name}>'
                    })

            else:
                archive_pickle['to_be_archived'].append(k)
                if proj_name.startswith('002'):
                    to_be_archived_list['002'].append(
                        f'<{URL_PREFIX}/{trimmed_id}/|{proj_name}>')
                else:
                    to_be_archived_list['003'].append({
                        'user': created_by,
                        'link': f'<{URL_PREFIX}/{trimmed_id}/|{proj_name}>'
                    })

    # sieve through each directory in staging52
    if old_enough_directories:
        log.info('Processing directories')

        trimmed_proj = PROJECT_52.lstrip('project-')

        for _, original_dir in old_enough_directories:

            trimmed_dir = original_dir.lstrip('/')

            # if there's 'never-archive' tag in any file, continue
            never_archive = list(dx.find_data_objects(
                project=PROJECT_52,
                folder=original_dir,
                tags=['never-archive']
            ))

            if never_archive:
                log.info(f'NEVER_ARCHIVE: {original_dir} in staging52')
                continue

            # check for 'no-archive' tag in any files
            files = list(dx.find_data_objects(
                project=PROJECT_52,
                folder=original_dir,
                tags=['no-archive'],
                describe=True
            ))

            STAGING_PREFIX = f'{URL_PREFIX}/{trimmed_proj}/data'

            if not files:
                archive_pickle[f'staging_52'].append(original_dir)
                to_be_archived_dir.append(
                    f'<{STAGING_PREFIX}/{trimmed_dir}|{original_dir}>')
            else:
                # check if files are active in the last X month
                # if no, remove tag and list for special notify
                # if yes, continue
                if all(
                    [older_than(
                            MONTH2, f['describe']['modified']) for f in files]
                            ):
                    log.info(
                        f'REMOVE_TAG: removing tag for {len(files)} file(s)')
                    for file in files:
                        dx.api.file_remove_tags(
                            file['id'],
                            input_params={
                                'tags': ['no-archive'],
                                'project': PROJECT_52})
                    special_notify_list.append(
                        f'{original_dir} in `staging52`')
                    archive_pickle[f'staging_52'].append(original_dir)
                    to_be_archived_dir.append(
                        f'<{STAGING_PREFIX}/{trimmed_dir}|{original_dir}>')
                else:
                    log.info(f'SKIPPED: {original_dir} in staging52')
                    continue

    no_archive_list, never_archive_list = get_tag_status(PROJECT_52)

    # get everything ready for slack notification
    proj002 = sorted(to_be_archived_list['002'])
    proj003 = []
    folders52 = sorted(to_be_archived_dir)
    no_archive_list = sorted(no_archive_list)
    never_archive_list = sorted(never_archive_list)

    # process 003 list to sort by user
    temp003 = to_be_archived_list['003']
    if temp003:
        temp003 = sorted(temp003, key=lambda d: d['user'])
        current_usr = None
        for link in temp003:
            if current_usr != link['user']:
                current_usr = link['user']

                if current_usr in MEMBER_LIST.keys():
                    proj003.append(f'<@{MEMBER_LIST[current_usr]}>')
                else:
                    proj003.append(f'Can\'t find ID for: {current_usr}')
            proj003.append(link['link'])

    big_list = [
          ('002_proj', proj002),
          ('003_proj', proj003),
          ('staging_52', folders52),
          ('special_nofify', special_notify_list),
          ('no_archive', no_archive_list),
          ('never_archive', never_archive_list)
        ]

    # send slack notification if there's old-enough dir / projs
    next_archiving_date = get_next_archiving_date(today)

    for purpose, data in big_list:
        if data:
            data.append('-- END OF MESSAGE --')

            post_message_to_slack(
                'egg-alerts',
                purpose,
                data=data,
                day=(next_archiving_date, None)
                )
        else:
            continue

    # save dict (only if there's to-be-archived)
    if proj002 or proj003 or folders52:
        log.info('Writing into pickle file')
        with open(ARCHIVE_PICKLE_PATH, 'wb') as f:
            pickle.dump(archive_pickle, f)

    log.info('End of finding projs and notify')


def archiving_function(archive_pickle, today):
    """
    Function to check previously listed projs and dirs (memory)
    and do the archiving.

    Skip projs if:
    1. tagged 'no-archive' or any directory with one file within
    tagged with 'no-archive'
    2. modified in the past TAR_MONTH month
    3. tagged 'never-archive'

    """

    log.info('Start archiving function')

    list_of_projs = archive_pickle['to_be_archived']
    list_of_dirs_52 = archive_pickle['staging_52']

    # just for recording what has been archived
    # plus for Slack notification
    temp_archived = collections.defaultdict(list)

    # do the archiving
    if list_of_projs:
        for id in list_of_projs:
            project = dx.DXProject(id)
            proj_desc = project.describe()
            proj_name = proj_desc['name']
            modified_epoch = proj_desc['modified']

            # check if proj been tagged with 'no-archive'
            if 'never-archive' in proj_desc['tags']:
                log.info(f'NEVER_ARCHIVE: {proj_name}')
                continue
            elif 'no-archive' in proj_desc['tags']:
                log.info(f'SKIPPED: {proj_name}')
                continue
            else:
                if older_than(ARCHIVE_MODIFIED_MONTH, modified_epoch):
                    # True if not modified in the last 3 month

                    log.info(f'ARCHIVING {id}')
                    res = dx.api.project_archive(id)
                    # if res.count = 0, no files are being archived
                    # so we save output only on res.count != 0
                    if res['count'] != 0:
                        archive_pickle['archived'].append(id)
                        temp_archived['archived'].append(id)
                else:
                    log.info(f'RECENTLY MODIFIED & SKIPPED: {proj_name}')
                    continue

    if list_of_dirs_52:
        for dir in list_of_dirs_52:
            archive_skip_function(
                dir, PROJECT_52, archive_pickle, temp_archived)

    # generate archiving txt file
    # ONLY IF THERE IS FILEs BEING ARCHIVED
    if temp_archived:
        if os.path.isfile(ARCHIVED_TXT_PATH):
            with open(ARCHIVED_TXT_PATH, 'a') as f:
                f.write(f'=== {today} ===')

                for line in temp_archived['archived']:
                    f.write('\n' + line)
        else:
            with open(ARCHIVED_TXT_PATH, 'w') as f:
                f.write(f'=== {today} ===')
                f.write('\n'.join(temp_archived['archived']))

        # also send a notification to say what have been archived
        post_message_to_slack(
            'egg-alerts',
            'archived',
            data=temp_archived['archived']
            )

    # empty pickle
    log.info('Clearing pickle file')

    archive_pickle['to_be_archived'] = []
    archive_pickle['staging_52'] = []

    # save dict
    log.info('Writing into pickle file')
    with open(ARCHIVE_PICKLE_PATH, 'wb') as f:
        pickle.dump(archive_pickle, f)

    log.info('End of archiving function')

    find_projs_and_notify(archive_pickle, today)


def get_next_archiving_date(today):
    """
    Function to get the next automated-archive run date

    Input:
        today (datetime)

    Return:
        If today.day is between 1-15: return 15
        If today.day is after 15: return 1st day of next month

    """

    while today.day not in [1, 15]:
        today += dt.timedelta(1)

    return today


def make_datetime_format(modified_epoch):
    """
    Function to turn modified epoch (returned by DNANexus)
    into readable datetime format

    Return:
        datetime

    """

    modified = modified_epoch / 1000.0
    modified_dt = dt.datetime.fromtimestamp(modified)

    return modified_dt


def get_old_tar_and_notify():
    """
    Function to get tar which are not modified in the last 3 months
    Regex Format:
        only returns "run.....tar.gz" in staging52

    Return:
        list of tar files not modified in the last 3 months
        min_date: earliest date among the list of tars
        max_date: latest date among the list of tars

    """
    log.info('Getting old tar.gz in staging52')

    result = list(
        dx.find_data_objects(
            name='^run.*.tar.gz',
            name_mode='regexp',
            describe=True,
            project=PROJECT_52))

    filtered_result = [
        x for x in result if older_than(TAR_MONTH, x['describe']['modified'])]

    id_results = [(x['id'], x['describe']['folder']) for x in filtered_result]

    dates = [make_datetime_format(
        d['describe']['modified']) for d in filtered_result]

    min_date = min(dates).strftime('%Y-%m-%d')
    max_date = max(dates).strftime('%Y-%m-%d')

    post_message_to_slack(
        'egg-alerts',
        'tar_notify',
        data=id_results,
        day=(min_date, max_date)
    )


def main():

    archive_pickle = read_or_new_pickle(ARCHIVE_PICKLE_PATH)
    to_be_archived = archive_pickle['to_be_archived']
    staging52 = archive_pickle['staging_52']

    today = dt.date.today()

    if today.day in [1, 15]:
        log.info(today)
        log.info('Today is archiving run date')

        dx_login()

        if today.day == 1:
            get_old_tar_and_notify()

        # if there is something in memory
        # we run archive function
        # else we find_and_notify
        if to_be_archived or staging52:
            archiving_function(archive_pickle)
        else:
            find_projs_and_notify(archive_pickle, today)

    elif today.day > 1 and today.day < 15:
        log.info(today)
        log.info('Today is within 1-15')

        if to_be_archived or staging52:
            # if there's to-be-archived in memory
            # we do the countdown to egg-alerts
            # else we just keep silence

            next_archiving_date = get_next_archiving_date(today)
            diff = next_archiving_date - today

            post_message_to_slack(
                'egg-alerts',
                'countdown',
                day=(diff.days, next_archiving_date),
                )
        else:
            log.info('There is no data in memory')

    else:
        log.info(today)
        log.info('Today is within 15-31')

        if to_be_archived or staging52:

            next_archiving_date = get_next_archiving_date(today)
            diff = next_archiving_date - today

            post_message_to_slack(
                'egg-alerts',
                'countdown',
                day=(diff.days, next_archiving_date),
                )
        else:
            log.info('There is no data in memory')

    log.info('End of script.')


if __name__ == "__main__":
    main()
