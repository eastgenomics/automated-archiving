"""
Automated-archiving

This script will check for projs and directories within staging52/53
which has not been active for the past X months (inactive). It will then
send a Slack notification to notify the will-be-archived files

The second run of the script will start the archiving process previously
noted to-be-archive files.
It skips files tagged with 'no-archive' / 'never-archive'

"""

import os
import sys
import dxpy as dx
import pickle
import collections
import datetime as dt

from xmlrpc.client import DateTime
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from typing import Union
from dotenv import load_dotenv

from helper import get_logger
from notify import Slack

from member.members import MEMBER_LIST

load_dotenv()

logger = get_logger("main-log")

try:
    logger.info('Reading env variables')

    DEBUG = os.environ.get('ARCHIVE_DEBUG', False)
    if DEBUG:
        logger.info('Running in DEBUG mode')
    else:
        logger.info('Running in PRODUCTION mode')

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
    AUTOMATED_REGEX_EXCLUDE = [
        text.strip() for text in os.environ['AUTOMATED_REGEX_EXCLUDE'].split()]

    SERVER = os.environ['ANSIBLE_SERVER']
    PORT = os.environ['ANSIBLE_PORT']
    SENDER = os.environ['ANSIBLE_SENDER']
    RECEIVERS = os.environ['ANSIBLE_RECEIVERS']

except Exception as err:
    logger.error(err)
    logger.info('End of script')
    sys.exit()

slack = Slack(
    SLACK_TOKEN,
    RECEIVERS,
    TAR_MONTH,
    DEBUG,
    SENDER,
    SERVER,
    PORT
)


def read_or_new_pickle(path: str) -> dict:
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


def older_than(month: int, modified_epoch: int) -> bool:
    """
    Determine if a modified epoch date is older than X month

    Inputs:
        month: X month (int)
        modified_epoch: proj modified date (epoch)

    Returns (Boolean):
        True if haven't been modified in last X month
        False if have been modified in last X month
    """

    modified = modified_epoch / 1000.0
    date = dt.datetime.fromtimestamp(modified)

    return date + relativedelta(months=+month) < dt.datetime.today()


def check_dir(dir: str, month: int) -> bool:
    """
    Function to check if project (002/003) for that directory
    exist. e.g. For 210407_A01295_0010_AHWL5GDRXX
    it will find 002_210407_A01295_0010_AHWL5GDRXX project

    If the 002/003 exist, we check if the proj has been inactive
    for the last X month. If yes, return True.

    Inputs:
        dir: trimmed directory (str),
        month: X month (int)

    Returns:
        Boolean:
        True if its 002 has not been active for X month
        False if no 002 returned / 002 been active in past X month
    """

    result = list(
        dx.find_projects(
            dir,
            name_mode='regexp',
            describe={'fields': {'modified': True}},
            limit=1))

    # if no 002/003 project
    if not result:
        return False

    modified_epoch = result[0]['describe']['modified']

    # check modified date of the 002/003 proj
    if older_than(month, modified_epoch):
        return True
    else:
        return False


def dx_login(today: DateTime) -> None:
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
        logger.info('Checking DNANexus login')
        dx.api.system_whoami()
        logger.info('DNANexus login successful')

    except Exception as err:
        error_msg = err.error_message()
        logger.error('Error with DNANexus login')
        logger.error(f'Error message from DNANexus: {error_msg}')

        slack.post_message_to_slack(
            channel='egg-alerts',
            purpose='alert',
            today=today,
            error=error_msg,
            )

        logger.info('End of script')
        sys.exit()


def remove_proj_tag(proj: str) -> None:
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

    logger.info(f'REMOVE_TAG: {proj}')
    dx.api.project_remove_tags(
        proj, input_params={'tags': ['no-archive']})


def get_all_projs() -> Union[dict, dict]:
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
        describe={
            'fields': {
                'name': True,
                'tags': True,
                'dataUsage': True,
                'archivedDataUsage': True,
                'modified': True,
                'createdBy': True
                }}
        ))

    projects003 = list(dx.search.find_projects(
        name='^003.*',
        name_mode='regexp',
        billed_to='org-emee_1',
        describe={
            'fields': {
                'name': True,
                'tags': True,
                'dataUsage': True,
                'archivedDataUsage': True,
                'modified': True,
                'createdBy': True
                }}
        ))

    # put all projects into a dict
    projects_dict_002.update({proj['id']: proj for proj in projects002})
    projects_dict_003.update({proj['id']: proj for proj in projects003})

    return projects_dict_002, projects_dict_003


def get_all_old_enough_projs(month2: int, month3: int) -> dict:
    """
    Get all 002 and 003 projects which are not modified
    in the last X months. Exclude projects: staging 52
    as they will be processed separately + exclude projects
    which had been archived.

    Input:
        month2: duration of inactivity in the last x month for 002 (int)
        month3: duration of inactivity in the last x month for 003 (int)

    Returns (dict):
        dict of key (proj-id) and value (describe return from dxpy)

    """

    # Get all 002 and 003 projects
    projects_dict_002, projects_dict_003 = get_all_projs()

    # sieve the dict to include only old-enough projs
    # and if dataUsage != archivedDataUsage
    # if dataUsage == archivedDataUsage, it means
    # all data within the proj have been archived
    # if proj or certain files within was unarchived
    # # the dataUsage should != archivedDataUsage
    old_enough_projects_dict_002 = {
        k: v for k, v in projects_dict_002.items() if
        older_than(month2, v['describe']['modified']) and
        v['describe']['dataUsage'] != v['describe']['archivedDataUsage']
        }
    old_enough_projects_dict_003 = {
        k: v for k, v in projects_dict_003.items() if
        older_than(month3, v['describe']['modified']) and
        v['describe']['dataUsage'] != v['describe']['archivedDataUsage']
        }

    old_enough_projects_dict = {
        **old_enough_projects_dict_002, **old_enough_projects_dict_003}

    # exclude staging52 & 53 (just in case)
    excluded_list = [PROJECT_52, PROJECT_53]

    # exclude projs (staging52/53) and archived projs
    old_enough_projects_dict = {
        k: v for k, v in old_enough_projects_dict.items()
        if k not in excluded_list
    }

    # get proj tagged archive
    tagged_archive_proj = list(dx.search.find_projects(
        name='^00[2,3].*',
        name_mode='regexp',
        tags=['archive'],
        describe={
            'fields': {
                'name': True,
                'tags': True,
                'dataUsage': True,
                'archivedDataUsage': True,
                'modified': True,
                'createdBy': True
                }}
        ))

    old_enough_projects_dict.update(
        {proj['id']: proj for proj in tagged_archive_proj})

    return old_enough_projects_dict


def get_all_dirs(proj_52: str) -> list:
    """
    Function to get all directories in staging52

    Inputs:
        proj_52: proj-id of staging52

    Return:
        List of tuple for ease of processing later on
        Tuple contains:
        1. trimmed dir name (e.g. 210407_A01295_0010_AHWL5GDRXX)
            for 002 querying later on
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

    return all_directories


def directory_archive(
    dir_path: str,
    proj_id: str,
    temp_dict: dict
        ) -> None:
    """
    Function to deal with directories in staging52

    If 'never-archive', recently modified, or 'no-archive' tag
    in any file within the directory, skip

    If no tag in any files, check if there're files in directory
    fit for exclusion based on AUTOMATED_REGEX_EXCLUDE. If yes,
    Get all file-id in directory, exclude those that fit & archive
    others. If no, archive everything in directory

    Input:
        dir_path: directory in staging52
        proj_id: staging52 project id
        temp_dict: temporary dict for recording what has been archived

    Returns:
        None
    """

    # check for 'never-archive' tag in directory
    never_archive = list(dx.find_data_objects(
        project=proj_id,
        folder=dir_path,
        tags=['never-archive']
        ))

    if never_archive:
        logger.info(f'NEVER ARCHIVE: {dir_path} in staging52')
        return

    # 2 * 4 week = 8 weeks
    num_weeks = ARCHIVE_MODIFIED_MONTH * 4

    # check if there's any files modified in the last num_weeks
    recent_modified = list(dx.find_data_objects(
        project=proj_id,
        folder=dir_path,
        modified_after=f'-{num_weeks}w'
    ))

    if recent_modified:
        logger.info(f'RECENTLY MODIFIED: {dir_path} in staging52')
        return

    # check for 'no-archive' tag in directory
    no_archive = list(dx.find_data_objects(
        project=proj_id,
        folder=dir_path,
        tags=['no-archive']
        ))

    if no_archive:
        logger.info(f'NO ARCHIVE: {dir_path} in staging52')
        return
    else:
        big_exclude_list = set()

        for word in AUTOMATED_REGEX_EXCLUDE:
            exclude_list = [
                file['id'] for file in list(
                    dx.find_data_objects(
                        name=word,
                        name_mode='regexp',
                        project=proj_id,
                        folder=dir_path
                        ))]
            big_exclude_list.update(exclude_list)

        if big_exclude_list:
            all_files = [
                file['id'] for file in list(
                    dx.find_data_objects(
                        project=proj_id,
                        folder=dir_path))]
            excluded_list = [
                id for id in all_files if id not in big_exclude_list]

            logger.info(f'ARCHIVING EXCLUDE staging52: {dir_path}')

            if not DEBUG:
                for id in excluded_list:
                    logger.info(f'ARCHIVING staging52: {id}')
                    dx.DXFile(id, project=proj_id).archive()
                temp_dict['archived'].append(f'`{proj_id}` : {dir_path}')
        else:
            # if there's thing in directory to be excluded
            # else we do an overall project.archive()
            logger.info(f'ARCHIVING staging52: {dir_path}')
            if not DEBUG:
                res = dx.api.project_archive(
                    proj_id,
                    input_params={'folder': dir_path}
                    )
                if res['count'] != 0:
                    temp_dict['archived'].append(f'`{proj_id}` : {dir_path}')


def get_tag_status(proj_52: str) -> Union[list, list]:
    """
    Function to get the latest tag status in staging52 and 002 projects

    Input:
        proj_52: staging52 project-id

    Returns:
        2 list of proj & directories in staging52 tagged with either
        no-archive and never-archive
    """

    no_archive_list = []
    never_archive_list = []

    # check no-archive & never-archive tag in staging52
    directory_no_archive = list(
        dx.find_data_objects(
            project=proj_52,
            tags=['no-archive'],
            describe={'fields': {'folder': True}}))
    directory_never_archive = list(
        dx.find_data_objects(
            project=proj_52,
            tags=['never-archive'],
            describe={'fields': {'folder': True}}))

    # get the directory pathway
    directory_no_archive = [
        t['describe']['folder'].lstrip('/') for t in directory_no_archive]
    directory_never_archive = [
        t['describe']['folder'].lstrip('/') for t in directory_never_archive]

    # clean the directory path and append to list
    for directory in directory_no_archive:
        temp = directory.split('/')
        if 'processed' in directory:
            no_archive_list.append(f'/{temp[0]}/{temp[1]} in `staging52`')
        else:
            no_archive_list.append(f'{temp[0]} in `staging52`')

    for directory in directory_never_archive:
        temp = directory.split('/')
        if 'processed' in directory:
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


def find_projs_and_notify(
    archive_pickle: dict,
    today: DateTime,
    status_dict: dict
        ) -> None:
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

    logger.info('Start finding projs and notify')

    # special notify include those projs / directories in staging52
    # which has been tagged 'no-archive' before but has not been modified
    # for X months. It will be listed under its own column in Slack msg
    # to make it more visible
    special_notify_list = []
    to_be_archived_list = collections.defaultdict(list)
    to_be_archived_dir = []

    # get all old enough projects
    old_enough_projects_dict = get_all_old_enough_projs(MONTH2, MONTH3)

    logger.info(f'No. of old enough projects: {len(old_enough_projects_dict)}')

    # get all directories
    all_directories = get_all_dirs(PROJECT_52)

    logger.info(f'Processing {len(all_directories)} directories in staging52')

    # check if directories have 002 projs made and 002 has not been modified
    # in the last X month
    old_enough_directories = [
        file for file in all_directories if check_dir(file[0], MONTH2)]

    logger.info(
        f'No. of old enough directories: {len(old_enough_directories)}')

    # get proj-id of each projs
    if old_enough_projects_dict:
        logger.info('Processing projects...')

        for proj_id, v in old_enough_projects_dict.items():

            proj_name = v['describe']['name']

            if proj_id in status_dict.keys():
                status = status_dict[proj_id]
            else:
                # get all files' archivalStatus in the proj
                all_files = list(
                    dx.find_data_objects(
                        classname='file',
                        project=proj_id,
                        describe={'fields': {'archivalState': True}}))
                status = set(
                    [x['describe']['archivalState'] for x in all_files])

            if 'live' in status:
                # there is something to be archived
                pass
            else:
                # everything has been archived
                logger.info(f'ALL ARCHIVED: {proj_id}: {proj_name}')
                continue

            # get proj tag status
            tags = [tag.lower() for tag in v['describe']['tags']]
            trimmed_id = proj_id.lstrip('project-')
            created_by = v['describe']['createdBy']['user']

            if 'never-archive' in tags:
                # proj tagged 'never-archive'
                logger.info(f'NEVER ARCHIVE: {proj_id}')
                continue
            elif 'no-archive' in tags:
                if not DEBUG:
                    # proj is old enough + have 'no-archive' tag
                    # thus, we remove the tag and
                    # list it in special_notify list
                    remove_proj_tag(proj_id)

                special_notify_list.append(proj_name)
                archive_pickle['to_be_archived'].append(proj_id)

                if proj_name.startswith('002'):
                    to_be_archived_list['002'].append(
                        f'<{URL_PREFIX}/{trimmed_id}/|{proj_name}>')
                else:
                    to_be_archived_list['003'].append({
                        'user': created_by,
                        'link': f'<{URL_PREFIX}/{trimmed_id}/|{proj_name}>'
                    })
            else:
                # proj tagged 'archive' & others will end up here
                archive_pickle['to_be_archived'].append(proj_id)
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
        logger.info('Processing directories...')

        # for building proj link
        trimmed_proj = PROJECT_52.lstrip('project-')

        for _, original_dir in old_enough_directories:

            trimmed_dir = original_dir.lstrip('/')

            all_files = list(
                dx.find_data_objects(
                    classname='file',
                    project=PROJECT_52,
                    folder=original_dir,
                    describe={'fields': {'archivalState': True}}))

            # get all files' archivalStatus
            status = set([x['describe']['archivalState'] for x in all_files])

            # if there're files in dir with 'live' status
            if 'live' in status:
                # if there's 'never-archive' tag in any file, continue
                never_archive = list(dx.find_data_objects(
                    project=PROJECT_52,
                    folder=original_dir,
                    tags=['never-archive']
                ))

                if never_archive:
                    logger.info(f'NEVER ARCHIVE: {original_dir} in staging52')
                    continue

                # check for 'no-archive' tag in any files
                no_archive = list(dx.find_data_objects(
                    project=PROJECT_52,
                    folder=original_dir,
                    tags=['no-archive'],
                    describe={'fields': {'modified': True}}
                ))

                STAGING_PREFIX = f'{URL_PREFIX}/{trimmed_proj}/data'

                if not no_archive:
                    # there's no 'no-archive' tag or 'never-archive' tag
                    archive_pickle['staging_52'].append(original_dir)
                    to_be_archived_dir.append(
                        f'<{STAGING_PREFIX}/{trimmed_dir}|{original_dir}>')
                else:
                    # if there's 'no-archive' tag
                    # check if all files are active in the last X month
                    # when tagged, modified date will change
                    # if modified date > x month, we know the tag was
                    # probably there for quite a while
                    # if all tagged files have modified date > x month
                    # we remove tags and list dir for archiving
                    if all([
                        older_than(
                            MONTH2,
                            f['describe']['modified']) for f in no_archive]):

                        logger.info(
                            f'REMOVE_TAG: removing tag \
                                for {len(no_archive)} file(s)')

                        if not DEBUG:
                            for file in no_archive:
                                dx.api.file_remove_tags(
                                    file['id'],
                                    input_params={
                                        'tags': ['no-archive'],
                                        'project': PROJECT_52})
                        special_notify_list.append(
                            f'{original_dir} in `staging52`')
                        archive_pickle['staging_52'].append(original_dir)
                        to_be_archived_dir.append(
                            f'<{STAGING_PREFIX}/{trimmed_dir}|{original_dir}>')
                    else:
                        logger.info(f'SKIPPED: {original_dir} in staging52')
                        continue
            else:
                # no 'live' status in dir == all files been archived
                logger.info(f'ALL ARCHIVED: {original_dir} in staging52')
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
                proj003.append('\n')
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
          ('special_notify', special_notify_list),
          ('no_archive', no_archive_list),
          ('never_archive', never_archive_list)
        ]

    next_archiving_date = get_next_archiving_date(today)

    for purpose, data in big_list:
        if data:
            data.append('-- END OF MESSAGE --')

            slack.post_message_to_slack(
                channel='egg-alerts',
                purpose=purpose,
                today=today,
                data=data,
                day=(next_archiving_date, None)
                )
        else:
            continue

    # save dict (only if there's to-be-archived)
    if proj002 or proj003 or folders52:
        logger.info('Writing into pickle file')
        with open(ARCHIVE_PICKLE_PATH, 'wb') as f:
            pickle.dump(archive_pickle, f)

    logger.info('End of finding projs and notify')


def tagging_function() -> dict:
    """
    Function to check latest archivalStatus of files
    in a project and add appropriate tag

    Output:
        status_dict: a dict with project-id (key)
            and (value) achival status of files within the project (set)

    """

    logger.info('Running tagging function')
    status_dict = {}

    projects_dict_002, projects_dict_003 = get_all_projs()
    all_proj = {**projects_dict_002, **projects_dict_003}

    # separate out those with archivedDataUsage == dataUsage
    # which are fully archived so we don't have to query them
    archived_proj = {
        k: v for k, v in all_proj.items() if
        v['describe']['archivedDataUsage'] == v['describe']['dataUsage']}

    for k, v in archived_proj.items():
        proj_name = v['describe']['name']
        tags = [tag.lower() for tag in v['describe']['tags']]

        logger.info(f'ALL ARCHIVED {k} {proj_name}')

        if not DEBUG:

            if 'partial archived' in tags:
                dx.api.project_remove_tags(
                    k, input_params={'tags': [
                        'partial archived', 'fully archived']})
                dx.api.project_add_tags(
                    k, input_params={'tags': ['fully archived']})
            elif 'fully archived' in tags:
                continue
            else:
                dx.api.project_add_tags(
                    k, input_params={'tags': ['fully archived']})

    # whatever is leftover from above projects, we do the query
    # they can be 'live' or 'partially archived'
    unsure_projects = {
        k: v for k, v in all_proj.items() if k not in archived_proj.keys()}

    for k, v in unsure_projects.items():
        proj_name = v['describe']['name']
        tags = [tag.lower() for tag in v['describe']['tags']]

        status = set()
        for item in dx.find_data_objects(
                classname='file',
                project=k,
                describe={'fields': {'archivalState': True}}):
            status.add(item['describe']['archivalState'])

        if 'archived' in status and 'live' in status:
            logger.info(f'PARTIAL ARCHIVED {k} {proj_name} {status}')

            if not DEBUG:
                # check tags and add/remove appropriate tag
                if 'fully archived' in tags:
                    # if 'fully archived' in tags
                    # we do a reset and add 'partial'
                    dx.api.project_remove_tags(
                        k, input_params={
                            'tags': ['partial archived', 'fully archived']})
                    dx.api.project_add_tags(
                        k, input_params={
                            'tags': ['partial archived']})
                elif 'partial archived' in tags:
                    # if 'fully' not in tags, if 'partial' is present
                    # this proj is correctly tagged. We continue.
                    continue
                else:
                    # both 'fully' and 'partial' are not present
                    dx.api.project_add_tags(
                        k, input_params={'tags': ['partial archived']})

            # save this project file status into a dictionary for later use
            status_dict[k] = status
        elif 'live' not in status:
            logger.info(f'ALL ARCHIVED {k} {proj_name} {status}')

            if not DEBUG:
                if 'partial archived' in tags:
                    dx.api.project_remove_tags(
                        k, input_params={'tags': [
                            'partial archived', 'fully archived']})
                    dx.api.project_add_tags(
                        k, input_params={'tags': ['fully archived']})
                elif 'fully archived' in tags:
                    continue
                else:
                    dx.api.project_add_tags(
                        k, input_params={'tags': ['fully archived']})

            status_dict[k] = status
        else:
            # if all live files, don't touch the project
            logger.info(f'ALL LIVE {k} {proj_name} {status}')
            continue

    return status_dict


def archiving_function(archive_pickle: dict, today: DateTime) -> None:
    """
    Function to check previously listed projs and dirs (memory)
    and do the archiving, then run find_proj_and_notify function.

    Skip projs or directories (staging52) if:
    1. tagged 'no-archive'
    2. tagged 'never-archive'
    3. modified in the past ARCHIVE_MODIFIED_MONTH month

    Inputs:
        archive_pickle: memory to get files previously flagged
            for archiving (dict)
        today: to record today's date (datetime)

    """

    logger.info('Start archiving function')

    list_of_projs = archive_pickle['to_be_archived']
    list_of_dirs_52 = archive_pickle['staging_52']

    # just for recording what has been archived
    # plus for Slack notification
    temp_archived = collections.defaultdict(list)

    # do the archiving
    if list_of_projs:
        for proj_id in list_of_projs:
            project = dx.DXProject(proj_id)
            proj_desc = project.describe()
            proj_name = proj_desc['name']
            modified_epoch = proj_desc['modified']

            if 'never-archive' in proj_desc['tags']:
                logger.info(f'NEVER_ARCHIVE: {proj_name}')
                continue
            elif 'no-archive' in proj_desc['tags']:
                logger.info(f'SKIPPED: {proj_name}')
                continue
            elif 'archive' in proj_desc['tags']:
                big_exclude_list = set()

                for word in AUTOMATED_REGEX_EXCLUDE:
                    exclude_list = [
                        file['id'] for file in list(
                            dx.find_data_objects(
                                name=word,
                                name_mode='regexp',
                                project=proj_id
                                ))]
                    big_exclude_list.update(exclude_list)

                if big_exclude_list:
                    all_files = [
                        file['id'] for file in list(
                            dx.find_data_objects(
                                project=proj_id))]
                    excluded_list = [
                        file_id for file_id in all_files if
                        file_id not in big_exclude_list]

                    logger.info(f'ARCHIVING EXCLUDE: {proj_id}')
                    if not DEBUG:
                        for file_id in excluded_list:
                            logger.info(f'ARCHIVING: {file_id}')
                            dx.DXFile(file_id, project=proj_id).archive()
                        temp_archived['archived'].append(
                            f'{proj_name} ({proj_id})')
                else:
                    logger.info(f'ARCHIVING {proj_id}')
                    if not DEBUG:
                        res = dx.api.project_archive(proj_id)
                        if res['count'] != 0:
                            temp_archived['archived'].append(
                                f'{proj_name} ({proj_id})')
            else:
                if older_than(ARCHIVE_MODIFIED_MONTH, modified_epoch):
                    # True if not modified in the last X month

                    big_exclude_list = set()

                    # loop through each word regex in ENV
                    # find_data_object based on the word regex
                    # compile that into a big_exclude_list

                    for word in AUTOMATED_REGEX_EXCLUDE:
                        exclude_list = [
                            file['id'] for file in list(
                                dx.find_data_objects(
                                    name=word,
                                    name_mode='regexp',
                                    project=proj_id
                                    ))]
                        big_exclude_list.update(exclude_list)

                    # if there're files fitting the exclude regex
                    # we get all_files, exclude those in big_exclude_list
                    # then archive each file
                    # if no files fitting exclude regex
                    # we archive the whole project as usual

                    if big_exclude_list:
                        all_files = [
                            file['id'] for file in list(
                                dx.find_data_objects(
                                    project=proj_id))]
                        excluded_list = [
                            file_id for file_id in all_files if
                            file_id not in big_exclude_list]

                        logger.info(f'ARCHIVING EXCLUDE: {proj_id}')
                        if not DEBUG:
                            for file_id in excluded_list:
                                logger.info(f'ARCHIVING: {file_id}')
                                dx.DXFile(file_id, project=proj_id).archive()
                            temp_archived['archived'].append(
                                f'{proj_name} ({proj_id})')
                    else:
                        logger.info(f'ARCHIVING {proj_id}')
                        if not DEBUG:
                            res = dx.api.project_archive(proj_id)
                            if res['count'] != 0:
                                temp_archived['archived'].append(
                                    f'{proj_name} (`{proj_id}`)')
                else:
                    # end up here if proj is not older than
                    # ARCHIVE_MODIFIED_MONTH, meaning
                    # proj has been modified recently, so we skip
                    logger.info(f'RECENTLY MODIFIED & SKIPPED: {proj_name}')
                    continue

    if list_of_dirs_52:
        for dir in list_of_dirs_52:
            directory_archive(dir, PROJECT_52, temp_archived)

    # generate archiving txt file
    # ONLY IF THERE ARE FILEs BEING ARCHIVED
    if temp_archived:
        if os.path.isfile(ARCHIVED_TXT_PATH):
            with open(ARCHIVED_TXT_PATH, 'a') as f:
                f.write('\n' + f'=== {today} ===')

                for line in temp_archived['archived']:
                    f.write('\n' + line)
        else:
            with open(ARCHIVED_TXT_PATH, 'w') as f:
                f.write('\n' + f'=== {today} ===')
                f.write('\n'.join(temp_archived['archived']))

        # also send a notification to say what have been archived
        slack.post_message_to_slack(
            channel='egg-alerts',
            purpose='archived',
            today=today,
            data=temp_archived['archived']
            )

    # do tagging for fully and partially archived projects
    status_dict = tagging_function()

    # empty pickle (memory)
    logger.info('Clearing pickle file')
    archive_pickle['to_be_archived'] = []
    archive_pickle['staging_52'] = []

    # save memory dict
    logger.info('Writing into pickle file')
    with open(ARCHIVE_PICKLE_PATH, 'wb') as f:
        pickle.dump(archive_pickle, f)

    logger.info('End of archiving function')

    find_projs_and_notify(archive_pickle, today, status_dict)


def get_next_archiving_date(today: DateTime) -> DateTime:
    """
    Function to get the next automated-archive run date

    Input:
        today (datetime)

    Return (datetime):
        If today.day is between 1-15: return 15th of this month
        If today.day is after 15: return 1st day of next month

    """

    if today.day not in [1, 15]:
        pass
    else:
        today += dt.timedelta(1)

    while today.day not in [1, 15]:
        today += dt.timedelta(1)

    return today


def make_datetime_format(modified_epoch: str) -> DateTime:
    """
    Function to turn modified epoch (returned by DNANexus)
    into readable datetime format

    Input:
        epoch modified datetime (from dnanexus describe)

    Return:
        datetime

    """

    modified = modified_epoch / 1000.0
    modified_dt = dt.datetime.fromtimestamp(modified)

    return modified_dt


def get_old_tar_and_notify(today) -> None:
    """
    Function to get tar which are not modified in the last 3 months
    Regex Format:
        only returns "run.....tar.gz" in staging52

    Return:
        None

    """
    logger.info('Getting old tar.gz in staging52')

    result = list(
        dx.find_data_objects(
            name='^run.*.tar.gz',
            name_mode='regexp',
            describe={
                'fields': {
                    'modified': True,
                    'folder': True,
                    'name': True}},
            project=PROJECT_52))

    # list of tar files not modified in the last 3 months
    filtered_result = [
        x for x in result if older_than(TAR_MONTH, x['describe']['modified'])]

    id_results = [
        (
            x['id'],
            x['describe']['folder'],
            x['describe']['name']) for x in filtered_result]

    dates = [make_datetime_format(
        d['describe']['modified']) for d in filtered_result]

    # earliest date among the list of tars
    min_date = min(dates).strftime('%Y-%m-%d')
    # latest date among the list of tars
    max_date = max(dates).strftime('%Y-%m-%d')

    slack.post_message_to_slack(
        channel='egg-alerts',
        purpose='tar_notify',
        today=today,
        data=id_results,
        day=(min_date, max_date)
    )


if __name__ == "__main__":

    archive_pickle = read_or_new_pickle(ARCHIVE_PICKLE_PATH)
    to_be_archived = archive_pickle['to_be_archived']
    staging52 = archive_pickle['staging_52']

    today = dt.date.today()
    logger.info(today)

    if today.day in [1, 15]:
        dx_login(today)

        if today.day == 1:
            get_old_tar_and_notify(today)

        # if there is something in memory
        # we run archive function
        # else we find_and_notify
        if to_be_archived or staging52:
            archiving_function(archive_pickle, today)
        else:
            find_projs_and_notify(archive_pickle, today, {})

    else:
        if to_be_archived or staging52:
            # if there's to-be-archived in memory
            # we do the countdown to egg-alerts
            # else we just keep silence

            next_archiving_date = get_next_archiving_date(today)
            diff = next_archiving_date - today

            slack.post_message_to_slack(
                channel='egg-alerts',
                purpose='countdown',
                today=today,
                day=(diff.days, next_archiving_date),
                )
        else:
            logger.info('No data in memory')

    logger.info('End of script.')
