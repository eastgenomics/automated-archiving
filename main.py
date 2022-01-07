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
    (1) send alert msg when dxpy token failed
    (2) send to-be-archived notification

    Inputs:
        channel: e.g. egg-alert, egg-log
        index: index for which proj in lists (below)
        data: list of projs / dirs to be archived
        error: (for dxpy token) error msg
        alert: (for dxpy token)

    Returns:
        dict: slack api response
    """

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
            try:
                error = error.error_message()
            except Exception as _:
                error = error

            error_msg = (
                "automated-archiving: Error with dxpy token! Error code: \n"
                f"`{error}`"
                )
            response = requests.post(
                'https://slack.com/api/chat.postMessage', {
                    'token': os.environ['SLACK_TOKEN'],
                    'channel': f'#{channel}',
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
            response = requests.post(
                'https://slack.com/api/chat.postMessage', {
                    'token': os.environ['SLACK_TOKEN'],
                    'channel': f'#{channel}',
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

    try:
        with open(path, 'rb') as f:
            b = pickle.load(f)
    except Exception as e:
        d = collections.defaultdict(list)

        with open(path, 'wb') as f:
            pickle.dump(d, f)

        with open(path, 'rb') as f:
            b = pickle.load(f)

    return b


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


def find_files(dir, proj, month):
    """
    Function for finding files within a folder directory
    which are not modified in the last X period (-120d)
    especially for staging52/53 project on DNANexus

    Inputs:
        directory, project-id, X month

    Returns:
        Boolean: True if there are no files which
        are modified in the last X period
    """

    days = str(30 * month)

    result = dx.find_data_objects(
        project=proj,
        folder=dir,
        modified_after=f'-{days}d'
    )
    if list(result):
        return False
    return True


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
        log.error(e)

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

    Input:
        project-id

    Returns:
        project-id of tag-removed project
    """

    r = dx.api.project_remove_tags(
        proj, input_params={'tags': ['no-archive']})
    return r['id']


def code_a():

    log.info('Running Code A')

    dx_login()

    archive_pickle = read_or_new_pickle(ARCHIVE_PICKLE_PATH)
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

    old_enough_projects_dict = {
        k: v for k, v in projects_dict.items() if older_than(
            MONTH, v['describe']['modified'])}

    excluded_list = [PROJECT_52, PROJECT_53]

    # exclude projs (staging52/53) and archived projs
    old_enough_projects_dict = {
        k: v for k, v in old_enough_projects_dict.items() if k not in excluded_list and k not in archive_pickle['archived']
    }

    log.info(f'No. of old enough projects: {len(old_enough_projects_dict)}')

    staging52 = dx.DXProject(PROJECT_52)
    staging53 = dx.DXProject(PROJECT_53)

    # check for folders in staging52
    dir_in_52 = [
        f for f in staging52.list_folder()['folders'] if f != '/processed']
    dir_in_52_processed = staging52.list_folder('/processed')['folders']

    # check for folders in staging53
    excluded_53 = ['/MVZ_upload', '/Reports', '/dx_describe']
    dir_in_53 = [
        f for f in staging53.list_folder()['folders'] if f not in excluded_53]

    all_52_dir = dir_in_52 + dir_in_52_processed

    # remove those which have been archived
    filtered_52 = [dir for dir in all_52_dir if dir not in archive_pickle['archived_52']]
    filtered_53 = [dir for dir in dir_in_53 if dir not in archive_pickle['archived_53']]

    # find dirs that are not modified in last X period (inactive folders)
    log.info(f'Processing staging52: {len(filtered_52)} folders detected')
    old_enough_52 = [
        f for f in filtered_52 if find_files(f, PROJECT_52, MONTH)
    ]

    log.info(f'No. of old enough staging52 folders: {len(old_enough_52)}')

    log.info(f'Processing staging53: {len(filtered_53)} folders detected')
    old_enough_53 = [
        f for f in filtered_53 if find_files(f, PROJECT_53, MONTH)
    ]
    log.info(f'No. of old enough staging53 folders: {len(old_enough_53)}')

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

    if old_enough_52:
        log.info('Saving dirs to pickle: staging52')

        for dir in old_enough_52:
            if dir in archive_pickle['skipped_52']:

                log.info(f'REMOVE_TAG: {dir} in skipped_52')
                res = list(dx.find_data_objects(
                    project=PROJECT_52,
                    folder=dir,
                    tags=['no-archive']
                ))

                log.info(f'REMOVE_TAG: removing tag for {len(res)} files')
                for file in res:
                    dx.api.file_remove_tags(
                        file['id'],
                        input_params={
                            'tags': ['no-archive'],
                            'project': PROJECT_52})

                special_notify.append(f'{dir} in staging52')
                archive_pickle['skipped_52'].remove(dir)

            archive_pickle['staging_52'].append(dir)

    if old_enough_53:
        log.info('Saving dirs to pickle: staging53')

        for dir in old_enough_53:
            if dir in archive_pickle['skipped_53']:

                log.info(f'REMOVE_TAG: {dir} in skipped_53')
                res = list(dx.find_data_objects(
                    project=PROJECT_53,
                    folder=dir,
                    tags=['no-archive']
                ))

                log.info(f'REMOVE_TAG: removing tag for {len(res)} files')
                for file in res:
                    dx.api.file_remove_tags(
                        file['id'],
                        input_params={
                            'tags': ['no-archive'],
                            'project': PROJECT_53})
                special_notify.append(f'{dir} in staging53')
                archive_pickle['skipped_52'].remove(dir)

            archive_pickle['staging_53'].append(dir)

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


def code_b():

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
            folders = list(dx.find_data_objects(
                project=PROJECT_52,
                folder=dir,
                describe=True))

            # check if any files within the folder dir
            # has been tagged with 'no-archive'
            tag_check = list(
                [r for r in folders if 'no-archive' in r['describe']['tags']])

            if len(tag_check) > 0:
                log.info(f'Skipped {dir} in staging52')
                archive_pickle['skipped_52'].append(dir)
                continue
            else:
                log.info(f'archiving staging52: {dir}')
                # dx.api.project_archive(
                #     PROJECT_52, input_params={'folder': dir})
                archive_pickle['archived_52'].append(dir)
                temp_archived['archived'].append(f'{PROJECT_52}:{dir}')

    if list_of_dirs_53:
        for dir in list_of_dirs_53:
            folders = list(dx.find_data_objects(
                project=PROJECT_53,
                folder=dir,
                describe=True
                ))

            tag_check = list([r for r in folders if 'no-archive' in r['describe']['tags']])

            if len(tag_check) > 0:
                log.info(f'Skipped {dir} in staging53')
                archive_pickle['skipped_53'].append(dir)
                continue
            else:
                log.info(f'archiving staging53: {dir}')
                # dx.api.project_archive(
                #     PROJECT_53, input_params={'folder': dir})
                archive_pickle['archived_53'].append(dir)
                temp_archived['archived'].append(f'{PROJECT_53}:{dir}')

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

    code_a()


def main():

    archive_pickle = read_or_new_pickle(ARCHIVE_PICKLE_PATH)
    to_be_archived = archive_pickle['to_be_archived']
    staging52 = archive_pickle['staging_52']
    staging53 = archive_pickle['staging_53']

    if to_be_archived or staging52 or staging53:
        code_b()
    else:
        code_a()

    log.info('End of script.')


if __name__ == "__main__":
    main()
