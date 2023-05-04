import os
import pickle
import collections
import datetime as dt
from dateutil.relativedelta import relativedelta
from typing import Union

import dxpy as dx

from bin.helper import get_logger
from bin.slack import SlackClass

logger = get_logger(__name__)


def read_or_new_pickle(path: str) -> dict:
    """
    Read stored pickle memory for the script
    Using defaultdict() automatically create new dict.key()

    Parameters:
    :param: path: directory path to pickle

    Returns:
        `dict`: collection.defaultdict(list)
    """
    logger.info(f"Reading pickle at: {path}")
    if os.path.isfile(path):
        with open(path, "rb") as f:
            pickle_dict = pickle.load(f)
    else:
        pickle_dict = collections.defaultdict(list)
        with open(path, "wb") as f:
            pickle.dump(pickle_dict, f)

    return pickle_dict


def older_than(month: int, modified_epoch: int) -> bool:
    """
    Determine if a modified epoch date is older than X month

    Parameters:
    :param: month: `int` N month to check against
    :param: modified_epoch: `int` project modified datetime epoch

    Returns (Boolean):
        - `True` if haven't been modified in last X month
        - `False` if have been modified in last X month
    """

    modified = modified_epoch / 1000.0
    date = dt.datetime.fromtimestamp(modified)

    return date + relativedelta(months=+month) < dt.datetime.today()


def check_directory(directory: str, month: int) -> bool:
    """
    Function to check if project (002/003) for that directory
    exist. e.g. For 210407_A01295_0010_AHWL5GDRXX
    it will find 002_210407_A01295_0010_AHWL5GDRXX project

    If the 002/003 exist, we check if the proj has been inactive
    for the last X month. If yes, return True.

    Inputs:
    :param: directory: trimmed directory pathway
    :param: month: number of month inactive

    Returns:
        - `True` if its 002 has not been active for X month
        - `False` if no 002 project
        - `False` if 002 project has been active in the past X month
    """

    result = list(
        dx.find_projects(
            directory,
            name_mode="regexp",
            describe={"fields": {"modified": True}},
            limit=1,
        )
    )

    # if no 002/003 project
    if not result:
        return False

    modified_epoch = result[0]["describe"]["modified"]

    # check modified date of the 002/003 proj
    if older_than(month, modified_epoch):
        return True
    else:
        return False


def dx_login(today: dt.datetime, token: str, slack: SlackClass) -> None:
    """
    DNANexus login check function.
    If fail, send Slack notification

    Parameters:
    :param: today: date for Slack notification purpose
    :param: token: dnanexus auth token
    :param: slack: SlackClass for notification purpose
    """

    DX_SECURITY_CONTEXT = {"auth_token_type": "Bearer", "auth_token": token}

    dx.set_security_context(DX_SECURITY_CONTEXT)

    try:
        dx.api.system_whoami()
        logger.info("DNANexus login successful")

    except dx.exceptions.InvalidAuthentication as e:
        error_message = e.error_message()
        logger.error(error_message)

        slack.post_message_to_slack(
            "#egg-alerts",
            "alert",
            today,
            dnanexus_error=error_message,
        )

        raise ValueError("Error with DNAnexus login")


def remove_project_tag(project_id: str) -> None:
    """
    Function to remove tag 'no-archive' for project-id

    When a project has been tagged 'no-archive' but
    has not been modified for X months
    the tag will be removed and the project will be
    notified to be archived

    This only applies to project-id
    Not directories in Staging52

    Parameters:
    :param: project-id
    """

    logger.info(f"REMOVE TAG: {project_id}")
    dx.api.project_remove_tags(project_id, input_params={"tags": ["no-archive"]})


def get_all_projects_in_dnanexus() -> Union[dict, dict]:
    """
    Get all 002 and 003 projects

    Returns:
        2 dict for 002 and 003 projs
    """
    # Get all 002 and 003 projects
    projects_dict_002 = dict()
    projects_dict_003 = dict()

    projects002 = list(
        dx.search.find_projects(
            name="^002.*",
            name_mode="regexp",
            billed_to="org-emee_1",
            describe={
                "fields": {
                    "name": True,
                    "tags": True,
                    "dataUsage": True,
                    "archivedDataUsage": True,
                    "modified": True,
                    "createdBy": True,
                }
            },
        )
    )

    projects003 = list(
        dx.search.find_projects(
            name="^003.*",
            name_mode="regexp",
            billed_to="org-emee_1",
            describe={
                "fields": {
                    "name": True,
                    "tags": True,
                    "dataUsage": True,
                    "archivedDataUsage": True,
                    "modified": True,
                    "createdBy": True,
                }
            },
        )
    )

    # put all projects into a dict
    projects_dict_002.update({proj["id"]: proj for proj in projects002})
    projects_dict_003.update({proj["id"]: proj for proj in projects003})

    return projects_dict_002, projects_dict_003


def get_old_enough_projects(
    month2: int, month3: int, project52: str, project53: str
) -> dict:
    """
    Function to get all 002 and 003 projects which are not modified
    in the last X months.

    Exclude projects: staging 52 as that will be processed separately and
        exclude projects which had been archived.

    Parameters:
    :param: month2: duration of inactivity in the last x month for 002 (int)
    :param: month3: duration of inactivity in the last x month for 003 (int)
    :param: project52: staging-52 project-id
    :param: project53: staging-53 project-id

    Return:
    dict of key (proj-id) : value (describe return from dxpy)

    """

    # Get all 002 and 003 projects
    projects_dict_002, projects_dict_003 = get_all_projects_in_dnanexus()

    # sieve the dict to include only old-enough projs
    # and if dataUsage != archivedDataUsage
    # if dataUsage == archivedDataUsage, it means
    # all data within the proj have been archived
    # if proj or certain files within was unarchived
    # the dataUsage should != archivedDataUsage
    old_enough_projects_dict_002 = {
        k: v
        for k, v in projects_dict_002.items()
        if older_than(month2, v["describe"]["modified"])
        and v["describe"]["dataUsage"] != v["describe"]["archivedDataUsage"]
    }
    old_enough_projects_dict_003 = {
        k: v
        for k, v in projects_dict_003.items()
        if older_than(month3, v["describe"]["modified"])
        and v["describe"]["dataUsage"] != v["describe"]["archivedDataUsage"]
    }

    old_enough_projects_dict = {
        **old_enough_projects_dict_002,
        **old_enough_projects_dict_003,
    }

    # exclude staging52 and staging53
    old_enough_projects_dict = {
        k: v
        for k, v in old_enough_projects_dict.items()
        if k not in [project52, project53]
    }

    # get proj tagged archive
    projects_tagged_with_archive = list(
        dx.search.find_projects(
            name="^00[2,3].*",
            name_mode="regexp",
            tags=["archive"],
            describe={
                "fields": {
                    "name": True,
                    "tags": True,
                    "dataUsage": True,
                    "archivedDataUsage": True,
                    "modified": True,
                    "createdBy": True,
                }
            },
        )
    )

    old_enough_projects_dict.update(
        {proj["id"]: proj for proj in projects_tagged_with_archive}
    )

    return old_enough_projects_dict


def get_all_directories(project_id: str) -> list:
    """
    Function to get all directories in a project-id

    Parameters:
    :param: project_id: DNAnexus project-id

    Return:
    list of tuple for ease of processing later on
    tuple contains:
        - trimmed directory name (e.g. 210407_A01295_0010_AHWL5GDRXX)
            for 002 querying later on
        - original directory path (e.g. /210407_A01295_0010_AHWL5GDRXX/)
    """

    dx_project = dx.DXProject(project_id)

    # get all directories in root
    all_directories_in_project = dx_project.list_folder(only="folders")["folders"]

    # filter out the /processed folder in root of staging-52
    directories_in_52 = [
        (file.lstrip("/").lstrip("/processed"), file)
        for file in all_directories_in_project
        if file != "/processed"
    ]

    # get all directories in /processed
    directories_in_52_processed = [
        (file.lstrip("/").lstrip("/processed"), file)
        for file in dx_project.list_folder("/processed", only="folders")["folders"]
    ]

    return directories_in_52 + directories_in_52_processed


def directory_archive(
    dir_path: str,
    proj_id: str,
    temp_dict: dict,
    archived_modified_month: int,
    regex_excludes: list,
    failed_archive: list,
    debug: bool,
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
        archived_modified_month: env variable ARCHIVE_MODIFIED_MONTH
        regex_excludes: list of regex match to exclude
        failed_archive: list to record which file-id failed to be archived
        debug: env ARCHIVE_DEBUG

    Returns:
        None
    """

    # check for 'never-archive' tag in directory
    never_archive = list(
        dx.find_data_objects(project=proj_id, folder=dir_path, tags=["never-archive"])
    )

    if never_archive:
        logger.info(f"NEVER ARCHIVE: {dir_path} in staging52")
        return

    # 2 * 4 week = 8 weeks
    num_weeks = archived_modified_month * 4

    # check if there's any files modified in the last num_weeks
    recent_modified = list(
        dx.find_data_objects(
            project=proj_id, folder=dir_path, modified_after=f"-{num_weeks}w"
        )
    )

    if recent_modified:
        logger.info(f"RECENTLY MODIFIED: {dir_path} in staging52")
        return

    # check for 'no-archive' tag in directory
    no_archive = list(
        dx.find_data_objects(project=proj_id, folder=dir_path, tags=["no-archive"])
    )

    if no_archive:
        logger.info(f"NO ARCHIVE: {dir_path} in staging52")
        return
    else:
        # if directory in staging52 got
        # no tag indicating dont archive
        # it will end up here
        file_ids_to_exclude = set()

        # get all file-id that match exclude regex
        for word in regex_excludes:
            file_ids_to_exclude.update(
                [
                    file["id"]
                    for file in list(
                        dx.find_data_objects(
                            name=word,
                            name_mode="regexp",
                            project=proj_id,
                            folder=dir_path,
                        )
                    )
                ]
            )

        if file_ids_to_exclude:
            # find all files in directory
            # exclude those file-id that match those in exclude list
            # run archive on each of those file
            file_ids = [
                file["id"]
                for file in list(dx.find_data_objects(project=proj_id, folder=dir_path))
            ]

            archived_file_count = 0
            if not debug:  # if running in production
                for file_id in file_ids:
                    if file_id in file_ids_to_exclude:
                        continue
                    try:
                        dx.DXFile(file_id, project=proj_id).archive()
                        archived_file_count += 1
                    except Exception as error:
                        logger.error(error)
                        failed_archive.append(f"{proj_id}:{file_id}")

                if archived_file_count > 0:
                    temp_dict["archived"].append(
                        f"{proj_id}:{dir_path}: {archived_file_count}"
                    )
        else:
            # no file-id match exclude regex
            # we do an overall dx.Project.archive
            if not debug:  # running in production
                try:
                    res = dx.api.project_archive(
                        proj_id, input_params={"folder": dir_path}
                    )
                    if res["count"] != 0:
                        temp_dict["archived"].append(
                            f"{proj_id}:{dir_path}:{res['count']}"
                        )
                except Exception:
                    logger.info(
                        f"Archiving {proj_id}:{dir_path} file by file because"
                        " dx.Project.archive failed"
                    )

                    file_ids = [
                        file["id"]
                        for file in list(
                            dx.find_data_objects(
                                project=proj_id,
                                classname="file",
                                folder=dir_path,
                            )
                        )
                    ]

                    archived_file_count = 0
                    for file_id in file_ids:
                        try:
                            dx.DXFile(file_id, project=proj_id).archive()
                            archived_file_count += 1
                        except Exception as er:
                            logger.error(er)
                            failed_archive.append(f"{proj_id}:{file_id}")

                    if archived_file_count > 0:
                        temp_dict["archived"].append(
                            f"{proj_id}:{dir_path}:{archived_file_count}"
                        )


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
            tags=["no-archive"],
            describe={"fields": {"folder": True}},
        )
    )
    directory_never_archive = list(
        dx.find_data_objects(
            project=proj_52,
            tags=["never-archive"],
            describe={"fields": {"folder": True}},
        )
    )

    # get the directory pathway
    directory_no_archive = [
        t["describe"]["folder"].lstrip("/") for t in directory_no_archive
    ]
    directory_never_archive = [
        t["describe"]["folder"].lstrip("/") for t in directory_never_archive
    ]

    # clean the directory path and append to list
    for directory in directory_no_archive:
        temp = directory.split("/")
        if "processed" in directory:
            no_archive_list.append(f"/{temp[0]}/{temp[1]} in `staging52`")
        else:
            no_archive_list.append(f"{temp[0]} in `staging52`")

    for directory in directory_never_archive:
        temp = directory.split("/")
        if "processed" in directory:
            never_archive_list.append(f"/{temp[0]}/{temp[1]} in `staging52`")
        else:
            never_archive_list.append(f"{temp[0]} in `staging52`")

    # check no-archive & never-archive in projects
    # Get all 002 and 003 projects
    projects_dict_002, projects_dict_003 = get_all_projects_in_dnanexus()

    # get proj tagged with no-archive or never-archive for notify later
    agg_dict = {**projects_dict_002, **projects_dict_003}

    proj_no_archive = [
        proj["describe"]["name"]
        for proj in agg_dict.values()
        if "no-archive" in proj["describe"]["tags"]
    ]
    proj_never_archive = [
        proj["describe"]["name"]
        for proj in agg_dict.values()
        if "never-archive" in proj["describe"]["tags"]
    ]

    no_archive_list += proj_no_archive
    never_archive_list += proj_never_archive

    return no_archive_list, never_archive_list


def find_projs_and_notify(
    archive_pickle: collections.defaultdict(list),
    today: dt.datetime,
    status_dict: dict,
    month2: int,
    month3: int,
    debug: bool,
    members: dict,
    archive_pickle_path: str,
    slack: SlackClass,
    url_prefix: str = "https://platform.dnanexus.com/panx/projects",
    project_52: str = "project-FpVG0G84X7kzq58g19vF1YJQ",
    project_53: str = "project-FvbzbX84gG9Z3968BJjxYZ1k",
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

    logger.info("Start finding projs and notify")

    # special notify include those projs / directories in staging52
    # which has been tagged 'no-archive' before but has not been modified
    # for X months. It will be listed under its own column in Slack msg
    # to make it more visible
    special_notify_list = []
    to_be_archived_list = collections.defaultdict(list)
    to_be_archived_dir = []

    # get all old enough projects
    old_enough_projects_dict = get_old_enough_projects(
        month2, month3, project_52, project_53
    )

    logger.info(f"No. of old enough projects: {len(old_enough_projects_dict)}")

    # get all directories in staging-52
    all_directories = get_all_directories(project_52)

    logger.info(f"Processing {len(all_directories)} directories in staging52")

    # check if directories have 002 projs made and 002 has not been modified
    # in the last X month
    old_enough_directories = [
        (trimmed_directory, original_directory)
        for trimmed_directory, original_directory in all_directories
        if check_directory(trimmed_directory, month2)
    ]

    logger.info(f"No. of old enough directories: {len(old_enough_directories)}")

    if old_enough_projects_dict:
        logger.info("Processing project")

        for proj_id, v in old_enough_projects_dict.items():
            proj_name = v["describe"]["name"]
            tags = [tag.lower() for tag in v["describe"]["tags"]]
            trimmed_id = proj_id.lstrip("project-")
            created_by = v["describe"]["createdBy"]["user"]

            if "never-archive" in tags:
                continue

            if proj_id in status_dict.keys():
                status = status_dict[proj_id]
            else:
                # get all files' archivalStatus in the project
                all_files = list(
                    dx.find_data_objects(
                        classname="file",
                        project=proj_id,
                        describe={"fields": {"archivalState": True}},
                    )
                )
                status = set([x["describe"]["archivalState"] for x in all_files])

            if "live" in status:
                # there is something to be archived
                pass
            else:
                # everything has been archived
                continue

            if "no-archive" in tags:
                if not debug:
                    # project is old enough + have 'no-archive' tag
                    # thus, we remove the tag and
                    # list it in special-notify list
                    remove_project_tag(proj_id)

                special_notify_list.append(proj_name)

            # add project-id to to-be-archived list in memory
            archive_pickle["to_be_archived"].append(proj_id)

            if proj_name.startswith("002"):
                to_be_archived_list["002"].append(
                    f"<{url_prefix}/{trimmed_id}/|{proj_name}>"
                )
            else:
                to_be_archived_list["003"].append(
                    {
                        "user": created_by,
                        "link": f"<{url_prefix}/{trimmed_id}/|{proj_name}>",
                    }
                )

    # sieve through each directory in staging52
    if old_enough_directories:
        logger.info("Processing directories")

        # for building proj link
        trimmed_proj = project_52.lstrip("project-")

        for _, original_dir in old_enough_directories:
            trimmed_dir = original_dir.lstrip("/")

            # get all the files within that directory in staging-52
            all_files = list(
                dx.find_data_objects(
                    classname="file",
                    project=project_52,
                    folder=original_dir,
                    describe={"fields": {"archivalState": True}},
                )
            )

            # get all files' archivalStatus
            status = set([x["describe"]["archivalState"] for x in all_files])

            # if there're files in dir with 'live' status
            if "live" in status:
                # if there's 'never-archive' tag in any file, continue
                never_archive = list(
                    dx.find_data_objects(
                        project=project_52,
                        folder=original_dir,
                        tags=["never-archive"],
                        limit=1,
                    )
                )

                if never_archive:
                    continue

                # check for 'no-archive' tag in any files
                no_archive = list(
                    dx.find_data_objects(
                        project=project_52,
                        folder=original_dir,
                        tags=["no-archive"],
                        describe={"fields": {"modified": True}},
                    )
                )

                STAGING_PREFIX = f"{url_prefix}/{trimmed_proj}/data"

                if not no_archive:
                    # there's no 'no-archive' tag or 'never-archive' tag
                    archive_pickle["staging_52"].append(original_dir)
                    to_be_archived_dir.append(
                        f"<{STAGING_PREFIX}/{trimmed_dir}|{original_dir}>"
                    )
                else:
                    # if there's 'no-archive' tag
                    # check if all files are active in the last X month
                    # when tagged, modified date will change
                    # if modified date > x month, we know the tag was
                    # probably there for quite a while
                    # if all tagged files have modified date > x month
                    # we remove tags and list dir for archiving
                    if all(
                        [
                            older_than(month2, f["describe"]["modified"])
                            for f in no_archive
                        ]
                    ):
                        logger.info(
                            "REMOVE TAG: removing tag                        "
                            f"         for {len(no_archive)} file(s)"
                        )

                        if not debug:
                            for file in no_archive:
                                dx.api.file_remove_tags(
                                    file["id"],
                                    input_params={
                                        "tags": ["no-archive"],
                                        "project": project_52,
                                    },
                                )
                        special_notify_list.append(f"{original_dir} in `staging52`")
                        archive_pickle["staging_52"].append(original_dir)
                        to_be_archived_dir.append(
                            f"<{STAGING_PREFIX}/{trimmed_dir}|{original_dir}>"
                        )
                    else:
                        logger.info(f"SKIPPED: {original_dir} in staging52")
                        continue
            else:
                # no 'live' status means all files
                # in the directory have been archived thus we continue
                continue

    no_archive_list, never_archive_list = get_tag_status(project_52)

    # get everything ready for slack notification
    proj002 = sorted(to_be_archived_list["002"])
    proj003 = []
    folders52 = sorted(to_be_archived_dir)
    no_archive_list = sorted(no_archive_list)
    never_archive_list = sorted(never_archive_list)

    # process 003 list to sort by user
    temp003 = to_be_archived_list["003"]
    if temp003:
        temp003 = sorted(temp003, key=lambda d: d["user"])
        current_usr = None
        for link in temp003:
            if current_usr != link["user"]:
                proj003.append("\n")
                current_usr = link["user"]

                if current_usr in members.keys():
                    proj003.append(f"<@{members[current_usr]}>")
                else:
                    proj003.append(f"Can't find ID for: {current_usr}")

            proj003.append(link["link"])

    big_list = [
        ("002", proj002),
        ("003", proj003),
        ("staging52", folders52),
        ("special-notify", special_notify_list),
        ("no-archive", no_archive_list),
        ("never-archive", never_archive_list),
    ]

    next_archiving_date = get_next_archiving_date(today)

    for purpose, data in big_list:
        if data:
            data.append("-- END OF MESSAGE --")

            slack.post_message_to_slack(
                "#egg-alerts",
                purpose,
                today,
                data=data,
                archiving_date=next_archiving_date,
            )

    # save dict (only if there's to-be-archived)
    if proj002 or proj003 or folders52:
        logger.info(f"Writing into pickle file at {archive_pickle_path}")
        with open(archive_pickle_path, "wb") as f:
            pickle.dump(archive_pickle, f)

    logger.info("End of finding projs and notify")


def tagging_function(debug: bool) -> None:
    """
    Function to check latest archivalStatus of files
    in a project and add appropriate tag

    Parameters:
    :param: debug: `bool` whether the script is ran in DEBUG mode
    """

    logger.info("Running tagging function")

    projects_dict_002, projects_dict_003 = get_all_projects_in_dnanexus()
    all_proj = {**projects_dict_002, **projects_dict_003}

    # separate out those with archivedDataUsage == dataUsage
    # which are fully archived so we don't have to query them
    archived_projects = {
        k: v
        for k, v in all_proj.items()
        if v["describe"]["archivedDataUsage"] == v["describe"]["dataUsage"]
    }

    for k, v in archived_projects.items():
        tags = [tag.lower() for tag in v["describe"]["tags"]]

        if not debug:  # if running in production
            if "partial archived" in tags:
                dx.api.project_remove_tags(
                    k,
                    input_params={"tags": ["partial archived", "fully archived"]},
                )
                dx.api.project_add_tags(k, input_params={"tags": ["fully archived"]})
            elif "fully archived" in tags:
                continue
            else:
                dx.api.project_add_tags(k, input_params={"tags": ["fully archived"]})

    # whatever is leftover from above projects, we do the query
    # they can be 'live' or 'partially archived'
    unsure_projects = {
        k: v for k, v in all_proj.items() if k not in archived_projects.keys()
    }

    for k, v in unsure_projects.items():
        tags = [tag.lower() for tag in v["describe"]["tags"]]

        status = set()
        for item in dx.find_data_objects(
            classname="file",
            project=k,
            describe={"fields": {"archivalState": True}},
        ):
            if len(status) > 1:
                break
            status.add(item["describe"]["archivalState"])

        if ("archived" in status) and ("live" in status):
            if not debug:  # if running in production
                if "fully archived" in tags:
                    # if 'fully archived' in tags
                    # we do a reset and add 'partial'
                    dx.api.project_remove_tags(
                        k,
                        input_params={"tags": ["partial archived", "fully archived"]},
                    )
                    dx.api.project_add_tags(
                        k, input_params={"tags": ["partial archived"]}
                    )
                elif "partial archived" in tags:
                    # if 'partially archived' is present
                    # this project is correctly tagged
                    continue
                else:
                    dx.api.project_add_tags(
                        k, input_params={"tags": ["partial archived"]}
                    )
        elif "live" not in status:
            if not debug:  # if running in production
                if "partial archived" in tags:
                    dx.api.project_remove_tags(
                        k,
                        input_params={"tags": ["partial archived", "fully archived"]},
                    )
                    dx.api.project_add_tags(
                        k, input_params={"tags": ["fully archived"]}
                    )
                elif "fully archived" in tags:
                    continue
                else:
                    dx.api.project_add_tags(
                        k, input_params={"tags": ["fully archived"]}
                    )
        else:
            # all files are live, no tagging needed
            continue


def archiving_function(
    archive_pickle: dict,
    today: dt.datetime,
    regex_excludes: list,
    debug: bool,
    archived_modified_month: int,
    archived_txt_path: str,
    archived_pickle_path: str,
    archived_failed_path: str,
    slack: SlackClass,
    project_52: str = "project-FpVG0G84X7kzq58g19vF1YJQ",
) -> None:
    """
    Function to check previously listed projs and dirs (memory)
    and do the archiving, then run find_proj_and_notify function.

    Skip projs or directories (staging52) if:
    1. tagged 'no-archive'
    2. tagged 'never-archive'
    3. modified in the past ARCHIVE_MODIFIED_MONTH month

    Parameters:
        archive_pickle: dict to store archived or to-be-archived proj/files
        today: to record today's date (datetime)
        regex_excludes: list of regex to match files for excluding
        debug: env ARCHIVE_DEBUG
        archived_modified_month: env ARCHIVED_MODIFIED_MONTH
        archived_txt_path: path to store archived.txt which list
            all archived project-id
        archived_pickle_path: path to store archiving memory
        archived_failed_path: path to store failed_archive.txt
            which list all file-id that failed archiving
        slack: Slack class for posting alert to Slack
        projec-52: staging52 project-id on DNAnexus

    """

    logger.info("Start archiving function")

    list_of_projects_in_memory = archive_pickle["to_be_archived"]
    list_of_directories_in_memory = archive_pickle["staging_52"]

    # just for recording what has been archived
    # plus for Slack notification
    temp_archived = collections.defaultdict(list)
    failed_archive = []

    if list_of_projects_in_memory:
        # loop through each project
        for proj_id in list_of_projects_in_memory:
            project = dx.DXProject(proj_id)

            # query latest project detail on archiving time
            detail = project.describe()
            proj_name = detail["name"]
            modified_epoch = detail["modified"]
            tags = detail["tags"]

            # check their tags

            if ("never-archive" in tags) or ("no-archive" in detail["tags"]):
                # project has been tagged never-archive or no-archive
                # normally project listed for archiving in memory
                # will not have no-archive tag to it
                # if there is, it means a user intentionally
                # tagged it thus we skip
                continue

            elif ("archive" in tags) or older_than(
                archived_modified_month, modified_epoch
            ):
                # if project is tagged with 'archive'
                # or project is inactive in last
                # 'archived_modified_month' month
                # both result in the same archiving process

                # find if there is file in this project
                # that match the exclude regex
                # if none, we can run dx.DXProject.archive
                # else, we archive file-id by file-id
                file_id_to_exclude = set()

                for word in regex_excludes:
                    # find all file-id that match the regex
                    file_id_to_exclude.update(
                        [
                            file["id"]
                            for file in list(
                                dx.find_data_objects(
                                    name=word,
                                    name_mode="regexp",
                                    project=proj_id,
                                    classname="file",
                                )
                            )
                        ]
                    )

                # get all file-ids in the project
                file_ids = [
                    file["id"]
                    for file in list(
                        dx.find_data_objects(project=proj_id, classname="file")
                    )
                ]
                archived_file_count = 0

                if file_id_to_exclude:
                    # if there is file-id that match exclude regex
                    if not debug:  # if running in production
                        for file_id in file_ids:
                            # if file-id match file-id in exclude list, skip
                            if file_id in file_id_to_exclude:
                                continue
                            try:
                                dx.DXFile(file_id, project=proj_id).archive()
                                archived_file_count += 1
                            except Exception as archiving_error:
                                logger.error(archiving_error)
                                failed_archive.append(f"{proj_id}:{file_id}")

                        if archived_file_count > 0:
                            temp_archived["archived"].append(
                                f"{proj_name} {proj_id} {archived_file_count}"
                            )
                else:
                    # if no file-id match the regex
                    # do an overall dx.Project.archive
                    if not debug:  # running in production
                        try:
                            res = dx.api.project_archive(
                                proj_id, input_params={"folder": "/"}
                            )
                            if res["count"] != 0:
                                temp_archived["archived"].append(
                                    f"{proj_name} {proj_id} {res['count']}"
                                )
                        except Exception:
                            # this normally happens when there are applet or
                            # record file type
                            # in project in which DNAnexus API for some reason
                            # run dx.File.archive on all of them which caused
                            # an error
                            # to pop up

                            # emailing DNAnexus support suggest running
                            # dx.File.archive
                            # individually as a workaround
                            logger.info(
                                f"Archiving {proj_id} file by file because"
                                " dx.Project.archive failed"
                            )

                            # get all files in project and do it individually
                            file_ids = [
                                file["id"]
                                for file in list(
                                    dx.find_data_objects(
                                        project=proj_id, classname="file"
                                    )
                                )
                            ]

                            archived_file_count = 0

                            for file_id in file_ids:
                                try:
                                    dx.DXFile(file_id, project=proj_id).archive()
                                    archived_file_count += 1
                                except Exception as e:
                                    logger.error(e)
                                    failed_archive.append(f"{proj_id}:{file_id}")

                            if archived_file_count > 0:
                                temp_archived["archived"].append(
                                    f"{proj_name} {proj_id} " f"{archived_file_count}"
                                )
            else:
                # project not older than ARCHIVE_MODIFIED_MONTH
                # meaning project has been modified recently, so skip
                logger.info(f"RECENTLY MODIFIED: {proj_name}")
                continue

    if list_of_directories_in_memory:
        for directory in list_of_directories_in_memory:
            directory_archive(
                directory,
                project_52,
                temp_archived,
                archived_modified_month,
                regex_excludes,
                failed_archive,
                debug,
            )

    # write file-id that failed archive
    if failed_archive:
        if os.path.isfile(archived_failed_path):
            with open(archived_failed_path, "a") as f:
                f.write("\n" + f"=== {today} ===")

                for line in failed_archive:
                    f.write("\n" + line)
        else:
            with open(archived_failed_path, "w") as f:
                f.write("\n" + f"=== {today} ===")
                f.write("\n".join(failed_archive))

    # keep a copy of what has been archived
    # ONLY IF THERE ARE FILEs BEING ARCHIVED
    if temp_archived:
        if os.path.isfile(archived_txt_path):
            with open(archived_txt_path, "a") as f:
                f.write("\n" + f"=== {today} ===")

                for line in temp_archived["archived"]:
                    f.write("\n" + line)
        else:
            with open(archived_txt_path, "w") as f:
                f.write("\n" + f"=== {today} ===")
                f.write("\n".join(temp_archived["archived"]))

        # also send a notification to say what have been archived
        slack.post_message_to_slack(
            "#egg-logs",
            "archived",
            today,
            data=temp_archived["archived"],
        )

    # empty pickle (memory)
    logger.info("Clearing pickle file")
    archive_pickle["to_be_archived"] = []
    archive_pickle["staging_52"] = []

    # save memory dict
    logger.info(f"Writing into pickle file at {archived_pickle_path}")
    with open(archived_pickle_path, "wb") as f:
        pickle.dump(archive_pickle, f)

    # do tagging for fully and partially archived projects
    tagging_function(debug)

    logger.info("End of archiving function")


def get_next_archiving_date(today: dt.datetime) -> dt.datetime:
    """
    Function to get the next automated-archive run date

    Parameters:
    :param: today `datetime`

    Return `datetime`
        if today.day is between 1-15: return 15th of this month
        if today.day is after 15: return 1st day of next month

    """

    if today.day not in [1, 15]:
        pass
    else:
        today += dt.timedelta(1)

    while today.day not in [1, 15]:
        today += dt.timedelta(1)

    return today


def make_datetime_format(modified_epoch: str) -> dt.datetime:
    """
    Function to turn modified epoch (returned by DNANexus)
    into readable datetime format

    Parameters:
    :param: modified_epoch: epoch datetime from dnanexus.describe()

    Return:
        epoch datetime in `datetime` format

    """

    modified = modified_epoch / 1000.0
    modified_dt = dt.datetime.fromtimestamp(modified)

    return modified_dt


def get_old_tar_and_notify(
    today: dt.datetime,
    tar_month: int,
    slack: SlackClass,
    project_52: str = "project-FpVG0G84X7kzq58g19vF1YJQ",
) -> None:
    """
    Function to get tar which are not modified in the last 3 months
    
    Regex Format:
        only returns "run.....tar.gz" in staging52
    
    :param: today: date for Slack notification
    :param: tar_month: N month of inactivity for tar.gz
        before getting picked up
    :param: slack: SlackClass for notification purpose
    :param: project_52: project-id of Staging52

    """
    logger.info("Getting old tar.gz in staging52")

    result = list(
        dx.find_data_objects(
            name="^run.*.tar.gz",
            name_mode="regexp",
            describe={"fields": {"modified": True, "folder": True, "name": True}},
            project=project_52,
        )
    )

    # list of tar files not modified in the last 3 months
    filtered_result = [
        x for x in result if older_than(tar_month, x["describe"]["modified"])
    ]

    id_results = [
        (x["id"], x["describe"]["folder"], x["describe"]["name"])
        for x in filtered_result
    ]

    dates = [make_datetime_format(d["describe"]["modified"]) for d in filtered_result]

    # earliest date among the list of tars
    min_date = min(dates).strftime("%Y-%m-%d")
    # latest date among the list of tars
    max_date = max(dates).strftime("%Y-%m-%d")

    slack.post_message_to_slack(
        "#egg-alerts",
        "tar",
        today,
        data=id_results,
        tar_period_start_date=min_date,
        tar_period_end_date=max_date,
    )
