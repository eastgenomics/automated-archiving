import os
import pickle
import collections
import datetime as dt
from dateutil.relativedelta import relativedelta

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
        # create new file if not present in path
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


def validate_directory(directory: str, month: int) -> bool:
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

    result: list = list(
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

    modified_epoch: int = result[0]["describe"]["modified"]

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

    DX_SECURITY_CONTEXT = {
        "auth_token_type": "Bearer",
        "auth_token": token,
    }

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
    try:
        dx.api.project_remove_tags(
            project_id,
            input_params={"tags": ["no-archive"]},
        )
    except Exception as e:
        logger.error(f"REMOVE TAG: {project_id} failed")
        logger.error(e)


def get_projects_as_dict(project_type: str) -> dict:
    """
    Function to fetch certain project type and return as
    dict (key: project id, value: describe return from dxpy)

    Parameters:
    :param: project_type: 002 or 003 or 004
    """
    result_dict = {}

    projects = list(
        dx.search.find_projects(
            name=f"^{project_type}.*",
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

    result_dict.update({proj["id"]: proj for proj in projects})

    return result_dict


def get_two_and_three_projects_as_single_dict() -> dict:
    """
    Function to get all 002 and 003 projects as a single dict
    """
    projects_dict_002: dict = get_projects_as_dict("002")
    projects_dict_003: dict = get_projects_as_dict("003")

    return {**projects_dict_002, **projects_dict_003}


def get_old_enough_projects(
    month2: int,
    month3: int,
    project52: str,
    project53: str,
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

    all_projects = get_two_and_three_projects_as_single_dict()

    projects_that_are_inactive = {
        k: v
        for k, v in all_projects.items()
        if (
            older_than(
                month2,
                v["describe"]["modified"],
            )  # condition for 002
            if v["describe"]["name"].startswith("002")
            else older_than(
                month3,
                v["describe"]["modified"],
            )  # condition for 003
            and v["describe"]["dataUsage"] != v["describe"]["archivedDataUsage"]
            and k not in [project52, project53]
        )
    }

    # get projects tagged 'archive'
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

    projects_that_are_inactive.update(
        {proj["id"]: proj for proj in projects_tagged_with_archive}
    )

    return projects_that_are_inactive


def get_all_directories_in_project(project_id: str) -> list:
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

    # filter out the /processed folder in root of staging-52
    directories_in_staging_area_52 = [
        (file.lstrip("/").lstrip("/processed"), file)
        for file in dx_project.list_folder(only="folders")["folders"]
        if file != "/processed"  # directories in root of staging-52
    ] + [
        (file.lstrip("/").lstrip("/processed"), file)
        for file in dx_project.list_folder(
            "/processed",
            only="folders",
        )[
            "folders"
        ]  # directories in /processed folder
    ]

    return directories_in_staging_area_52


def get_files_in_project_based_on_one_tag(tag: str, project_id: str) -> list:
    """
    Function to get files in a project based on a single tag

    Parameters:
    :param: tag: tag to search for
    :param: project_id: project-id to search for
    """
    if not tag:
        return []

    results = list(
        dx.find_data_objects(
            project=project_id,
            tags=[tag],
            describe={
                "fields": {
                    "folder": True,
                },
            },
        )
    )

    return results


def get_projects_and_directory_based_on_single_tag(
    tag: str,
    project_id: str,
) -> list:
    """
    Function to get all projects and directories tagged with
    a single tag

    Parameters:
    :param: tag: tag to search for
    :param: project_id: project-id to search for
    """

    if not tag:
        return []

    results = []

    staging_area_files = [
        file["describe"]["folder"].lstrip("/")
        for file in get_files_in_project_based_on_one_tag(
            tag,
            project_id,  # stagingarea-52 project id
        )
    ]

    # clean the directory path and append to list
    for directory in staging_area_files:
        temp = directory.split("/")
        if "processed" in directory:
            results.append(f"/{temp[0]}/{temp[1]} in `staging52`")
        else:
            results.append(f"{temp[0]} in `staging52`")

    agg_dict = get_two_and_three_projects_as_single_dict()

    results += [
        proj["describe"]["name"]
        for proj in agg_dict.values()
        if tag in proj["describe"]["tags"]
    ]

    return results


def add_tag_to_project(tag: str, project_id: str) -> None:
    try:
        dx.api.project_add_tags(
            project_id,
            input_params={
                "tags": [tag],
            },
        )
    except dx.exceptions.ResourceNotFound:
        logger.error(f"{project_id} not found when tagging")
    except dx.exceptions.InvalidInput:
        logger.error(f"invalid tag input when tagging {tag}")
    except dx.exceptions.PermissionDenied:
        logger.error(f"permission denied when tagging {project_id}")
    except Exception as e:
        # no idea what's wrong
        logger.error(e)


def remove_tags_from_project(tags: list, project_id: str) -> None:
    try:
        dx.api.project_remove_tags(
            project_id,
            input_params={
                "tags": tags,
            },
        )
    except dx.exceptions.ResourceNotFound:
        logger.error(f"{project_id} not found when tagging")
    except dx.exceptions.InvalidInput:
        logger.error(f"invalid tag input when tagging {tags}")
    except dx.exceptions.PermissionDenied:
        logger.error(f"permission denied when tagging {project_id}")
    except Exception as e:
        # no idea what's wrong
        logger.error(e)


def tagging_function(debug: bool) -> None:
    """
    Function to check latest archivalStatus of files
    in a project and add appropriate tag

    Parameters:
    :param: debug: `bool` whether the script is ran in DEBUG mode
    """

    logger.info("Running tagging function")

    all_projects = get_two_and_three_projects_as_single_dict()

    # separate out those with archivedDataUsage == dataUsage
    # which are fully archived so we don't have to query them
    archived_projects = {
        k: v
        for k, v in all_projects.items()
        if v["describe"]["archivedDataUsage"] == v["describe"]["dataUsage"]
    }

    for project_id, v in archived_projects.items():
        tags = [tag.lower() for tag in v["describe"]["tags"]]

        if not debug:  # if running in production
            if "partial archived" in tags:
                remove_tags_from_project(
                    ["partial archived", "fully archived"],
                    project_id,
                )
                add_tag_to_project(project_id, "fully archived")
            elif "fully archived" in tags:
                continue
            else:
                add_tag_to_project(project_id, "fully archived")

    # whatever is leftover from above projects, we do the query
    # they can be 'live' or 'partially archived'
    projects_with_unsure_archival_status = {
        project_id: v
        for project_id, v in all_projects.items()
        if project_id not in archived_projects.keys()
    }

    for project_id, v in projects_with_unsure_archival_status.items():
        # get project tags
        tags = [tag.lower() for tag in v["describe"]["tags"]]

        # get all archival status within the projects
        status = set(
            [
                file["describe"]["archivalState"]
                for file in dx.find_data_objects(
                    classname="file",
                    project=project_id,
                    describe={
                        "fields": {
                            "archivalState": True,
                        },
                    },
                )
            ]
        )

        if "archived" in status and "live" in status:
            if not debug:  # if running in production
                if "fully archived" in tags:
                    # if 'fully archived' in tags
                    # we do a reset and add 'partial'
                    dx.api.project_remove_tags(
                        project_id,
                        input_params={
                            "tags": ["partial archived", "fully archived"],
                        },
                    )
                    remove_tags_from_project()
                    add_tag_to_project("partial archived", project_id)
                elif "partial archived" in tags:
                    # if 'partially archived' is present
                    # this project is correctly tagged
                    continue
                else:
                    add_tag_to_project("partial archived", project_id)
        elif "live" not in status:
            # everything is archived within the project
            if not debug:
                if "partial archived" in tags:
                    dx.api.project_remove_tags(
                        project_id,
                        input_params={
                            "tags": ["partial archived", "fully archived"],
                        },
                    )
                    add_tag_to_project("fully archived", project_id)
                elif "fully archived" in tags:  # correctly tagged
                    continue
                else:
                    add_tag_to_project("fully archived", project_id)
        else:
            # all files are live, no tagging needed
            continue


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
            describe={
                "fields": {"modified": True, "folder": True, "name": True},
            },
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
