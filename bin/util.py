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


def _older_than(month: int, modified_epoch: int) -> bool:
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


def _get_projects_as_dict(project_type: str) -> dict:
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


def _get_two_and_three_projects_as_single_dict() -> dict:
    """
    Function to get all 002 and 003 projects as a single dict
    """
    projects_dict_002: dict = _get_projects_as_dict("002")
    projects_dict_003: dict = _get_projects_as_dict("003")

    return {**projects_dict_002, **projects_dict_003}


def _add_tag_to_project(tag: str, project_id: str) -> None:
    """
    Add tag to project. Deal with exceptions

    Parameters:
    :param: tag: `str` tag to add to project
    :param: project_id: `str` project id to add tag to
    """
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


def _remove_tags_from_project(tags: list, project_id: str) -> None:
    """
    Remove tag from project. Deal with exceptions

    Parameters:
    :param: tags: `list` tags to remove from project
    :param: project_id: `str` project id to remove tag from
    """
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
    in a project and add appropriate tag to the project

    Parameters:
    :param: debug: `bool` whether the script is ran in DEBUG mode
    """

    logger.info("Running tagging function")

    if debug:  # if debug, return
        logger.info("Running in DEBUG mode. Skipping tagging function")
        return

    all_projects = _get_two_and_three_projects_as_single_dict()

    # separate out those with archivedDataUsage == dataUsage
    # which are fully archived so we don't have to query them
    archived_projects = {
        k: v
        for k, v in all_projects.items()
        if v["describe"]["archivedDataUsage"] == v["describe"]["dataUsage"]
    }

    for project_id, v in archived_projects.items():
        tags = [tag.lower() for tag in v["describe"]["tags"]]

        if "partial archived" in tags:
            _remove_tags_from_project(
                ["partial archived", "fully archived"],
                project_id,
            )
            _add_tag_to_project(project_id, "fully archived")
        elif "fully archived" in tags:
            continue
        else:
            _add_tag_to_project(project_id, "fully archived")

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
            if "fully archived" in tags:
                # if 'fully archived' in tags
                # we do a reset and add 'partial'
                dx.api.project_remove_tags(
                    project_id,
                    input_params={
                        "tags": ["partial archived", "fully archived"],
                    },
                )
                _remove_tags_from_project()
                _add_tag_to_project("partial archived", project_id)
            elif "partial archived" in tags:
                # if 'partially archived' is present
                # this project is correctly tagged
                continue
            else:
                _add_tag_to_project("partial archived", project_id)
        elif "live" not in status:
            # everything is archived within the project
            if "partial archived" in tags:
                dx.api.project_remove_tags(
                    project_id,
                    input_params={
                        "tags": ["partial archived", "fully archived"],
                    },
                )
                _add_tag_to_project("fully archived", project_id)
            elif "fully archived" in tags:  # correctly tagged
                continue
            else:
                _add_tag_to_project("fully archived", project_id)
        else:
            # all files are live, no tagging needed
            continue


def _make_datetime_format(modified_epoch: str) -> dt.datetime:
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
    debug: bool = False,
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
                "fields": {
                    "modified": True,
                    "folder": True,
                    "name": True,
                },
            },
            project=project_52,
            limit=5 if debug else None,
        )
    )

    # list of tar files not modified in the last 3 months
    filtered_result = [
        x for x in result if _older_than(tar_month, x["describe"]["modified"])
    ]

    if not filtered_result:
        # no .tar older than tar_month
        return None

    id_results = [
        (x["id"], x["describe"]["folder"], x["describe"]["name"])
        for x in filtered_result
    ]

    dates = [_make_datetime_format(d["describe"]["modified"]) for d in filtered_result]

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
