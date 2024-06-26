import os
import pickle
import collections
import datetime as dt
from dateutil.relativedelta import relativedelta
import dxpy as dx
import argparse
import configparser
import concurrent

from bin.helper import get_logger

logger = get_logger(__name__)


def parse_arguments() -> argparse.Namespace:
    """
    Parse arguments

    Returns: date Object
    """
    parser = argparse.ArgumentParser(
        description="optional datetime override argument in format YYYYMMDD",
    )

    # optional arguments
    parser.add_argument(
        "-dt",
        "--datetime",
        help="override script datetime. input format: YYYYMMDD",
    )

    return parser.parse_args()


def parse_datetime(args: argparse.Namespace) -> dt.date:
    """
    Parse datetime from arguments
    If not provided, use today's date
    """
    datetime = dt.date.today()

    if args.datetime:
        try:
            datetime = dt.datetime.strptime(args.datetime, "%Y%m%d").date()
        except ValueError:
            logger.error(
                f"Invalid datetime format. Use YYYYMMDD. Arg: {args.datetime}"
            )

    return datetime


def older_than(
    month: int,
    modified_epoch: int,
) -> bool:
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


def call_in_parallel(func, items, **find_data_args) -> list:
    """
    Calls the given function in parallel using concurrent.futures on
    the given set of items (i.e for calling dxpy.describe() on multiple
    object IDs)
    Borrowed from dias_reports_bulk_reanalysis

    Parameters
    ----------
    func : callable
        function to call on each item
    items : list
        list of items to call function on
    find_data_args: dict
        kwargs - these need to be passed to the iterated-over function

    Returns
    -------
    list
        list of responses
    """
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        if find_data_args:
            concurrent_jobs = {
                executor.submit(func, item, **find_data_args): item
                for item in items
            }
        else:
            concurrent_jobs = {
                executor.submit(func, item): item for item in items
            }

        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                results.append(future.result())
            except Exception as exc:
                # catch any errors that might get raised during querying
                print(
                    f"Error getting data for {concurrent_jobs[future]}: {exc}"
                )
                raise exc

    return results


def find_files_by_folder_paths_parallel(paths, project):
    """
    Finding files with parallelised search, known project, list of paths.
    Return tags and archival states, so that we can inspect for live files
    and the presence of 'never-archive' tags.
    """

    def _find(path, **find_data_args):
        """
        Run individual search job
        """
        return list(
            dx.find_data_objects(
                classname="file",
                project=find_data_args["project"],
                folder=path,
                describe={
                    "fields": {
                        "created": True,
                        "archivalState": True,
                        "tags": True,
                        "modified": True,
                    }
                },
            )
        )

    return call_in_parallel(_find, paths, project=project)


def read_or_new_pickle(path: str) -> dict:
    """
    Read stored pickle memory for the script

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


def write_to_pickle(path: str, pickle_dict: dict) -> None:
    """
    Write to memory pickle

    Parameters:
    :param: path: directory path to pickle
    :param: pickle_dict: `dict` to write into pickle

    Returns:
        `None`
    """
    logger.info(f"Writing into pickle file at: {path}")
    with open(path, "wb") as f:
        pickle.dump(pickle_dict, f)


def dx_login(token: str) -> bool:
    """
    DNAnexus login
    Return True if successful, False otherwise

    Parameters:
    :param: token: dnanexus auth token
    """

    DX_SECURITY_CONTEXT = {
        "auth_token_type": "Bearer",
        "auth_token": token,
    }

    dx.set_security_context(DX_SECURITY_CONTEXT)

    try:
        dx.api.system_whoami()
        logger.info("DNANexus login successful")
        return True

    except dx.exceptions.InvalidAuthentication as _:
        return False


def get_projects_as_dict(project_prefix: str) -> dict:
    """
    Function to fetch certain project type and return as
    dict (key: project id, value: describe return from dxpy)

    Parameters:
    :param: project_prefix: 002 or 003 or 004
    """

    return {
        proj["id"]: proj
        for proj in dx.search.find_projects(
            name=f"^{project_prefix}.*",
            name_mode="regexp",
            billed_to="org-emee_1",
            describe={
                "fields": {
                    "name": True,
                    "tags": True,
                    "created": True,
                    "modified": True,
                    "createdBy": True,
                    "dataUsage": True,
                    "archivedDataUsage": True,
                }
            },
        )
    }


def get_members(config_path: str) -> dict:
    """
    Function to read members.ini config file for members' dnanexus id and slack id

    Parameters:
    :param: config_path: path to members.ini file

    Returns:
    :return: dict: {dnanexus_id: slack_id}
    """
    config = configparser.ConfigParser()
    config.read(config_path)

    try:
        return dict(config.items("members"))
    except configparser.NoSectionError as e:
        logger.error(e)
        return {}
