import itertools
import collections
import dxpy as dx
import datetime as dt
from itertools import groupby

from bin.environment import EnvironmentVariableClass
from bin.helper import get_logger
from bin.util import (
    find_files_by_folder_paths_parallel,
    older_than,
    read_or_new_pickle,
    write_to_pickle,
    get_projects_as_dict,
    call_in_parallel,
)

logger = get_logger(__name__)


class FindClass:
    def __init__(
        self,
        env: EnvironmentVariableClass,
        dnanexus_id_to_slack_id: dict,
    ):
        self.env = env
        self.dnanexus_id_to_slack_id = dnanexus_id_to_slack_id

        self.archiving_projects = []

        self.archiving_directories = []
        self.archiving_precision_directories = []

        self.archiving_projects_2_slack = []
        self.archiving_projects_3_slack = []

        self.archiving_directories_slack = []
        self.archiving_precision_directories_slack = []

        self.archive_pickle = read_or_new_pickle(
            env.AUTOMATED_ARCHIVE_PICKLE_PATH
        )

    def _get_old_enough_projects(
        self,
    ) -> dict:
        """
        Function to get all 002 and 003 projects
        - that are old enough (based on AUTOMATED_MONTH_002 and AUTOMATED_MONTH_003)
            - CEN/WES projects are old enough based on AUTOMATED_CEN_WES_MONTH
        - that are not fully archived
        - that have `archive` tag

        Returns:
            - dict (key: project id, value: describe return from dxpy)
        """

        all_projects = {
            **get_projects_as_dict("002"),
            **get_projects_as_dict("003"),
        }

        filtered_projects = {
            k: v
            for k, v in all_projects.items()
            if (
                (
                    (
                        (
                            older_than(
                                self.env.AUTOMATED_MONTH_002,
                                v["describe"]["created"],
                            )
                            if v["describe"]["name"].startswith("002")
                            and not (
                                v["describe"]["name"].endswith("WES")
                                or v["describe"]["name"].endswith("CEN")
                            )
                            else (
                                older_than(
                                    self.env.AUTOMATED_CEN_WES_MONTH,
                                    v["describe"]["created"],
                                )
                                if v["describe"]["name"].startswith("002")
                                and (
                                    v["describe"]["name"].endswith("WES")
                                    or v["describe"]["name"].endswith("CEN")
                                )
                                else older_than(
                                    self.env.AUTOMATED_MONTH_003,
                                    v["describe"]["created"],
                                )
                            )
                        )  # old enough logic
                        and v["describe"]["dataUsage"]
                        != v["describe"][
                            "archivedDataUsage"
                        ]  # not fully archived
                    )
                    and (
                        older_than(
                            self.env.ARCHIVE_MODIFIED_MONTH,
                            v["describe"]["modified"],
                        )
                    )  # not modified in the last ARCHIVE_MODIFIED_MONTH
                )
                or "archive" in v["describe"]["tags"]  # has 'archive' tag
            )
            and k
            not in self.env.PRECISION_ARCHIVING  # exclude precision projects
        }

        return filtered_projects

    def _get_folders_in_project(
        self,
        project_id: str,
        directory_path: str = "/",  # default to root
    ) -> list:
        """
        Function to grab all folders in a project-id

        Parameters:
        :param: project_id: DNAnexus project-id
        :param: directory_path: directory path to search for folders e.g. "/" or "/processed"

        Return:
        list of folders in the project-id

        """
        try:
            return dx.DXProject(project_id).list_folder(
                folder=directory_path,
                only="folders",  # just get folders
                describe={"fields": {"archivalState": True}},
            )["folders"]
        except Exception as e:
            logger.error(e)  # probably wont happen but just in case
            return []

    def _validate_directory(self, directory: str) -> bool:
        """
        Check if directory or folder is valid:
            - if its 002 or 003 project fits the criteria for archiving
            - criteria is the month of inactivity of its parent project

        Parameters:
        :param: directory: directory or folder to check

        Returns:
        :return: True if parent project fits the criteria
        :return: False if parent project does not fit the criteria or parent project not found
        """

        # find its parent project (002 or 003)
        data: list = list(
            dx.find_projects(
                f"(002|003)_{directory}*",
                name_mode="regexp",
                describe={"fields": {"modified": True, "name": True}},
                limit=1,
            )
        )

        # if no 002/003 project
        if not data:
            return False

        project_name: str = data[0]["describe"]["name"]
        modified_epoch: int = data[0]["describe"]["modified"]

        # check modified date of the 002 or 003 project
        if older_than(
            (
                self.env.AUTOMATED_MONTH_002
                if project_name.startswith("002")
                else self.env.AUTOMATED_MONTH_003
            ),
            modified_epoch,
        ):
            return True

        return False

    def get_archival_status_parallel(self, projects) -> list:
        """
        Call dxpy.find_data_objects in parallel.

        Adapted from dias_reports_bulk_reanalysis.

        Parameters
        ----------
        projects : list
            project IDs in which to restrict search scope

        Returns
        -------
        list
            list of all found dxpy object details
        """

        def _find(project):
            """
            Query given patterns as a regex search term to find all files
            """
            return list(
                dx.find_data_objects(
                    project=project,
                    describe={"fields": {"archivalState": True}},
                )
            )

        return call_in_parallel(
            func=_find, items=projects
        )

    def find_projects(
        self,
    ) -> None:
        """
        Function to find projects that are old enough
        and not fully archived (by checking status of files within the project)
        """

        logger.info("Finding projects..")

        # get all old enough projects
        # the results will also contain archivalState
        qualified_projects = self._get_old_enough_projects()

        logger.info(
            f"Number of 'old enough' projects found: {len(qualified_projects)}!"
        )

        user_to_project_id_and_dnanexus = collections.defaultdict(list)

        # get archival statuses for each project
        file_archival_statuses = self.get_archival_status_parallel(
            qualified_projects
        )
        file_archival_statuses = {
            k: list(v)
            for k, v in groupby(file_archival_statuses, lambda x: x["project"])
        }

        for index, (project_id, v) in enumerate(qualified_projects.items()):
            if (index + 1) % 25 == 0:
                logger.info(
                    f"Processing {index + 1}/{len(qualified_projects)}"
                )

            project_name: str = v["describe"]["name"]
            project_tags: list[str] = [
                tag.lower() for tag in v["describe"]["tags"]
            ]
            trimmed_project_id = project_id.lstrip("project-")
            user: str = v["describe"]["createdBy"]["user"]

            if "never-archive" in project_tags:
                logger.info(
                    f'Project {project_name} is tagged with "never-archive". Skip.'
                )
            else:
                # get all files' archivalStatus
                project_statuses = file_archival_statuses.get(project_id)
                statuses = set(
                    [x["describe"]["archivalState"] for x in project_statuses],
                )

                if "live" in statuses:
                    pass  # there is something to be archived
                else:
                    logger.info(f"Everything archived in {project_id}. Skip.")
                    continue  # everything has been archived

                # add project-id to archiving list (002 and 003)
                self.archiving_projects.append(project_id)

                # below are preparation for slack notification
                dnanexus_project_url = f"<{self.env.DNANEXUS_URL_PREFIX}/{trimmed_project_id}/|{project_name}>"

                if project_name.startswith("002"):
                    self.archiving_projects_2_slack.append(
                        dnanexus_project_url
                    )
                else:
                    user_to_project_id_and_dnanexus[user].append(
                        {
                            "id": project_id,
                            "link": dnanexus_project_url,
                        }
                    )

        # get everything ready for slack notification
        self.archiving_projects_2_slack = sorted(
            self.archiving_projects_2_slack
        )

        # sort 003 project by user for slack notification
        self.sort_slack_3_by_user(user_to_project_id_and_dnanexus)

    def sort_slack_3_by_user(self, user_to_project_id_and_dnanexus):
        """
        Sort 003 project by user for Slack notification
        """
        current_user = None
        for user, values in user_to_project_id_and_dnanexus.items():
            if current_user is None:  # first user
                current_user = user

            if current_user != user and current_user is not None:
                self.archiving_projects_3_slack.append("\n")
                current_user = user

            self.archiving_projects_3_slack.append(
                f"<@{self.dnanexus_id_to_slack_id[user]}>"
                if user in self.dnanexus_id_to_slack_id
                else f"Cannot find id for: {user}"
            )

            for row in values:
                project_id = row["id"]
                dnanexus_link = row["link"]

                self.archiving_projects_3_slack.append(dnanexus_link)

    def find_directories(
        self,
    ) -> None:
        """
        Function to find directories in staging-52 parent projects (002 or
        003). The projects have already been checked to ensure they've not
        been modified in the last AUTOMATED_MONTH_002 or AUTOMATED_MONTH_003
        months.
        """

        logger.info("Finding directories..")

        trimmed_to_original_folder_path = {}

        # get folders in root of stagingarea-52
        for folder in self._get_folders_in_project(self.env.PROJECT_52):
            if folder == "/processed":
                continue

            trimmed_to_original_folder_path[folder.lstrip("/")] = folder

        # get folders in /processed of stagingarea-52
        for folder in self._get_folders_in_project(
            self.env.PROJECT_52, directory_path="/processed"
        ):
            trimmed_to_original_folder_path[
                folder.lstrip("/processed/")
            ] = folder

        logger.info(
            f"Found {len(trimmed_to_original_folder_path)} directories in staging-52"
        )

        # check if directories have parent project (002 / 003)
        # and it has not been modified in the last N month
        trimmed_to_original_folder_path = {
            trimmed: _
            for trimmed, _ in trimmed_to_original_folder_path.items()
            if self._validate_directory(trimmed)
        }

        # trimmed_to_original_folder_path looks like:
        # e.g {"my_folder": "/my_folder", "a_folder": "/processed/folder"}

        logger.info(
            f"Number of 'old enough' directories: {len(trimmed_to_original_folder_path)}",
        )

        # project url for slack notification
        project52 = self.env.PROJECT_52.lstrip("project-")
        STAGING_PREFIX = f"{self.env.DNANEXUS_URL_PREFIX}/{project52}/data"

        # retrieve files in every folder
        # then group the files per-folder
        project_files = find_files_by_folder_paths_parallel(
            self.env.PROJECT_52, trimmed_to_original_folder_path.values()
        )
        project_files = {
            k: list(v)
            for k, v in groupby(project_files, lambda x: x["folder"])
        }
        # look at the files in each path for the project
        for folder in trimmed_to_original_folder_path.values():
            # get files in this folder
            folder_files = project_files.get(folder)

            tags = set(
                itertools.chain.from_iterable(
                    [x["describe"]["tags"] for x in folder_files]
                )
            )

            # if there's 'never-archive' tag in any file, continue
            if "never-archive" in tags:
                logger.info('Directory has "never-archive" tag. Skip.')
                continue

            # filter out files so you only get 'live' ones
            statuses = set(
                [x["describe"]["archivalState"] for x in folder_files],
            )
            if "live" in statuses:
                self.archiving_directories.append(folder)
                self.archiving_directories_slack.append(
                    f"<{STAGING_PREFIX}{folder}|{folder}>"
                )

    def _turn_epoch_to_datetime(self, epoch: int) -> dt.datetime:
        """
        Function to turn epoch to datetime
        """
        return dt.datetime.fromtimestamp(epoch / 1000.0)

    def find_precisions(
        self,
    ) -> None:
        """
        Function to find folders in "precisions" projects
        that have not been modified in last PRECISION_MONTH
        """
        logger.info("Finding precision projects..")

        # assemble a dictionary of project IDs with their directories
        project_to_prefix = dict()

        for project_id in self.env.PRECISION_ARCHIVING:
            try:
                project = dx.DXProject(project_id)
            except Exception:
                # project is not found by dnanexus
                logger.info(
                    f"Precision project {project_id} not found on DNAnexus. Skip."
                )
                continue  # skip

            # there are specific prefixes for the Slack message, for some reason
            project_to_prefix[
                project_id
            ] = f"{self.env.DNANEXUS_URL_PREFIX}/{project_id.lstrip('project-')}/data"

            # get all folders within the project
            folders = self._get_folders_in_project(project)

            # parallel-fetch the files for the project
            folder_files = find_files_by_folder_paths_parallel(
                folders,
                project["id"],
            )
            folder_files = {
                k: list(v)
                for k, v in groupby(
                    folder_files, lambda x: x["describe"]["folder"]
                )
            }

            # for each folder, check whether the contents are live, never-archive,
            # or were modified recently enough to archive
            for folder, files in folder_files.items():
                active_files = [
                    file
                    for file in files
                    if file["describe"]["archivalState"] == "live"
                ]
                project_tags = [file["describe"]["tags"] for file in files]
                latest_modified_date = max(
                    [file["describe"]["modified"] for file in files]
                )  # get latest modified date

                if "never-archive" in project_tags:
                    logger.info(
                        f'Folder {folder} is tagged with "never-archive". Skip.'
                    )
                    if not active_files:  # if no file in folder
                        logger.info(
                            f"No live files found in {project_id}:{folder}. Skip."
                        )
                        continue

                # see if latest modified date is more than precision_month
                if older_than(self.env.PRECISION_MONTH, latest_modified_date):
                    # if the oldest modified file is older than precision_month
                    # add the folder path and project-id to memory pickle
                    self.archiving_precision_directories.append(
                        f"{project_id}|{folder}"
                    )
                    self.archiving_precision_directories_slack.append(
                        f"<{project_to_prefix[project_id]}{folder}|{folder}>"
                    )

    def save_to_pickle(self):
        """
        Save memory to pickle
        """

        self.archive_pickle["projects"] = self.archiving_projects
        self.archive_pickle["directories"] = self.archiving_directories
        self.archive_pickle[
            "precisions"
        ] = self.archiving_precision_directories

        write_to_pickle(
            self.env.AUTOMATED_ARCHIVE_PICKLE_PATH, self.archive_pickle
        )

    def get_tar(self) -> list:
        """
        Function to get all .tar files in staging-52 that fits below criteria:
        - not been modified in the last TAR_MONTH

        Returns:
        :return: list of dict (id, folder, name, modified)
        """
        logger.info("Getting all .tar files in staging-52..")

        # make the plain-number month value compatible with 'modified_before'
        # argument in find_data_objects
        month_modified_before = f"-{self.env.TAR_MONTH}m"

        # list of tar files not modified in the last 3 months
        tars = dx.find_data_objects(
            name="^run.*.tar.gz",
            name_mode="regexp",
            modified_before=month_modified_before,
            describe={
                "fields": {
                    "modified": True,
                    "folder": True,
                    "name": True,
                },
            },
            project=self.env.PROJECT_52,
        )
        if not tars:
            # no .tar older than tar_month
            return []

        tars_slack = [
            (
                f["id"],
                f["describe"]["folder"],
                f["describe"]["name"],
                self._turn_epoch_to_datetime(
                    f["describe"]["modified"]
                ).strftime("%c"),
            )
            for f in tars
        ]

        return tars_slack
