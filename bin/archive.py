import os
import collections
import datetime as dt
import dxpy as dx
from dateutil.relativedelta import relativedelta

from bin.util import older_than
from bin.slack import SlackClass
from bin.helper import get_logger

logger = get_logger(__name__)


class ArchiveClass:
    def __init__(
        self,
        debug: bool,
        today_datetime: dt.datetime,
        archived_modified_month: int,
        month2: int,
        month3: int,
        regex_excludes: list,
        project_52: str,
        project_53: str,
        archive_pickle_path: str,
        archived_failed_path: str,
        archived_txt_path: str,
        members: dict,
        dnanexus_url_prefix: str,
        precision_projects: list,
        precision_month: int,
        slack: SlackClass,
    ):
        self.debug = debug
        self.today_datetime = today_datetime
        self.archived_modified_month = archived_modified_month
        self.month2 = month2
        self.month3 = month3
        self.regex_excludes = regex_excludes
        self.project_52 = project_52
        self.project_53 = project_53
        self.archive_pickle_path = archive_pickle_path
        self.archived_failed_path = archived_failed_path
        self.archived_txt_path = archived_txt_path
        self.members = members
        self.dnanexus_url_prefix = dnanexus_url_prefix
        self.precision_projects = precision_projects
        self.precision_month = precision_month
        self.slack = slack

        self.project_ids_to_exclude = set(
            precision_projects + [self.project_52, self.project_53]
        )

        self.special_notify_list: list[str] = []

    def _get_files_in_project_based_on_one_tag(self, tag: str, project_id: str) -> list:
        """
        Function to get files in a project based on a single tag

        Parameters:
        :param: tag: tag to search for
        :param: project_id: project-id to search for
        """
        if not tag:
            return []

        return list(
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

    def get_projects_and_directory_based_on_single_tag(
        self,
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
            for file in self._get_files_in_project_based_on_one_tag(
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

        agg_dict = self._get_two_and_three_projects_as_single_dict()

        results += [
            proj["describe"]["name"]
            for proj in agg_dict.values()
            if tag in proj["describe"]["tags"]
        ]

        return results

    def _remove_tag_from_file(self, file_id: str, project_id: str) -> None:
        try:
            dx.api.file_remove_tags(
                file_id,
                input_params={
                    "tags": ["no-archive"],
                    "project": project_id,
                },
            )
        # catching DNAnexus-related errors
        except (
            dx.exceptions.ResourceNotFound,
            dx.exceptions.PermissionDenied,
            dx.exceptions.InvalidInput,
        ) as e:
            logger.error(f"Tag file error (DNAnexus): {e}")
        # non-DNAnexus related errors
        except Exception as e:
            logger.error(f"Archiving file error (Unknown): {e}")

    def _remove_project_tag(self, project_id: str) -> None:
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
            logger.error(f"REMOVE TAG: {project_id} failed with error {e}")

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
        if self._older_than(
            self.month2 if project_name.startswith("002") else self.month3,
            modified_epoch,
        ):
            return True
        else:
            return False

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
            dx_project = dx.DXProject(project_id)

            return dx_project.list_folder(folder=directory_path, only="folders")[
                "folders"
            ]
        except Exception as e:
            logger.error(e)  # probably wont happen but just in case
            return []

        # def _get_all_directories_in_project(self, project_id: str) -> list:
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

        # need to do this twice in root and in /processed
        # because that's how staging-52 is structured
        return [
            (file.lstrip("/").lstrip("/processed"), file)
            for file in self._get_folders_in_project(project_id)
            if file != "/processed"  # directories in root of staging-52
        ] + [
            (file.lstrip("/").lstrip("/processed"), file)
            for file in self._get_folders_in_project(
                project_id, directory_path="/processed"
            )  # directories in /processed folder
        ]

    def _get_projects_as_dict(self, project_type: str) -> dict:
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

    def _get_two_and_three_projects_as_single_dict(self) -> dict:
        """
        Function to get all 002 and 003 projects as a single dict
        """
        projects_dict_002: dict = self._get_projects_as_dict("002")
        projects_dict_003: dict = self._get_projects_as_dict("003")

        return {**projects_dict_002, **projects_dict_003}

    def _older_than(
        self,
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

    def _get_old_enough_projects(
        self,
        project_ids_to_exclude: set,
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

        all_projects = self._get_two_and_three_projects_as_single_dict()

        projects_that_are_inactive = {
            k: v
            for k, v in all_projects.items()
            if (
                self._older_than(
                    self.month2,
                    v["describe"]["modified"],
                )  # condition for 002
                if v["describe"]["name"].startswith("002")
                else self._older_than(
                    self.month3,
                    v["describe"]["modified"],
                )  # condition for 003
                and v["describe"]["dataUsage"] != v["describe"]["archivedDataUsage"]
                and k not in project_ids_to_exclude
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

    def get_next_archiving_date_relative_to_today(
        self, today: dt.datetime
    ) -> dt.datetime:
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

    def _archive_file(
        self,
        file_id: str,
        project_id: str,
        count: int = 0,
        failed_record: list = [],
    ) -> None:
        """
        Function to archive file-id on DNAnexus

        Parameters:
            file_id: file-id to be archived
            project_id: project-id where the file is in
            count: counter to keep track of how many files have been archived
            failed_record: list to record file-id that failed archiving
        """
        try:
            dx.DXFile(
                file_id,
                project=project_id,
            ).archive()
            count += 1

        # catching DNAnexus-related errors
        except (
            dx.exceptions.ResourceNotFound,
            dx.exceptions.PermissionDenied,
            dx.exceptions.InvalidInput,
            dx.exceptions.InvalidState,
        ) as e:
            logger.error(f"Archiving file error (DNAnexus): {e}")
            failed_record.append(f"{project_id}:{file_id}")
        # non-DNAnexus related errors
        except Exception as e:
            logger.error(f"Archiving file error (Unknown): {e}")
            failed_record.append(f"{project_id}:{file_id}")

    def _archive_directory_based_on_directory_path(
        self,
        directory_path: str,
        temp_dict: dict,
        failed_archive: list,
    ) -> None:
        """
         Function to deal with directories in staging52

        Parameters:
             directory_path: directory path in staging52
             temp_dict: temporary dict to store file-id that have been archived
             failed_archive: list to record file-id that failed archiving

         Returns:
             None
        """

        # check for 'never-archive' tag in directory
        never_archive = list(
            dx.find_data_objects(
                project=self.project_52,
                folder=directory_path,
                tags=["never-archive"],
                limit=1,
            )
        )

        if never_archive:
            logger.info(f"NEVER ARCHIVE: {directory_path} in staging52")
            return

        # 2 * 4 week = 8 weeks
        num_weeks = self.archived_modified_month * 4

        # check if there's any files modified in the last num_weeks
        recent_modified = list(
            dx.find_data_objects(
                project=self.project_52,
                folder=directory_path,
                modified_after=f"-{num_weeks}w",
                limit=1,
            )
        )

        if recent_modified:
            logger.info(f"RECENTLY MODIFIED: {directory_path} in staging52")
            return

        # check for 'no-archive' tag in directory
        no_archive = list(
            dx.find_data_objects(
                project=self.project_52,
                folder=directory_path,
                tags=["no-archive"],
                limit=1,
            )
        )

        if no_archive:
            logger.info(f"NO ARCHIVE: {directory_path} in staging52")
            return
        else:
            # if directory in staging52 got
            # no tag indicating dont archive
            # it will end up here
            file_ids_to_exclude = set()

            # get all file-id that match exclude regex
            for word in self.regex_excludes:
                file_ids_to_exclude.update(
                    [
                        file["id"]
                        for file in list(
                            dx.find_data_objects(
                                name=word,
                                name_mode="regexp",
                                project=self.project_52,
                                folder=directory_path,
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
                    for file in list(
                        dx.find_data_objects(
                            project=self.project_52,
                            folder=directory_path,
                            limit=5 if self.debug else None,
                        )
                    )
                ]

                archived_file_count = 0
                if not self.debug:  # if running in production
                    for file_id in file_ids:
                        if file_id in file_ids_to_exclude:
                            continue
                        self._archive_file(
                            file_id,
                            self.project_52,
                            archived_file_count,
                            failed_archive,
                        )

                    if archived_file_count > 0:
                        temp_dict["archived"].append(
                            f"{self.project_52}:{directory_path} | {archived_file_count}"
                        )
                else:
                    logger.info("Running in debug mode. Skipping archiving")
            else:
                # no file-id match exclude regex
                # we do an overall dx.Project.archive
                if not self.debug:  # running in production
                    try:
                        res = dx.api.project_archive(
                            self.project_52, input_params={"folder": directory_path}
                        )
                        if res["count"] != 0:
                            temp_dict["archived"].append(
                                f"{self.project_52}:{directory_path} | {res['count']}"
                            )
                    except Exception:
                        logger.info(
                            f"Archiving {self.project_52}:{directory_path} file by file"
                            " because dx.project.archive failed"
                        )

                        file_ids = [
                            file["id"]
                            for file in list(
                                dx.find_data_objects(
                                    project=self.project_52,
                                    classname="file",
                                    folder=directory_path,
                                    limit=5
                                    if self.debug
                                    else None,  # limit to 5 files if debug
                                )
                            )
                        ]

                        archived_file_count = 0
                        for file_id in file_ids:
                            self._archive_file(
                                file_id,
                                self.project_52,
                                archived_file_count,
                                failed_archive,
                            )

                        if archived_file_count > 0:
                            temp_dict["archived"].append(
                                f"{self.project_52}:{directory_path} | {archived_file_count}"
                            )
                else:
                    logger.info("Running in debug mode. Skipping archiving")

    def find_directories(self, project_id: str, archive_pickle: dict) -> None:
        """
        Find directories or folders in a project which fit the criteria for archiving

        Parameters:
            project_id: project id
            archive_pickle: dict to store folder path that will be archived

        Returns:
            None
        """

        archive_pickle["staging_52"] = []

        # get all directories in staging-52
        trimmed_folder_to_original_value = {}

        # list to contain to-be-archived directories
        # for Slack notification
        to_be_archived_directories: list[str] = []

        # get folders in root of stagingarea-52
        for folder in self._get_folders_in_project(project_id):
            if folder == "/processed":
                continue

            trimmed_folder = folder.lstrip("/")
            trimmed_folder_to_original_value[trimmed_folder] = folder

        # get folders in /processed of stagingarea-52
        for folder in self._get_folders_in_project(
            project_id, directory_path="/processed"
        ):
            trimmed_folder = folder.lstrip("/processed/")
            trimmed_folder_to_original_value[trimmed_folder] = folder

        logger.info(
            f"Processing {len(trimmed_folder_to_original_value)} directories in stagingA-52"
        )

        # check if directories have 002 projs made and 002 has not been modified
        # in the last X month
        trimmed_folder_to_original_value = {
            trimmed: original
            for trimmed, original in trimmed_folder_to_original_value.items()
            if self._validate_directory(trimmed)
        }

        logger.info(
            f"Number of old enough directories: {len(trimmed_folder_to_original_value)}",
        )

        if trimmed_folder_to_original_value:
            logger.info("Processing directories...")

            # for building proj link
            trimmed_project = self.project_52.lstrip("project-")

            # set a counter
            n = 0

            for _, original_directory in trimmed_folder_to_original_value.items():
                trimmed_directory = original_directory.lstrip("/")

                # progress tracking
                if n > 0 and n % 20 == 0:
                    logger.info(
                        f"Processing {n}/{len(trimmed_folder_to_original_value)} directories",
                    )

                n += 1

                # get all the files within that directory in staging-52
                all_files = list(
                    dx.find_data_objects(
                        classname="file",
                        project=self.project_52,
                        folder=original_directory,
                        describe={"fields": {"archivalState": True}},
                        limit=5 if self.debug else None,  # limit to 5 files if debug
                    )
                )

                # get all files' archivalStatus
                status = set([x["describe"]["archivalState"] for x in all_files])

                # if there're files in directory with 'live' status
                if "live" in status:
                    # if there's 'never-archive' tag in any file, continue
                    never_archive = list(
                        dx.find_data_objects(
                            project=self.project_52,
                            folder=original_directory,
                            tags=["never-archive"],
                            limit=1,
                        )
                    )

                    if never_archive:
                        continue

                    # check for 'no-archive' tag in any files
                    no_archive = list(
                        dx.find_data_objects(
                            project=self.project_52,
                            folder=original_directory,
                            tags=["no-archive"],
                            describe={"fields": {"modified": True}},
                            limit=5
                            if self.debug
                            else None,  # limit to 5 files if debug
                        )
                    )

                    staging_prefix = (
                        f"{self.dnanexus_url_prefix}/{trimmed_project}/data"
                    )

                    if not no_archive:
                        # there's no 'no-archive' tag or 'never-archive' tag
                        archive_pickle["staging_52"].append(original_directory)

                        # this is for slack notification
                        to_be_archived_directories.append(
                            f"<{staging_prefix}/{trimmed_directory}|{original_directory}>"
                        )
                    else:
                        # if there's 'no-archive' tag
                        # check if all files are active in the last X month
                        # when tagged, modified date will change
                        # if modified date > x month, we know the tag was
                        # probably there for quite a while
                        # if all files have modified date > x month
                        # we remove tags and list directory for archiving
                        if all(
                            [
                                self._older_than(self.month2, f["describe"]["modified"])
                                for f in no_archive
                            ]
                        ):
                            # if all files within the directory are older than
                            # x month
                            logger.info(f"Removing tag for {len(no_archive)} files")

                            if not self.debug:
                                for file in no_archive:
                                    self._remove_tag_from_file(
                                        file["id"], self.project_52
                                    )

                            self.special_notify_list.append(
                                f"{original_directory} in `staging52`",
                            )
                            archive_pickle["staging_52"].append(original_directory)
                            to_be_archived_directories.append(
                                f"<{staging_prefix}/{trimmed_directory}|{original_directory}>"
                            )
                        else:
                            logger.info(
                                f"SKIPPED: {original_directory} in stagingarea52",
                            )
                            continue
                else:
                    # no 'live' status means all files
                    # in the directory have been archived thus we continue
                    continue

    def find_projects(
        self,
        archive_pickle: dict,
        status_dict: dict = {},
    ) -> dict:
        """
        Function to fetch qualified projects and notify on Slack

        Parameters:
            archive_pickle: dict to store projects that have been archived
            status_dict: dict to store status of the runs
        """

        logger.info("Start finding projects")

        archive_pickle["to_be_archived"] = []

        # store to-be-archived projects
        to_be_archived_list: dict = collections.defaultdict(list)

        # get all old enough projects
        old_enough_projects_dict = self._get_old_enough_projects(
            self.project_ids_to_exclude,
        )

        logger.info(f"Number of old enough projects: {len(old_enough_projects_dict)}")

        if old_enough_projects_dict:
            logger.info("Processing projects...")

            n: int = 0

            for proj_id, v in old_enough_projects_dict.items():
                # keep track of the progress silently
                if n > 0 and n % 20 == 0:
                    logger.info(
                        f"Processing {n}/{len(old_enough_projects_dict)} projects",
                    )

                n += 1

                project_name: str = v["describe"]["name"]
                tags: list[str] = [tag.lower() for tag in v["describe"]["tags"]]
                trimmed_id: str = proj_id.lstrip("project-")
                created_by: str = v["describe"]["createdBy"]["user"]

                if "never-archive" in tags:
                    # project tagged with 'never-archive'
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
                            limit=5
                            if self.debug
                            else None,  # limit to 5 files if debug
                        )
                    )
                    status = set(
                        [x["describe"]["archivalState"] for x in all_files],
                    )

                if "live" in status:
                    # there is something to be archived
                    pass
                else:
                    # everything has been archived
                    continue

                if "no-archive" in tags:
                    if not self.debug:
                        # project is old enough + have 'no-archive' tag
                        # thus, we remove the tag and
                        # list it in special-notify list
                        self._remove_project_tag(proj_id)

                    self.special_notify_list.append(project_name)

                # add project-id to to-be-archived list in memory
                archive_pickle["to_be_archived"].append(proj_id)

                # this is for slack notification
                if project_name.startswith("002"):
                    to_be_archived_list["002"].append(
                        f"<{self.dnanexus_url_prefix}/{trimmed_id}/|{project_name}>"
                    )
                else:
                    to_be_archived_list["003"].append(
                        {
                            "user": created_by,
                            "link": f"<{self.dnanexus_url_prefix}/{trimmed_id}/|{project_name}>",
                        }
                    )

        # get everything ready for slack notification
        projects_002: list = sorted(to_be_archived_list["002"])
        projects_003: list = []

        # process 003 list to sort by user in Slack notification
        temp003 = to_be_archived_list["003"]
        if temp003:
            # sort list by user
            temp003 = sorted(temp003, key=lambda d: d["user"])
            current_usr = None

            for link in temp003:
                # if new user
                if current_usr != link["user"]:
                    projects_003.append("\n")
                    current_usr = link["user"]  # update current user

                    projects_003.append(
                        f"<@{self.members[current_usr]}>"
                        if self.members.get(current_usr)
                        else f"Cannot find id for: {current_usr}"
                    )

                projects_003.append(link["link"])

        logger.info("End of finding projects")

        return {"002": projects_002, "003": projects_003}

    def notify_on_slack(
        self, notification_data: dict, next_archiving_date: dt.datetime
    ) -> None:
        """
        Notify on slack

        :param notification_data: dictionary of key (purpose) and value (list of projects)
        :param next_archiving_date: next archiving date

        :return: None
        """
        ORDERS = [
            "002",
            "003",
            "staging52",
            "precision",
            "special-notify",
            "no-archive",
            "never-archive",
        ]

        for order in ORDERS:
            data: list = notification_data.get(order)
            if data:
                data.append(":checkered_flag:")

                self.slack.post_message_to_slack(
                    "#egg-alerts",
                    order,
                    self.today_datetime,
                    data=data,
                    archiving_date=next_archiving_date,
                )

    def _write_to_file(self, data: list, target: str) -> None:
        """
        Function to write to file

        :param data: list of data to be written
        :param target: target filepath

        :return: None
        """
        # if file not present, create one
        if os.path.isfile(target):
            with open(target, "a") as f:
                f.write("\n" + f"=== {self.today_datetime} ===")

                for line in data:
                    f.write("\n" + line)
        else:  # if file present, append to it
            with open(target, "w") as f:
                f.write("\n" + f"=== {self.today_datetime} ===")
                f.write("\n".join(data))

    def archiving_function(
        self,
        archive_pickle: dict,
    ) -> None:
        """
        Function to archive projects and directories

        :param archive_pickle: dict
            which contains list of projects and directories
            to be archived
        """

        logger.info("Archiving...")

        list_of_projects_in_memory: list = archive_pickle.get("to_be_archived", [])
        list_of_directories_in_memory: list = archive_pickle.get("staging_52", [])

        # just for recording what has been archived
        # plus for Slack notification
        temp_archived = collections.defaultdict(list)
        failed_archive = []

        if list_of_projects_in_memory:
            # loop through each project

            for index, project_id in enumerate(list_of_projects_in_memory):
                if index > 0 and index % 20 == 0:
                    logger.info(
                        f"Processing {index}/{len(list_of_projects_in_memory)} projects",
                    )

                try:
                    project = dx.DXProject(project_id)

                    # query latest project detail on archiving time
                    detail = project.describe()
                except dx.exceptions.ResourceNotFound as e:
                    # if project-id no longer exist on DNAnexus
                    # probably project got deleted or etc.
                    # causing this part to fail
                    logger.info(f"{project_id} seems to have been deleted" f"{e}")
                    continue
                except Exception as e:
                    # no idea what kind of exception DNAnexus will give
                    # log and move on
                    logger.error(e)
                    continue

                project_name: str = detail["name"]
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

                elif ("archive" in tags) or self._older_than(
                    self.archived_modified_month, modified_epoch
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

                    for word in self.regex_excludes:
                        # find all file-id that match the regex
                        file_id_to_exclude.update(
                            [
                                file["id"]
                                for file in list(
                                    dx.find_data_objects(
                                        name=word,
                                        name_mode="regexp",
                                        project=project_id,
                                        classname="file",
                                        limit=5
                                        if self.debug
                                        else None,  # limit to 5 if debug
                                    )
                                )
                            ]
                        )

                    # get all file-ids in the project
                    file_ids = [
                        file["id"]
                        for file in list(
                            dx.find_data_objects(
                                project=project_id,
                                classname="file",
                                limit=5 if self.debug else None,  # limit to 5 if debug
                            )
                        )
                    ]
                    archived_file_count: int = 0

                    if file_id_to_exclude:
                        # if there is file-id that match exclude regex
                        if not self.debug:  # if running in production
                            for file_id in file_ids:
                                # if file-id match file-id in exclude list, skip
                                if file_id in file_id_to_exclude:
                                    continue
                                self._archive_file(
                                    file_id,
                                    project_id,
                                    archived_file_count,
                                    failed_archive,
                                )

                            if archived_file_count > 0:
                                temp_archived["archived"].append(
                                    f"{project_id} | {project_name} | {archived_file_count}"
                                )
                        else:
                            logger.info("Running in debug mode, skipping archiving")
                    else:
                        # if no file-id match the regex
                        # do an overall dx.Project.archive
                        if not self.debug:  # running in production
                            try:
                                res = dx.api.project_archive(
                                    project_id, input_params={"folder": "/"}
                                )
                                if res["count"] != 0:
                                    temp_archived["archived"].append(
                                        f"{project_id} | {project_name} | {res['count']}"
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
                                    f"Archiving {project_id} file by file because"
                                    " dx.project.archive failed"
                                )

                                # get all files in project and do it individually
                                file_ids = [
                                    file["id"]
                                    for file in list(
                                        dx.find_data_objects(
                                            project=project_id,
                                            classname="file",
                                            limit=5
                                            if self.debug
                                            else None,  # limit to 5 if debug
                                        )
                                    )
                                ]

                                archived_file_count = 0

                                for file_id in file_ids:
                                    self._archive_file(
                                        file_id,
                                        project_id,
                                        archived_file_count,
                                        failed_archive,
                                    )

                                if archived_file_count > 0:
                                    temp_archived["archived"].append(
                                        f"{project_id} | {project_name} | {archived_file_count}"
                                    )
                        else:
                            logger.info("Running in debug mode, skipping archiving")
                else:
                    # project not older than ARCHIVE_MODIFIED_MONTH
                    # meaning project has been modified recently, so skip
                    logger.info(f"RECENTLY MODIFIED: {project_name}")
                    continue

        if list_of_directories_in_memory:
            # directories in to-be-archived list in stagingarea52
            for index, directory in enumerate(list_of_directories_in_memory):
                if index > 0 and index % 20 == 0:
                    logger.info(
                        f"Processing {index}/{len(list_of_directories_in_memory)} directories",
                    )
                self._archive_directory_based_on_directory_path(
                    directory,
                    temp_archived,
                    failed_archive,
                )

        # write file-id that failed archive
        if failed_archive:
            self._write_to_file(failed_archive, self.archived_failed_path)

        # keep a copy of what has been archived
        # ONLY IF THERE ARE FILEs BEING ARCHIVED
        if temp_archived:
            self._write_to_file(temp_archived["archived"], self.archived_txt_path)

            # also send a notification to say what have been archived
            self.slack.post_message_to_slack(
                "#egg-logs",
                "archived",
                self.today_datetime,
                data=temp_archived["archived"],
            )

        logger.info("End of archiving function")

    def _get_all_files_in_folder(self, project_id: str, folder: str = None) -> list:
        """
        Function fetch all files within a folder in a project

        Args:
            project_id (str): project-id
            folder (str): folder path

        Returns:
            list: list of files
        """
        return list(
            dx.find_data_objects(
                classname="file",
                folder=folder,
                project=project_id,
                describe={
                    "modified": True,
                    "archivalState": True,
                },
            )
        )

    def _archive_project(self, project_id, folder) -> None:
        """
        Function to archive project with folder param.
        If dnanexus-related error, we resort to archiving file by file
        If unknown error, we log it and move on

        Args:
            project_id (str): project-id
            folder (str): folder path

        Returns:
            None
        """
        try:
            dx.api.project_archive(
                project_id,
                input_params={"folder": folder},
            )
        except (
            dx.exceptions.ResourceNotFound,
            dx.exceptions.PermissionDenied,
            dx.exceptions.InvalidInput,
            dx.exceptions.InvalidState,
        ):
            # if dnanexus project-archive failed
            # archive file by file
            files = self._get_all_files_in_folder(project_id, folder)

            if files:
                active_files = [
                    file
                    for file in files
                    if file["describe"]["archivalState"] == "active"
                ]

                for file_id in active_files:
                    self._archive_file(file_id, project_id)
        except Exception as e:
            logger.error(f"Archiving project error (unknown): {e}")

    def find_precision_project(
        self,
        archive_pickle: dict,
    ) -> list:
        """
        Function to find folder in "specific" projects
        that are older than precision_month and record them

        :param archive_pickle: pickle file that act as the memory
        :param precision_projects: list of project-id that are precision projects
        :param precision_month: number of months to be considered as "old"

        :return: list of links to folder within precision projects that are going
        to be archived
        """
        # clear the memory first
        logger.info("Finding precision projects...")
        archive_pickle["precision"] = []
        to_be_archived = []

        for project_id in self.precision_projects:
            try:
                dx_project = dx.DXProject(project_id)
            except Exception:
                # project is not found by dnanexus
                # incorrect project-id
                logger.info(f'Project "{project_id}" not found by DNAnexus')
                continue  # skip

            folders = dx_project.list_folder().get("folders", [])

            for folder in folders:
                files = self._get_all_files_in_folder(project_id, folder)

                if not files:  # if no file in folder
                    continue

                active_files = [
                    file
                    for file in files
                    if file["describe"]["archivalState"] != "archived"
                ]  # only process those that are not archived

                if not active_files:  # no active file, everything archived
                    continue

                latest_modified_date = max(
                    [file["describe"]["modified"] for file in active_files]
                )  # get latest modified date

                # see if latest modified date is more than precision_month
                is_older_than: bool = older_than(
                    self.precision_month, latest_modified_date
                )

                if is_older_than:
                    # if the oldest modified file is older than precision_month
                    # add the folder path and project-id to memory pickle
                    archive_pickle["precision"].append(f"{project_id} | {folder}")
                    to_be_archived.append(
                        f"<{self.dnanexus_url_prefix}/{project_id.lstrip('project-')}/data/{folder.lstrip('/')}|{folder}>"
                    )

        return to_be_archived

    def archive_precision_projects(
        self,
        project_id_and_folders: list,
    ) -> None:
        """
        Function to archive "specific" projects that are older than precision_month

        :param project_id_and_folders: list of tuple consisting of project-id and folder path
        :param precision_month: number of months to be considered as "old"

        :return: None
        """
        logger.info("Archiving precision projects...")

        for project_id_and_folder in project_id_and_folders:
            project_id, folder = (p.strip() for p in project_id_and_folder.split("|"))

            # check again the same criteria if latest modified date is older than precision_month
            # because it might have been modified recently
            files = self._get_all_files_in_folder(project_id, folder)

            active_files = [
                file
                for file in files
                if file["describe"]["archivalState"] != "archived"
            ]  # only process those that are not archived

            if not active_files:  # no active file, everything archived
                continue

            latest_modified_date = max(
                [file["describe"]["modified"] for file in active_files]
            )  # get latest modified date

            # see if latest modified date is more than precision_month
            is_older_than: bool = older_than(self.precision_month, latest_modified_date)

            if is_older_than:
                # archive the folder in the project-id
                if not self.debug:
                    # archive the folder
                    self._archive_project(project_id, folder)
                else:
                    logger.info("Debug mode, skipping archiving...")
