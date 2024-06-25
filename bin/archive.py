import dxpy as dx
import collections
from typing import Optional, List
from itertools import groupby

from bin.util import (
    older_than,
    find_precision_files_by_folder_paths_parallel,
    call_in_parallel,
)
from bin.helper import get_logger
from bin.environment import EnvironmentVariableClass

logger = get_logger(__name__)


class ArchiveClass:
    """
    Class to handle archiving of projects and directories
    """

    def __init__(self, env: EnvironmentVariableClass):
        self.env = env

    def _get_projects_describe(
        self, project_ids: list, **find_data_args
    ) -> Optional[dict]:
        """
        Fetch describe of a list of projects.
        Return None if failed.

        Parameters:
        :param: project_ids: a list of project-ids
        """

        def _get(project_id, **find_data_args):
            try:
                return dx.DXProject(project_id).describe()
            except dx.exceptions.ResourceNotFound as e:
                # if project-id no longer exist on DNAnexus
                # probably project got deleted or etc.
                # causing this part to fail
                logger.info(f"{project_id} seems to be missing. {e}")
                return None
            except Exception as e:
                # no idea what kind of exception DNAnexus will give
                # log and move on to the next project
                logger.error(e)
                return None

        return call_in_parallel(
            func=_get, items=project_ids, find_data_args=None
        )

    def _find_file_ids_that_match_regex(
        self,
        regexes: list,
        project_id: str,
        directory_path: str = None,
    ) -> set:
        """
        Function to find file-ids that match the regexes

        Parameters:
        :param: regexes: list of regexes to match
        :param: project_id: project-id
        :param: directory_path: directory path in the project-id

        Returns: set of file-ids
        """

        file_ids = set()

        for regex in regexes:
            try:
                for file in dx.find_data_objects(
                    name=regex,
                    name_mode="regexp",
                    project=project_id,
                    folder=directory_path,
                    classname="file",
                ):
                    file_ids.add(file.get("id"))
            except Exception as e:
                logger.error(e)
                continue

        return file_ids

    def _parallel_archive_file(self, file_ids, project) -> None:
        """
        Archiving a list of file-id on DNAnexus in parallel
        """

        def _archive(file, **find_data_args):
            dx.DXFile(
                file,
                project=find_data_args["project"],
            ).archive()

        return call_in_parallel(_archive, file_ids, project=project)

    def _archive_file(
        self,
        file_id: str,
        project_id: str,
    ) -> None:
        """
        Function to archive file-id on DNAnexus

        Parameters:
            file_id: file-id to be archived
            project_id: project-id where the file is in
        """
        try:
            dx.DXFile(
                file_id,
                project=project_id,
            ).archive()

        except (
            dx.exceptions.ResourceNotFound,
            dx.exceptions.PermissionDenied,
            dx.exceptions.InvalidInput,
            dx.exceptions.InvalidState,
        ) as dnanexus_error:  # catching DNAnexus-related errors
            logger.error(dnanexus_error.error_message())
        except Exception as e:  # non-DNAnexus related errors
            logger.error(e)

    def list_files_to_archive_per_project(self, list_of_projects: list) -> set:
        """
        Checks that functions and their constituent files, previously listed
         as ready-to-archive, are still in a valid state.
        Adds the valid project ids and files to a dict-of-lists ready for
        bulk archiving.

        Param: a list, list_of_projects, which gives project IDs for assessment

        Returns: dict of ready-to-archive files in format
        {project-ids: [file-1, file-2]}
        """

        # jot down what has been archived
        archived_projects = set()

        logger.info(f"{len(list_of_projects)} projects found for archiving.")

        # get up-to-date descriptions for the projects
        # use them to check we still want to archive them, in which case, add
        # their constituent qualifying files to project_files_cleared_for_archive
        project_details = self._get_projects_describe(list_of_projects)
        project_details = {
            k: list(v)
            for k, v in groupby(project_details, lambda x: x["project"])
        }

        project_files_cleared_for_archive = dict()

        for project_id in list_of_projects:
            project_detail = project_details.get(project_id)

            if not project_detail:
                continue

            project_name: str = project_detail.get("name")
            modified_epoch = project_detail.get("modified")
            tags = project_detail.get("tags", [])

            # check their tags
            if "never-archive" in tags:
                # project has been tagged never-archive - skip archiving it
                logger.info(f"NEVER ARCHIVE: {project_name}. Skip archiving!")
                continue

            elif ("archive" in tags) or older_than(
                self.env.ARCHIVE_MODIFIED_MONTH, modified_epoch
            ):
                # if project is tagged with 'archive'
                # or project is inactive in last
                # 'archived_modified_month' month
                # both result in the same archiving process

                # exclude files that match the exclude regex
                file_ids_to_exclude = self._find_file_ids_that_match_regex(
                    self.env.AUTOMATED_REGEX_EXCLUDE, project_id
                )

                for file in dx.find_data_objects(
                    project=project_id,
                    classname="file",
                    archival_state="live",
                    folder="/",
                ):
                    if (
                        file["id"] in file_ids_to_exclude
                    ):  # skip file-id that match exclude regex
                        continue

                    # this file needs to be archived, add to dict
                    if project_files_cleared_for_archive.get(project_id):
                        project_files_cleared_for_archive[project_id].append(
                            file["id"]
                        )
                    else:
                        project_files_cleared_for_archive[project_id] = [
                            file["id"]
                        ]

            else:
                # project not older than ARCHIVE_MODIFIED_MONTH
                # meaning project has been modified recently, so skip
                logger.info(
                    f"RECENTLY MODIFIED: {project_name}. Skip archiving!"
                )
                continue

        return project_files_cleared_for_archive

    def _archive_directory_based_on_directory_path(
        self,
        project_id: str,
        directory_path: str,
    ) -> int:
        """
        Function to archive files in directories

        Arguments:
        :param: project_id: project-id
        :param: directory_path: directory path in the project-id

        Returns: number of files archived in the directory
        """
        archived_count = 0

        # check for 'never-archive' tag in directory
        never_archive = list(
            dx.find_data_objects(
                project=project_id,
                folder=directory_path,
                tags=["never-archive"],
                limit=1,
            )
        )

        if never_archive:
            logger.info(f"NEVER ARCHIVE: {directory_path} in {project_id}")
            return archived_count

        # 2 * 4 week = 8 weeks
        num_weeks = self.env.ARCHIVE_MODIFIED_MONTH * 4

        # check if there's any files modified in the last num_weeks
        recent_modified = list(
            dx.find_data_objects(
                project=self.env.PROJECT_52,
                folder=directory_path,
                modified_after=f"-{num_weeks}w",
                limit=1,
            )
        )

        if recent_modified:
            logger.info(f"RECENTLY MODIFIED: {directory_path} in {project_id}")
            return archived_count

        # if directory in staging52 got
        # no tag indicating dont archive
        # it will end up here
        excluded_file_ids = self._find_file_ids_that_match_regex(
            self.env.AUTOMATED_REGEX_EXCLUDE,
            self.env.PROJECT_52,
            directory_path,
        )

        # get a collection of the files in this project/directory that 
        # should be archived
        active_file_ids = list()

        for file in dx.find_data_objects(
            project=project_id,
            classname="file",
            archival_state="live",
            folder=directory_path,
        ):
            if (
                file["id"] not in excluded_file_ids
            ):  
                # only archive file-id that DON'T match exclude regex
                active_file_ids.append(file["id"])

        # archive the files if running in production
        if not self.env.ARCHIVE_DEBUG:
            active_file_ids = list(set(active_file_ids))
            self._parallel_archive_file(self, active_file_ids, project_id)

            archived_count = len(active_file_ids)
            logger.info(
                f"{archived_count} files archived in {directory_path} in {project_id}"
            )
        else:
            logger.info(
                f"Running in DEBUG mode. Skip archiving {directory_path} in {project_id}!"
            )

        return archived_count

    def archive_staging52(self, directory_list: list) -> dict:
        """
        Function to archive directories in staging-52

        Parameters:
        :param: directory_list: list of directories to be archived

        Returns: dictionary of archived directories and number of files archived
        """

        archived_dict = {}
        logger.info(f"{len(directory_list)} directories found for archiving.")

        # directories in to-be-archived list in stagingarea52
        for index, directory in enumerate(directory_list):
            if index > 0 and index % 20 == 0:
                logger.info(
                    f"Processing {index}/{len(directory_list)} directory",
                )
            archived_num = self._archive_directory_based_on_directory_path(
                self.env.PROJECT_52,
                directory,
            )

            if archived_num > 0:
                archived_dict[directory] = archived_num

        return archived_dict

    def archive_precisions(
        self,
        project_id_and_folders: List[str],
    ) -> dict:
        """
        Function to archive "precisions" projects that are older than precision_month

        :param project_id_and_folders: list of tuple consisting of project-id and folder path

        :return: dictionary of archived project-id (key) and folder paths (list)
        """

        archived_precisions = collections.defaultdict(list)
        logger.info("Archiving precisions..")

        # reformat the pickle into a dict of project IDs linked to their paths
        project_id_to_folder = dict()
        for project_id_and_folder in project_id_and_folders:
            project_id, folder_path = (
                p.strip() for p in project_id_and_folder.split("|")
            )
            if not project_id_to_folder.get(project_id):
                project_id_to_folder[project_id] = [folder_path]
            else:
                project_id_and_folder[project_id].append(project_id)

        for project_id, folder_path in project_id_to_folder.items():
            # check again the same criteria if latest modified date is older than precision_month
            # because it might have been modified recently
            active_files = find_precision_files_by_folder_paths_parallel(
                folder_path, project_id
            )

            if not active_files:  # no active file, everything archived
                continue

            latest_modified_date = max(
                [file["describe"]["modified"] for file in active_files]
            )  # get latest modified date

            # see if latest modified date is more than precision_month
            is_older_than: bool = older_than(
                self.env.PRECISION_MONTH, latest_modified_date
            )

            if is_older_than:
                # archive the folder in the project-id
                if not self.env.ARCHIVE_DEBUG:
                    # archive the folder
                    active_file_ids = [x for x in active_files["id"]]
                    self._parallel_archive_file(self, active_file_ids, project_id)

                    archived_precisions[project_id].append(folder_path)
                    logger.info(f"{project_id}:{folder_path} archived!")
                else:
                    logger.info("Debug mode, Skip archiving..")

        return archived_precisions
