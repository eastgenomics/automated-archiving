import dxpy as dx

from bin.helper import get_logger
from bin.util import get_projects_as_dict

logger = get_logger(__name__)


class TagClass:
    def _add_tag_to_project(self, tag: str, project_id: str) -> None:
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
            logger.error(
                f"invalid tag input when tagging {tag} for project id {project_id}"
            )
        except dx.exceptions.PermissionDenied:
            logger.error(f"permission denied when tagging {project_id}")
        except Exception as e:
            # no idea what's wrong
            logger.error(e)

    def _remove_tags_from_project(self, tags: list, project_id: str) -> None:
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
            logger.error(
                f"invalid tag input when removing tag {tags} from project id {project_id}"
            )
        except dx.exceptions.PermissionDenied:
            logger.error(f"permission denied when tagging {project_id}")
        except Exception as e:
            # no idea what's wrong
            logger.error(e)

    def tag(self) -> None:
        """
        Function to tag projects based on their archival status
        """

        logger.info("Tagging projects..")

        all_projects = {
            **get_projects_as_dict("002"),
            **get_projects_as_dict("003"),
        }

        # separate out those with archivedDataUsage == dataUsage
        # which are fully archived so we don't have to query them
        archived_projects = {
            k: v
            for k, v in all_projects.items()
            if v["describe"]["archivedDataUsage"] == v["describe"]["dataUsage"]
        }

        for project_id, v in archived_projects.items():
            if not project_id.strip():
                continue

            tags = [tag.lower() for tag in v["describe"]["tags"]]

            if "partial archived" in tags:
                self._remove_tags_from_project(["partial archived"], project_id)

            self._add_tag_to_project("fully archived", project_id)

        # whatever is leftover from above projects, we do the query
        # they can be 'live' or 'partially archived'
        projects_with_unsure_archival_status = {
            project_id: v
            for project_id, v in all_projects.items()
            if project_id not in archived_projects.keys()
        }

        for project_id, v in projects_with_unsure_archival_status.items():
            # get project tags
            if not project_id.strip():
                continue

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
                if 'fully archived' in tags:  # some files are live
                    self._remove_tags_from_project(["fully archived"], project_id)
                self._add_tag_to_project("partial archived", project_id)
            elif "live" not in status:  # all files are archived
                if 'partial archived' in tags:
                    self._remove_tags_from_project(['partial archived'], project_id)
                self._add_tag_to_project("fully archived", project_id)
            else:
                # all files are live, no tagging needed
                continue
