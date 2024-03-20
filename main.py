import datetime as dt
from typing import Optional

from bin.helper import get_logger

from bin.slack import SlackClass
from bin.archive import ArchiveClass
from bin.find import FindClass
from bin.environment import EnvironmentVariableClass

from bin.util import (
    read_or_new_pickle,
    dx_login,
    parse_arguments,
    get_members,
    parse_datetime,
)

logger = get_logger(__name__)


def main():
    args = parse_arguments()
    datetime = parse_datetime(args)

    logger.info(datetime)

    dnanexus_id_to_slack_id = get_members("members.ini")

    env = EnvironmentVariableClass()
    env.load_configs()

    # define Slack class
    slack = SlackClass(env, datetime)

    # define Archive class
    archive = ArchiveClass(env.ARCHIVE_DEBUG)

    # define Find class
    find = FindClass(env, dnanexus_id_to_slack_id)

    # dnanexus login
    if not dx_login(env.DNANEXUS_TOKEN):
        slack.post_simple_message_to_slack(
            "#egg-alerts", "automated-archiving: dnanexus login failed."
        )
        raise Exception("dnanexus login failed.")

    archive_pickle = read_or_new_pickle(env.AUTOMATED_ARCHIVE_PICKLE_PATH)

    projects_marked_for_archiving: Optional[list] = archive_pickle.get("projects", [])
    staging52_directories: Optional[list] = archive_pickle.get("directories", [])
    precision_projects: Optional[list] = archive_pickle.get("precisions", [])

    tars = find.get_tar()

    slack.notify({"tars": tars})

    if datetime.day in [1, 15]:
        archived_project_ids = archive.archive_projects(projects_marked_for_archiving)
        archived_directories_dict = archive.archive_staging52(staging52_directories)
        archived_precisions = archive.archive_precisions(precision_projects)

        slack.post_long_message_to_slack(
            "#egg-alerts" "archived", list(archived_project_ids)
        )
        slack.post_long_message_to_slack(
            "#egg-alerts" "archived",
            [
                f"{archived_count} files archived in {folder_path} in `staging52`."
                for folder_path, archived_count in archived_directories_dict.items()
            ],
        )
        slack.post_long_message_to_slack(
            "#egg-alerts" "archived",
            [
                f"{project_id}:{','.join(folder_path)} archived in `precision`."
                for project_id, folder_path in archived_precisions.items()
            ],
        )

    find.find_projects()
    find.find_directories()
    find.find_precisions()

    find.save_to_pickle()

    slack.notify(
        {
            "projects2": find.archiving_projects_2_slack,
            "projects3": find.archiving_projects_3_slack,
            "directories": find.archiving_directories_slack,
            "precisions": find.archiving_precision_directories_slack,
        }
    )


if __name__ == "__main__":
    main()
