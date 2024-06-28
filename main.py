from typing import Optional
from itertools import groupby

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
    archive = ArchiveClass(env)

    # define Find class
    find = FindClass(env, dnanexus_id_to_slack_id)

    # dnanexus login
    if not dx_login(env.DNANEXUS_TOKEN):
        slack.post_simple_message_to_slack(
            "#egg-alerts", "automated-archiving: dnanexus login failed."
        )
        raise Exception("dnanexus login failed.")

    archive_pickle = read_or_new_pickle(env.AUTOMATED_ARCHIVE_PICKLE_PATH)

    projects_marked_for_archiving: Optional[list] = archive_pickle.get(
        "projects", []
    )
    staging52_directories: Optional[list] = archive_pickle.get(
        "directories", []
    )
    precision_projects: Optional[list] = archive_pickle.get("precisions", [])

    tars = find.get_tar_staging(staging52_directories)
    
    slack.notify({"tars": tars})

    if str(datetime.day) in env.ARCHIVING_RUN_DATES:
        # check that projects are still ready to archive, and if so,
        # check that their constituent files pass archive criteria too.
        # Finally, archive the files.
        checked_projects_to_archive = (
            archive.check_projects_still_ready_to_archive(
                projects_marked_for_archiving
            )
        )

        files = archive.find_live_files_parallel_multiproject(
            checked_projects_to_archive
        )
        files = {k: list(v) for k, v in groupby(files, lambda x: x["project"])}

        files_to_archive = archive.check_files_ready_to_archive(files)

        if not archive.env.ARCHIVE_DEBUG:  # if running in production
            # run the archiving
            for (
                project_id,
                files_to_archive,
            ) in checked_projects_to_archive.items():
                archive._parallel_archive_file(files_to_archive, project_id)
        else:
            logger.info(
                f"Running in DEBUG mode. Skip archiving "
                f"{checked_projects_to_archive.keys()}!"
            )

        archived_directories_dict = archive.archive_staging52(
            staging52_directories
        )
        archived_precisions = archive.archive_precisions(precision_projects)

        slack.post_long_message_to_slack(
            "#egg-alerts" "archived", list(checked_projects_to_archive.keys())
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

    slack.post_simple_message_to_slack(
        "#egg-alerts",
        f"automated-archiving guideline: {env.GUIDELINE_URL}",
    )

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
