import os
import datetime as dt

from bin.helper import get_logger
from bin.slack import SlackClass
from bin.archive import ArchiveClass
from bin.util import (
    read_or_new_pickle,
    dx_login,
    get_old_tar_and_notify,
    tagging_function,
)


from member.members import MEMBER_LIST

logger = get_logger(__name__)

DNANEXUS_URL_PREFIX: str = "https://platform.dnanexus.com/panx/projects"


if __name__ == "__main__":
    # env imports
    try:
        logger.info("Reading env variables")

        # debug mode
        DEBUG: bool = os.environ.get("ARCHIVE_DEBUG", False)
        logger.info(f"Running in DEBUG mode {DEBUG}")

        # slack token
        SLACK_TOKEN: str = os.environ["SLACK_TOKEN"]
        # dnanexus token
        DNANEXUS_TOKEN: str = os.environ["DNANEXUS_TOKEN"]

        # project ids
        PROJECT_52: str = os.environ.get(
            "PROJECT_52",
            "project-FpVG0G84X7kzq58g19vF1YJQ",
        )
        PROJECT_53: str = os.environ.get(
            "PROJECT_53",
            "project-FvbzbX84gG9Z3968BJjxYZ1k",
        )

        # project ids which require special attention
        PRECISION_ARCHIVING_PROJECTS: list = [
            project_id.strip()
            for project_id in os.environ["PRECISION_ARCHIVING"].split(",")
            if os.environ.get("PRECISION_ARCHIVING")
        ]

        # inactivity month for special attention projects
        PRECISION_MONTH: int = int(os.environ.get("PRECISION_MONTH", 1))

        logger.info(
            f"Precision archiving projects: {', '.join(PRECISION_ARCHIVING_PROJECTS)} with inactivity month set to {PRECISION_MONTH}"
        )

        # inactivity period for 002 projects in MONTH
        MONTH2: int = int(os.environ.get("AUTOMATED_MONTH_002", 3))
        # inactivity period for 003 projects in MONTH
        MONTH3: int = int(os.environ.get("AUTOMATED_MONTH_003", 1))
        # inactivity period for .tar files in staging52 in MONTH
        TAR_MONTH: int = int(os.environ.get("TAR_MONTH", 3))

        logger.info(
            f"Inactivity variable (months): 002={MONTH2}, 003={MONTH3}, tar={TAR_MONTH}"
        )

        # grace period
        ARCHIVE_MODIFIED_MONTH: int = int(
            os.environ.get(
                "ARCHIVE_MODIFIED_MONTH",
                1,
            )
        )

        # pathway for memory pickle file
        ARCHIVE_PICKLE_PATH = os.environ.get(
            "AUTOMATED_ARCHIVE_PICKLE_PATH",
            "/monitoring/archive_dict.pickle",
        )
        # pathway for txt file to record file id that failed archiving
        ARCHIVE_FAILED_PATH = os.environ.get(
            "AUTOMATED_ARCHIVE_FAILED_PATH",
            "/monitoring/failed_archive.txt",
        )
        # pathway for txt file to record file id that has been archived
        ARCHIVED_TXT_PATH = os.environ.get(
            "AUTOMATED_ARCHIVED_TXT_PATH",
            "/monitoring/archived.txt",
        )

        # regex to exclude filename that fits the pattern
        AUTOMATED_REGEX_EXCLUDE = [
            text.strip()
            for text in os.environ["AUTOMATED_REGEX_EXCLUDE"].split(",")
            if text.strip()
        ]

        logger.info(f"File regex exclude: {', '.join(AUTOMATED_REGEX_EXCLUDE)}")

    except KeyError as missing_env:
        logger.error(f"env {missing_env} cannot be found in config file")

        raise KeyError(f"env {missing_env} cannot be found in config file")

    # define Slack class
    slack = SlackClass(SLACK_TOKEN, TAR_MONTH, DEBUG)

    # re-define env variables for debug / testing
    if DEBUG:
        ARCHIVE_PICKLE_PATH = "/monitoring/archive_dict.test.pickle"
        ARCHIVE_FAILED_PATH = "/monitoring/failed_archive.test.txt"
        ARCHIVED_TXT_PATH = "/monitoring/archived.test.txt"

    logger.info(f"Archive pickle path: {ARCHIVE_PICKLE_PATH}")
    logger.info(f"Archive failed path: {ARCHIVE_FAILED_PATH}")
    logger.info(f"Archived .txt path: {ARCHIVED_TXT_PATH}")

    # read pickle memory
    archive_pickle: dict = read_or_new_pickle(ARCHIVE_PICKLE_PATH)
    to_be_archived: list = archive_pickle["to_be_archived"]
    staging52: list = archive_pickle["staging_52"]

    today: dt.date = dt.date(2023, 8, 1)  # determine overall script date
    logger.info(f"datetime: {today}")

    archive_class = ArchiveClass(
        DEBUG,
        today,
        ARCHIVE_MODIFIED_MONTH,
        MONTH2,
        MONTH3,
        AUTOMATED_REGEX_EXCLUDE,
        PROJECT_52,
        PROJECT_53,
        ARCHIVE_PICKLE_PATH,
        ARCHIVE_FAILED_PATH,
        ARCHIVED_TXT_PATH,
        MEMBER_LIST,
        DNANEXUS_URL_PREFIX,
        PRECISION_ARCHIVING_PROJECTS,
        slack,
    )

    if today.day in [1, 15]:
        dx_login(today, DNANEXUS_TOKEN, slack)

        if today.day == 1:
            get_old_tar_and_notify(
                today,
                TAR_MONTH,
                slack,
                PROJECT_52,
                DEBUG,
            )

        # if there is something in memory
        # we run archive function
        # else we find_and_notify
        if to_be_archived or staging52:
            archive_class.archiving_function(archive_pickle)

            tagging_function(DEBUG)

            archive_class.find_projects_and_notify(archive_pickle, {})

        else:
            archive_class.find_projects_and_notify(archive_pickle, {})

    else:
        if to_be_archived or staging52:
            # if there's to-be-archived in memory
            # we do the countdown to egg-alerts
            # else we just keep silence

            next_archiving_date = (
                archive_class.get_next_archiving_date_relative_to_today(today)
            )
            diff = next_archiving_date - today

            slack.post_message_to_slack(
                "#egg-alerts",
                "countdown",
                today,
                days_till_archiving=diff.days,
                archiving_date=next_archiving_date,
            )
        else:
            logger.info("No data in memory")

    logger.info("End of script.")
